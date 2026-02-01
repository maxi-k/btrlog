use std::collections::VecDeque;

use crate::{
    io::buffer::IoBuffer,
    server::wal::DiskLocation,
    types::{
        error::{FetchEntryError, JournalPushError},
        id::{Epoch, JournalId, JournalOffset, LSN, LogicalJournalOffset, PageIdentifier},
    },
};

use super::page::JournalPage;

pub struct Journal {
    id: JournalId,
    offset_cumsum: LogicalJournalOffset,
    current_page: JournalPage,
    unflushed_old: VecDeque<(PageIdentifier, JournalPage)>,
    unflushable_lsn_window: u64,
}

pub struct FlushGroup {
    pub id: JournalId,
    pub to_flush: Vec<(PageIdentifier, IoBuffer)>,
}

impl Journal {
    pub fn new_with_id(id: JournalId, unflushable_lsn_window: u64) -> Self {
        Self {
            id,
            offset_cumsum: 0,
            current_page: JournalPage::alloc_fresh_log(),
            unflushed_old: VecDeque::with_capacity(8),
            unflushable_lsn_window,
        }
    }

    fn find_page_for_lsn(&mut self, epoch: Epoch, lsn: LSN) -> Option<&mut JournalPage> {
        if self.current_page.lsn_range().contains(&lsn) && self.current_page.epoch == epoch {
            return Some(&mut self.current_page);
        }
        if let Some((_page_id, page)) = self
            .unflushed_old
            .iter_mut()
            .find(|(_page_id, page)| page.lsn_range().contains(&lsn) && page.epoch == epoch)
        {
            return Some(page);
        }
        return None;
    }

    pub fn push(
        &mut self,
        epoch: Epoch,
        lsn: LSN,
        offset: LogicalJournalOffset,
        data: &[u8],
    ) -> Result<LogicalJournalOffset, JournalPushError> {
        let page_offset = match self.compute_page_offset(offset) {
            Ok(off) => off,
            Err(delta) => {
                // uncommon case: look through unflushed pages, push there if possible
                if let Some((page_id, page)) = self.unflushed_old.iter_mut().find(|(_page_id, page)| {
                    page.lsn_range().contains(&lsn)
                }) {
                    let page_cumulative_offset = page_id.2;
                    debug_assert!(page_cumulative_offset <= offset);
                    let page_offset = (offset - page_cumulative_offset) as JournalOffset;
                    let res = page.push(epoch, lsn, page_offset, data);
                    return match res {
                        Ok(off) => Ok(page_cumulative_offset + off as LogicalJournalOffset),
                        Err(e) => Err(e),
                    };
                } else {
                    log::error!(
                        "passed journal offset too large by {}; not found in {} old pages",
                        delta,
                        self.unflushed_old.len()
                    );
                    return Err(JournalPushError::OffsetTooSmall);
                }
            }
        };
        let res = self.current_page.push(epoch, lsn, page_offset, data);
        match res {
            Ok(off) => Ok(self.offset_cumsum + off as LogicalJournalOffset),
            Err(e) => {
                match e {
                    JournalPushError::NotEnoughFreeSpace => {
                        self.retire_current(epoch);
                        // XXX max retries
                        self.push(epoch, lsn, offset, data)
                    }
                    JournalPushError::EpochTooLarge => {
                        self.retire_current(epoch);
                        // XXX max retries
                        self.push(epoch, lsn, offset, data)
                    }
                    // all other errors are 'user errors' and passed back
                    _ => Err(e),
                }
            }
        }
    }

    pub fn announce_disk_location(&mut self, epoch: Epoch, lsn: LSN, loc: DiskLocation) {
        if let Some(page_ref) = self.find_page_for_lsn(epoch, lsn) {
            page_ref.set_last_known_disk_location(epoch, lsn, loc);
        }
    }

    pub fn retire_current(&mut self, epoch: Epoch) {
        let new_log = JournalPage::alloc_empty(epoch, self.current_page.max_lsn());
        let old_log = std::mem::replace(&mut self.current_page, new_log);
        let key = PageIdentifier(old_log.epoch, old_log.lsn_range().end, self.offset_cumsum);
        self.offset_cumsum += old_log.page_offset() as u64;
        self.unflushed_old.push_back((key, old_log));
    }

    #[allow(dead_code)]
    pub fn id(&self) -> JournalId {
        self.id
    }

    pub fn max_lsn(&self) -> u64 {
        self.current_page.lsn_range().end
    }

    fn compute_page_offset(&self, offset: LogicalJournalOffset) -> Result<JournalOffset, u64> {
        if offset >= self.offset_cumsum {
            Ok((offset - self.offset_cumsum) as JournalOffset)
        } else {
            Err(self.offset_cumsum - offset)
        }
    }

    #[inline]
    fn wants_flush(&self) -> bool {
        !self.unflushed_old.is_empty()
    }

    pub fn last_known_epoch(&self) -> u64 {
        self.current_page.epoch
    }

    pub fn last_known_logical_offset(&self) -> u64 {
        self.offset_cumsum + self.current_page.page_offset() as u64
    }

    pub(super) fn pop_flushable(&mut self, req_epoch: Epoch, req_lsn: LSN) -> Option<FlushGroup> {
        if !self.wants_flush() {
            return None;
        }
        let mut to_flush = Vec::with_capacity(self.unflushed_old.len());
        while let Some((page_id, page)) = self.unflushed_old.pop_front_if(|(page_id, _page)| {
            let (page_epoch, page_end_lsn, _offset) = page_id.split();
            page_epoch < req_epoch || page_end_lsn + self.unflushable_lsn_window <= req_lsn
        }) {
            to_flush.push((page_id, page.take_data()));
        }
        Some(FlushGroup { id: self.id, to_flush })
    }

    pub fn fetch_entry(&self, epoch: Epoch, lsn: LSN) -> Result<(LogicalJournalOffset, Vec<u8>), FetchEntryError> {
        // First check if the epoch matches our current epoch
        if epoch != self.current_page.epoch {
            return Err(FetchEntryError::WrongEpoch(self.current_page.epoch));
        }

        // Check current page first
        if self.current_page.lsn_range().contains(&lsn) {
            return match self.current_page.get_entry_data_with_offset(lsn) {
                Some((offset, data)) => {
                    let logical_offset = self.offset_cumsum + offset as LogicalJournalOffset;
                    Ok((logical_offset, data))
                }
                None => Err(FetchEntryError::LSNNotFound),
            };
        }

        // Check unflushed old pages
        let mut page_offset_cumsum = 0u64;
        for (page_id, page) in &self.unflushed_old {
            if page_id.0 == epoch && page.lsn_range().contains(&lsn) {
                return match page.get_entry_data_with_offset(lsn) {
                    Some((offset, data)) => {
                        let logical_offset = page_offset_cumsum + offset as LogicalJournalOffset;
                        Ok((logical_offset, data))
                    }
                    None => Err(FetchEntryError::LSNNotFound),
                };
            }
            page_offset_cumsum += page.page_offset() as LogicalJournalOffset;
        }

        // LSN not found in any in-memory pages
        // It might be in S3 (flushed) or doesn't exist
        if lsn < self.current_page.lsn_range().start {
            Err(FetchEntryError::LSNTooOld)
        } else {
            Err(FetchEntryError::LSNNotFound)
        }
    }
}

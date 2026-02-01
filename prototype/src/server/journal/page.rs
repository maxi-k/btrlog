use std::ops::Range;

use crate::{
    config::{JournalPageMemoryConfig, platform_dependent},
    io::{ThreadBuffers, buffer::IoBuffer},
    server::wal::DiskLocation,
    types::{
        error::JournalPushError,
        id::{Epoch, JournalOffset, LSN},
    },
};

const JOURNAL_PAGE_SIZE: usize = 16 * 1024 * 1024; // 16MB
static_assertions::const_assert!((JournalOffset::max_value() as u64) > (JOURNAL_PAGE_SIZE as u64));

////////////////////////////////////////////////////////////////////////////////

fn local_page_pool() -> ThreadBuffers {
    let cfg: JournalPageMemoryConfig = platform_dependent();
    crate::io::local_buffer_pool(
        JOURNAL_PAGE_SIZE,
        crate::io::BufferPoolConfig {
            vm_size: cfg.virtual_memory_size,
            prealloc_size: JOURNAL_PAGE_SIZE * cfg.preallocated_page_count,
            hugepage: true,
        },
    )
}

pub(super) fn prepopulate_page_pool() {
    local_page_pool().pop();
}

////////////////////////////////////////////////////////////////////////////////

#[repr(align(8))]
pub(super) struct JournalPage {
    // everything that is required to get written to disk/s3
    data: IoBuffer<u8>,
    // additional metadata for fast operations
    free_slot_offset: JournalOffset,
    write_head: JournalOffset,
    _pin: std::marker::PhantomPinned,
}
static_assertions::assert_eq_align!(JournalPage, u64);

// information required per journal entry:
// - lsn
// - data [offset in JournalBlob]
//
// lsn is set to `INVALID_LSN` if it represents a hole;
// epoch changes open a new page, so no mixed-epoch pages
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct JournalSlot {
    lsn: LSN,
    offset: JournalOffset, // offset in blob
    size: JournalOffset,   // size
}
// Slot has expected compact binary representation
static_assertions::assert_eq_size!(JournalSlot, [u8; 16]);

#[derive(Copy, Clone)]
#[allow(dead_code)]
pub struct JournalEntry<'a> {
    context: &'a JournalPage,
    slot: JournalSlot,
}

// information required per journal page (also flushed to S3):
// - epoch
// - start lsn, end lsn (-> entry_count)
// - write head
pub struct JournalPageHeader {
    pub(super) epoch: Epoch,
    lsns: Range<LSN>,
    last_known_disk_location: DiskLocation,
}
// Header has expected compact binary representation
static_assertions::assert_eq_size!(JournalPageHeader, [u8; 32]);

impl JournalPageHeader {
    fn new_empty(epoch: Epoch, start_lsn: LSN) -> Self {
        Self {
            epoch,
            lsns: start_lsn..start_lsn,
            last_known_disk_location: DiskLocation::invalid(),
        }
    }

    pub(super) fn lsn_range(&self) -> Range<LSN> {
        self.lsns.clone()
    }

    pub(super) fn max_lsn(&self) -> LSN {
        self.lsns.end
    }

    fn entry_count(&self) -> JournalOffset {
        (self.lsns.end - self.lsns.start) as JournalOffset
    }

    fn set_max_lsn(&mut self, lsn: LSN) {
        self.lsns.end = lsn;
    }
}

impl JournalSlot {
    fn new(lsn: LSN, offset: JournalOffset, size: JournalOffset) -> Self {
        JournalSlot { lsn, offset, size }
    }

    fn invalid(lsn: LSN, offset: JournalOffset) -> Self {
        Self::new(lsn, offset, 0)
    }

    fn is_invalid(&self) -> bool {
        self.size == 0
    }

    #[allow(dead_code)]
    const fn byte_size() -> JournalOffset {
        size_of::<Self>() as JournalOffset
    }

    #[allow(dead_code)]
    fn end_byte(&self) -> JournalOffset {
        self.offset + self.size
    }
}

impl std::ops::Deref for JournalPage {
    type Target = JournalPageHeader;

    fn deref(&self) -> &Self::Target {
        self.header()
    }
}

////////////////////////////////////////////////////////////////////////////////

impl JournalPage {
    const MAX_BYTE: JournalOffset = JOURNAL_PAGE_SIZE as JournalOffset;
    const HEADER_START: JournalOffset = (Self::MAX_BYTE - size_of::<JournalPageHeader>() as JournalOffset);

    #[allow(dead_code)]
    const fn byte_size() -> usize {
        size_of::<Self>()
    }

    pub(super) fn alloc_empty(epoch: Epoch, start_lsn: LSN) -> Self {
        let mut res = Self {
            data: local_page_pool().pop(),
            free_slot_offset: Self::HEADER_START - JournalSlot::byte_size(),
            write_head: 0,
            _pin: std::marker::PhantomPinned,
        };
        *res.header_mut() = JournalPageHeader::new_empty(epoch, start_lsn);
        res
    }

    pub(super) fn alloc_fresh_log() -> Self {
        Self::alloc_empty(0, 0)
    }

    pub(super) fn take_data(self) -> IoBuffer {
        self.data
    }

    pub(super) fn push(
        &mut self,
        epoch: Epoch,
        lsn: LSN,
        offset: JournalOffset,
        entry: &[u8],
    ) -> Result<JournalOffset, JournalPushError> {
        self.check_epoch(epoch)?;
        let my_lsns = self.header().lsns.clone();
        if lsn < my_lsns.start {
            log::warn!("passed lsn {} < start lsn {}", lsn, my_lsns.start);
            return Err(JournalPushError::LsnOutsideOfRange);
        }
        let internal_index = (lsn - my_lsns.start) as JournalOffset;
        let slot_offset = self.slot_offset_at(internal_index);
        let data_len = entry.len() as JournalOffset;
        if !self.fits(entry, slot_offset) {
            return Err(JournalPushError::NotEnoughFreeSpace);
        };
        match slot_offset.cmp(&self.free_slot_offset) {
            // target offset < free slot -> push will produce holes
            std::cmp::Ordering::Less => {
                log::debug!("pushing lsn {} to too-large offset {}; will produce holes", lsn, slot_offset);
                debug_assert!(lsn > my_lsns.end);
                // passed offset has to be larger than write head, plus
                // *at least* one byte for each missing journal entry
                if offset <= (self.write_head + (lsn - my_lsns.end) as JournalOffset) {
                    log::warn!(
                        "passed offset {} at lsn {} produces holes, but would not allow enough free space for missing lsns",
                        offset,
                        lsn
                    );
                    return Err(JournalPushError::OffsetTooSmall);
                }
                for missing_lsn in my_lsns.end..lsn {
                    // push slots without pushing data
                    self.push_slot(JournalSlot::invalid(missing_lsn, self.write_head))
                }
                // then assume they take as much data space overall as
                self.write_head = offset;
                // XXX cleaner to replace with recursive call?
                self.push_slot(JournalSlot::new(lsn, offset, data_len));
                self.push_data(entry);
                self.header_mut().set_max_lsn(lsn + 1);
                debug_assert_eq!(self.write_head, offset + data_len);
                Ok(self.write_head)
            }
            // target offset == free slot -> push to end
            std::cmp::Ordering::Equal => {
                log::trace!("correct offset {} passed for lsn {}, appending", slot_offset, lsn);
                debug_assert!(lsn == my_lsns.end);
                match offset.cmp(&self.write_head) {
                    // offset has to match exactly
                    std::cmp::Ordering::Less => Err(JournalPushError::OffsetTooSmall),
                    std::cmp::Ordering::Greater => Err(JournalPushError::OffsetTooLarge),
                    std::cmp::Ordering::Equal => {
                        self.push_slot(JournalSlot::new(lsn, offset, data_len));
                        self.push_data(entry);
                        self.header_mut().set_max_lsn(lsn + 1);
                        debug_assert_eq!(self.write_head, offset + data_len);
                        Ok(self.write_head)
                    }
                }
            }
            // target offset > free slot -> push will replace lsn
            std::cmp::Ordering::Greater => {
                let tgt_slot = self.obj_at::<JournalSlot>(slot_offset);
                if !tgt_slot.is_invalid() {
                    log::trace!("trying to fill invalid data hole for lsn {} at offset {}", lsn, slot_offset);
                    if tgt_slot.size != data_len || tgt_slot.offset != offset {
                        log::warn!(
                            "lsn already seen and has different offset ({} vs existing {}) or size ({} vs {})",
                            offset,
                            tgt_slot.offset,
                            data_len,
                            tgt_slot.size
                        );
                        // give error back in case data != already seen data size
                        // -> something is wrong on the client side
                        Err(JournalPushError::LsnAlreadySeen)
                    } else {
                        // NO-OP: don't replace already seen data
                        Ok(offset + data_len)
                    }
                } else {
                    log::trace!(
                        "replacing invalid slot for lsn {} (aka hole); checking that data fits at offset {}",
                        lsn,
                        slot_offset
                    );
                    // replacing invalid slot (aka hole): check that data fits
                    let (lower_bound, upper_bound) = {
                        let slotidx = (self.header().entry_count() - internal_index - 1) as usize;
                        let slots = self.slots_as_slice();
                        let lower_bound = match slots[slotidx..].iter().find(|s| !s.is_invalid()) {
                            Some(s) => s.offset + s.size,
                            // we are now the first valid slot; other slots have to have
                            // at least one byte of data.
                            None => internal_index,
                        };
                        let upper_bound = match slots[..slotidx].iter().rev().find(|s| !s.is_invalid()) {
                            Some(s) => s.offset,
                            // we are now the last valid slot; valid slots after us have
                            // to have at lest one byte of data.
                            // XXX this should never happen b/c we don't produce invalid slots at
                            // the end of the journal page. panic here?
                            // None => self.free_slot_offset - internal_index
                            None => panic!("Patching hole at the end of Journal Page not permitted."),
                        };
                        (lower_bound, upper_bound)
                    };
                    if offset >= lower_bound && offset + data_len <= upper_bound {
                        let tgt_slot = self.mut_obj_at::<JournalSlot>(slot_offset);
                        *tgt_slot = JournalSlot::new(lsn, offset, data_len);
                        self[(offset as usize)..((offset + data_len) as usize)].copy_from_slice(entry);
                        // write head doesn't change
                        // XXX return write head instead of written-to offset here?
                        Ok(offset + data_len)
                    } else {
                        log::debug!(
                            "lsn {} at offset {} could overwrite existing data (lb {}, ub {})",
                            lsn,
                            offset,
                            lower_bound,
                            upper_bound
                        );
                        // refusing to patch hole that could override already written data
                        // XXX better error name?
                        Err(JournalPushError::OffsetTooSmall)
                    }
                }
            }
        }
    }

    pub(super) fn set_last_known_disk_location(&mut self, epoch: Epoch, lsn: LSN, loc: DiskLocation) {
        debug_assert_ne!(loc, DiskLocation::invalid());
        if epoch == self.epoch && lsn == self.lsn_range().end {
            self.header_mut().last_known_disk_location = loc;
        }
    }

    pub fn page_offset(&self) -> JournalOffset {
        self.write_head
    }

    fn push_slot(&mut self, slot: JournalSlot) {
        *self.free_slot() = slot;
        self.free_slot_offset -= JournalSlot::byte_size();
    }

    fn push_data(&mut self, data: &[u8]) {
        // XXX WAL LOG
        let start = self.write_head as usize;
        let len = data.len();
        self[start..(start + len)].copy_from_slice(data);
        self.write_head += len as JournalOffset;
    }

    fn check_epoch(&self, epoch: Epoch) -> Result<Epoch, JournalPushError> {
        match epoch.cmp(&self.header().epoch) {
            std::cmp::Ordering::Less => Err(JournalPushError::EpochTooSmall),
            std::cmp::Ordering::Greater => Err(JournalPushError::EpochTooLarge),
            std::cmp::Ordering::Equal => Ok(epoch),
        }
    }

    fn free_slot(&mut self) -> &mut JournalSlot {
        self.mut_obj_at(self.free_slot_offset)
    }

    fn header(&self) -> &JournalPageHeader {
        self.obj_at(Self::HEADER_START)
    }

    fn header_mut(&mut self) -> &mut JournalPageHeader {
        self.mut_obj_at(Self::HEADER_START)
    }

    fn fits(&self, data: &[u8], target_slot: JournalOffset) -> bool {
        (target_slot as isize) - (self.write_head as isize) > (data.len() as isize)
    }

    #[allow(dead_code)]
    fn free_data_space(&self) -> usize {
        let dist = self.free_slot_offset - self.write_head + JournalSlot::byte_size();
        debug_assert!(dist > 0);
        dist as usize
    }

    fn slot_offset_at(&self, idx: u32) -> JournalOffset {
        Self::HEADER_START - ((idx + 1) * JournalSlot::byte_size())
    }

    #[allow(dead_code)]
    fn slot_at(&self, idx: u32) -> (&JournalSlot, JournalOffset) {
        let start = self.slot_offset_at(idx);
        (self.obj_at(start), start)
    }

    fn obj_at<T: Sized>(&self, start: JournalOffset) -> &T {
        debug_assert!(start + (size_of::<JournalOffset>() as JournalOffset) < Self::MAX_BYTE);
        unsafe { std::mem::transmute::<&u8, &T>(&self[start as usize]) }
    }

    fn mut_obj_at<T: Sized>(&mut self, start: JournalOffset) -> &mut T {
        debug_assert!(start + (size_of::<JournalOffset>() as JournalOffset) < Self::MAX_BYTE);
        unsafe { std::mem::transmute::<&mut u8, &mut T>(&mut self[start as usize]) }
    }

    #[allow(dead_code)]
    fn obj_range<T: Sized>(start: usize) -> Range<usize> {
        start..(start + size_of::<T>())
    }

    fn slots_as_slice(&self) -> &[JournalSlot] {
        let cnt = self.header().entry_count();
        if cnt == 0 {
            return &[];
        }
        let start: *const u8 = &self[self.slot_offset_at(cnt - 1) as usize];
        unsafe { std::slice::from_raw_parts(start as *const JournalSlot, cnt as usize) }
    }

    pub(super) fn get_entry_data_with_offset(&self, lsn: LSN) -> Option<(JournalOffset, Vec<u8>)> {
        let lsn_range = self.header().lsn_range();
        if !lsn_range.contains(&lsn) {
            return None;
        }

        let index = (lsn - lsn_range.start) as usize;
        let slots = self.slots_as_slice();

        if index >= slots.len() {
            return None;
        }

        let slot = &slots[index];
        if slot.is_invalid() {
            return None;
        }

        let start = slot.offset as usize;
        let end = start + slot.size as usize;
        Some((slot.offset, self[start..end].to_vec()))
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<Idx: std::slice::SliceIndex<[u8]>> std::ops::Index<Idx> for JournalPage {
    type Output = Idx::Output;

    #[inline]
    fn index(&self, index: Idx) -> &Self::Output {
        &self.data.data()[index]
    }
}

impl<Idx: std::slice::SliceIndex<[u8]>> std::ops::IndexMut<Idx> for JournalPage {
    #[inline]
    fn index_mut(&mut self, index: Idx) -> &mut Self::Output {
        &mut self.data.data_mut()[index]
    }
}

////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_page() {
        let p = JournalPage::alloc_empty(10, 100);
        let h = p.header();
        assert_eq!(h.epoch, 10);
        assert_eq!(h.lsns.start, 100);
        assert!(h.lsns.is_empty());
    }

    #[test]
    fn push_empty_happy_path() {
        let mut p = JournalPage::alloc_fresh_log();
        let data = vec![1; 100];
        let res = p.push(0, 0, 0, data.as_slice());
        // data was successfuly stored, new offset is 0
        assert_eq!(res, Ok(100));
    }

    #[test]
    fn push_nonempty_happy_path() {
        let mut p = JournalPage::alloc_fresh_log();
        let data1 = vec![1; 100];
        let data2 = vec![1; 50];
        let res1 = p.push(0, 0, 0, data1.as_slice());
        // data was successfuly stored, new offset is 100
        assert_eq!(res1, Ok(100));
        let res2 = p.push(0, 1, res1.unwrap(), data2.as_slice());
        assert_eq!(res2, Ok(150));
    }

    #[test]
    fn push_nonempty_wrong_offset() {
        let mut p = JournalPage::alloc_fresh_log();
        let data1 = vec![1; 100];
        let data2 = vec![1; 50];
        let res1 = p.push(0, 0, 0, data1.as_slice());
        assert_eq!(res1, Ok(100));
        let res2 = p.push(0, 1, 0, data2.as_slice());
        assert_eq!(res2, Err(JournalPushError::OffsetTooSmall));
        assert_eq!(p.write_head, 100);
        assert_eq!(p.header().lsns.end, 1);
        let res3 = p.push(0, 1, 200, data2.as_slice());
        assert_eq!(res3, Err(JournalPushError::OffsetTooLarge));
    }

    #[test]
    fn push_with_holes() {
        let mut p = JournalPage::alloc_fresh_log();
        {
            let data1 = vec![1; 100];
            let res1 = p.push(0, 2, 100, data1.as_slice());
            assert_eq!(res1, Ok(200));
            assert_eq!(p.header().lsns.end, 3);
            assert_eq!(p.write_head, 200);
        }
        {
            let data2 = vec![1; 50];
            let res2 = p.push(0, 4, 300, data2.as_slice());
            assert_eq!(res2, Ok(350));
            assert_eq!(p.header().lsns.end, 5);
            assert_eq!(p.write_head, 350);
        }
    }

    #[test]
    fn push_patch_holes_at_start() {
        let mut p = JournalPage::alloc_fresh_log();
        {
            let data1 = vec![1; 100];
            let res1 = p.push(0, 1, 50, data1.as_slice());
            assert_eq!(res1, Ok(150));
            assert_eq!(p.header().lsns.end, 2);
            assert_eq!(p.write_head, 150);
        }
        {
            let data2 = vec![2; 50];
            let res2 = p.push(0, 0, 0, data2.as_slice());
            assert_eq!(res2, Ok(50));
            assert_eq!(p.header().lsns.end, 2);
            assert_eq!(p.write_head, 150);
        }
        // data is correct
        assert_eq!(p[0..50], [2; 50]);
        assert_eq!(p[50..150], [1; 100]);
    }

    #[test]
    fn push_patch_holes_in_middle() {
        let mut p = JournalPage::alloc_fresh_log();
        {
            let data1 = vec![1; 100];
            let res1 = p.push(0, 0, 0, data1.as_slice());
            assert_eq!(res1, Ok(100));
        }
        {
            let data2 = vec![3; 50];
            let res2 = p.push(0, 2, 250, data2.as_slice());
            assert_eq!(res2, Ok(300));
        }
        {
            let data3 = vec![2; 150];
            let res3 = p.push(0, 1, 100, data3.as_slice());
            assert_eq!(res3, Ok(250));
        }
        assert_eq!(p.header().lsns.end, 3);
        assert_eq!(p.write_head, 300);
        // data is correct
        assert_eq!(p[0..100], [1; 100]);
        assert_eq!(p[100..250], [2; 150]);
        assert_eq!(p[250..300], [3; 50]);
    }

    #[test]
    fn push_complains_about_epoch() {
        {
            let mut p = JournalPage::alloc_fresh_log();
            let data = vec![3; 50];
            let res = p.push(1, 0, 0, data.as_slice());
            assert_eq!(res, Err(JournalPushError::EpochTooLarge));
        }
        {
            let mut p = JournalPage::alloc_empty(10, 1000);
            let data = vec![3; 50];
            let res = p.push(9, 0, 0, data.as_slice());
            assert_eq!(res, Err(JournalPushError::EpochTooSmall));
        }
    }

    #[test]
    fn push_complains_about_lsn() {
        {
            let mut p = JournalPage::alloc_empty(0, 5);
            let data = vec![3; 50];
            let res = p.push(0, 0, 0, data.as_slice());
            assert_eq!(res, Err(JournalPushError::LsnOutsideOfRange));
        }
    }

    #[test]
    fn journal_page_alloc_perf() {
        use perf_event_block::*;
        {
            let buf = local_page_pool();
            assert!(buf.pop().data().len() >= JOURNAL_PAGE_SIZE);
        }
        let scale = 100;
        let mut res = Vec::with_capacity(scale);
        {
            let _perf = PerfEventBlock::default_params(scale as u64, true);
            for i in 0..scale {
                res.push(JournalPage::alloc_empty(i as u64, 100 + i as u64));
            }
            std::mem::drop(_perf);
        }
        for (i, p) in res.iter().enumerate() {
            assert_eq!(p.header().epoch, i as u64);
            assert_eq!(p.header().max_lsn(), 100 + i as u64);
        }
    }
}

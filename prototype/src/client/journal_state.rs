use std::pin::Pin;

use crate::{
    runtime::waker_chain::{WakerChainNode, WakerGuard, WakerList},
    types::{
        error::JournalPushError,
        id::{Epoch, JournalId, LSN, LogicalJournalOffset},
        message::*,
    },
};

use super::ForCluster;

////////////////////////////////////////////////////////////////////////////////
//  Client-side state for a single journal

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalPosition {
    pub epoch: Epoch, // XXX need here?
    pub lsn: LSN,
    pub offset: LogicalJournalOffset,
}

impl From<&AddEntryResponse> for WalPosition {
    fn from(value: &AddEntryResponse) -> Self {
        Self {
            epoch: value.epoch,
            lsn: value.lsn,
            offset: *value.result.as_ref().unwrap(),
        }
    }
}

impl WalPosition {
    fn new_empty() -> Self {
        Self {
            epoch: 0,
            lsn: LSN::MAX,
            offset: 0,
        }
    }

    fn new_for_existing(meta: &JournalMetadata) -> Self {
        Self {
            epoch: meta.current_epoch,
            lsn: meta.max_known_lsn,
            offset: meta.max_known_offset,
        }
    }

    fn optimistic_from_committed(self) -> Self {
        Self {
            epoch: self.epoch,
            lsn: self.lsn.wrapping_add(1),
            offset: self.offset,
        }
    }

    fn is_next_entry(&self, next_lsn: LSN) -> bool {
        next_lsn == self.lsn.wrapping_add(1)
    }

    fn inc(&mut self, offset: LogicalJournalOffset) -> Self {
        let cur = self.clone();
        self.lsn = self.lsn.wrapping_add(1);
        self.offset = self.offset.wrapping_add(offset);
        cur
    }
}

#[derive(Debug)]
pub enum WalRequestStatus {
    Ok,
    FatalFailure,
}

// hide impl detail
#[derive(Debug)]
pub struct WalRequestQueue {
    queue: WakerList,
    // pending_head: Cell<*const WakerChainNode>
}
impl WalRequestQueue {
    pub fn new() -> Self {
        let queue = WakerList::new();
        // let sentinel = unsafe { queue.sentinel() };
        Self {
            queue,
            // pending_head: Cell::new(sentinel)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Client-side logical state for a single journal

#[derive(Debug)]
pub struct JournalClientState {
    id: JournalId,
    committed: WalPosition,
    optimistic_head: WalPosition,
    last_completed: WalRequestStatus,
    reply_to: ForCluster<u16>,
    lsn_window: u64,
}

impl JournalClientState {
    pub fn new_empty(id: JournalId, ports: ForCluster<u16>, lsn_window: u64) -> JournalClientState {
        let committed = WalPosition::new_empty();
        Self {
            id,
            committed,
            optimistic_head: committed.optimistic_from_committed(),
            last_completed: WalRequestStatus::Ok,
            reply_to: ports,
            lsn_window,
        }
    }

    pub fn from_existing(meta: JournalMetadata, ports: ForCluster<u16>, lsn_window: u64) -> Self {
        let committed = WalPosition::new_for_existing(&meta);
        Self {
            id: *meta.id,
            committed,
            optimistic_head: committed.optimistic_from_committed(),
            last_completed: WalRequestStatus::Ok,
            reply_to: ports,
            lsn_window,
        }
    }

    pub fn id(&self) -> JournalId {
        self.id
    }

    pub fn reply_to_iter(&self) -> impl Iterator<Item = &u16> {
        self.reply_to.iter()
    }

    fn mark_completed(&mut self, status: WalRequestStatus, pos: WalPosition) {
        self.last_completed = status;
        match self.last_completed {
            WalRequestStatus::Ok => {
                assert!(self.committed.is_next_entry(pos.lsn));
                assert!(self.committed.epoch == pos.epoch);
                self.committed.lsn = pos.lsn;
                self.committed.offset = pos.offset;
            }
            WalRequestStatus::FatalFailure => {
                log::error!("request completed with fatalfailure, not updating commited wal position")
            }
        }
    }

    pub fn is_request_allowed(&self, pos: &WalPosition) -> bool {
        let max_pending = self.committed.lsn.wrapping_add(self.lsn_window) as u64;
        pos.lsn <= max_pending && pos.epoch == self.committed.epoch
    }

    pub fn allocate_optimistic_request(&mut self, size: LogicalJournalOffset) -> WalPosition {
        self.optimistic_head.inc(size)
    }

    pub fn optimistic_head(&self) -> &WalPosition {
        &self.optimistic_head
    }

    pub fn committed_head(&self) -> &WalPosition {
        &self.committed
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Client Queue behavior

pin_project_lite::pin_project! {
    #[derive(Debug)]
    pub struct WalSlotGuard<'a> {
        state: &'a mut JournalClientState,
        queue: &'a WalRequestQueue,
        #[pin] sequencer: WakerGuard<'a>,
        finalized: bool
    }

    impl<'a> PinnedDrop for WalSlotGuard<'a> {
        fn drop(mut this: Pin<&mut Self>) {
            debug_assert!(this.finalized);
        }
    }
}

impl<'a> WalSlotGuard<'a> {
    pub fn new(state: &'a mut JournalClientState, queue: &'a WalRequestQueue) -> Self {
        let sequencer = queue.queue.create_guard();
        Self {
            state,
            queue,
            sequencer,
            finalized: false,
        }
    }

    pub fn state(&self) -> &JournalClientState {
        self.state
    }

    pub fn get_wal_slot(self: &mut Pin<&mut Self>, size: LogicalJournalOffset) -> Result<WalPosition, ()> {
        let this = self.as_mut().project();
        let wal_pos = this.state.allocate_optimistic_request(size);
        if this.state.is_request_allowed(&wal_pos) {
            this.sequencer.link(); // insert into the chain now that we have an LSN
            Ok(wal_pos)
        } else {
            Err(())
            // wait for wal position to become free
            // let node = unsafe { this.sequencer.get_waker_node() };
            // this.queue.pending_head.set(node);
            // this.sequencer.wait_for_condition(|| this.state.is_request_allowed(&wal_pos)).await;
            // if this.queue.pending_head.get() == node {
            //     this.queue.pending_head.set(unsafe { &*node }.get_prev());
            // }
            // return wal_pos
        }
    }

    pub async fn ensure_completion_sequence(mut self: Pin<&mut Self>, resp: &AddEntryResponse) {
        // 'fast path': request completed in order; wake others in `drop`
        if self.state.committed.is_next_entry(resp.lsn) {
            self.complete(resp);
            return;
        }
        // request completed out of order; we have to wait with completion until in-sequence lsn request completes
        let this = self.as_mut().project();
        this.sequencer.wait_for_condition(|| this.state.committed.is_next_entry(resp.lsn)).await;
        self.complete(resp);
    }

    fn complete(self: Pin<&mut Self>, resp: &AddEntryResponse) {
        let status = self.status_from_response(resp);
        let this = self.project();
        debug_assert!(this.state.committed.is_next_entry(resp.lsn));
        debug_assert!(this.sequencer.am_last());
        this.state.mark_completed(status, resp.into());
        // wake any next pending request (which may be completed already)
        this.sequencer.pop_wake_ensure_fifo();
        *this.finalized = true;
    }

    fn status_from_response(&self, resp: &AddEntryResponse) -> WalRequestStatus {
        match resp.result {
            Ok(_offset) => WalRequestStatus::Ok,
            Err(_) => WalRequestStatus::FatalFailure,
        }
    }

    pub fn check(&self, resp: &AddEntryResponse) -> bool {
        use JournalPushError::*;
        match &resp.result {
            Ok(_offset) => {
                log::trace!("got ok response with offset {} for message {:?}", _offset, resp);
                true
            }
            Err(e) => {
                log::warn!("got retryable error {:?} for message {:?}", e, resp);
                match e {
                    // TODO return code that can abort this op, try to fence etc.
                    LsnAlreadySeen => true,    // duplicate message
                    LsnOutsideOfRange => true, // already flushed to S3
                    // EpochTooSmall => todo!(),
                    // EpochTooLarge => todo!(),
                    // OffsetTooSmall => todo!(),
                    // OffsetTooLarge => todo!(),
                    _ => false,
                }
            }
        }
    }

    pub fn set_reply_to(&mut self, id: usize, reply_to: u16) {
        self.state.reply_to[id] = reply_to;
    }

    // // XXX avoid copy
    pub fn make_request(&self, pos: &WalPosition, data: &[u8]) -> AddEntryRequest {
        AddEntryRequest {
            id: self.state.id.into(),
            epoch: pos.epoch,
            lsn: pos.lsn,
            offset: pos.offset,
            payload: data.to_vec(),
        }
    }

    pub fn make_request_from_vec(&self, pos: &WalPosition, payload: Vec<u8>) -> AddEntryRequest {
        AddEntryRequest {
            id: self.state.id.into(),
            epoch: pos.epoch,
            lsn: pos.lsn,
            offset: pos.offset,
            payload,
        }
    }
}

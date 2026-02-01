use io_uring::types::FsyncFlags;
use io_uring::{IoUring, opcode, squeue, types};
#[cfg(feature = "trace-requests")]
use reqtrace::CycleMeasurement;
use reqtrace::FrequencyScale;
use socket2::{Domain, SockAddr, Socket, Type};
use std::cell::{Cell, UnsafeCell};
use std::ffi::CString;
use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::config::IOConfig;
use crate::runtime::rcwaker::RcWaker;
use crate::runtime::waker_chain::{WakerGuard, WakerList};

use super::buffer::IoBuf;

pub struct PendingOp {
    /// Unique operation ID for ABA problem detection (0 = empty slot)
    uid: OpUid,
    epoch: usize,
    buffer: Option<IoBuf>,
    waker: Option<Waker>,
    pub result: Option<i32>,
    #[cfg(feature = "trace-requests")]
    duration_trace: u64,
}

pub(super) enum OpStage {
    Unsubmitted,
    Submitted,
    Completed,
    Invalid,
}

impl PendingOp {
    const fn empty(epoch: usize) -> Self {
        Self {
            uid: 0,
            epoch,
            buffer: None,
            waker: None,
            result: None,
            #[cfg(feature = "trace-requests")]
            duration_trace: 0,
        }
    }

    pub(super) fn buffer_ref(&mut self) -> Option<&mut IoBuf> {
        self.buffer.as_mut()
    }

    pub(super) fn set_waker(&mut self, waker: Waker) {
        let _old = self.waker.replace(waker);
    }

    pub(super) fn op_uid(&self) -> OpUid {
        self.uid
    }

    #[inline]
    pub(super) fn get_stage(&self, expected_id: OpUid, current_epoch: usize) -> OpStage {
        if expected_id != self.uid {
            return OpStage::Invalid;
        }
        if self.result.is_some() {
            return OpStage::Completed;
        }
        if current_epoch > self.epoch {
            return OpStage::Submitted;
        }
        return OpStage::Unsubmitted;
    }
}

/// Opaque identifier for a uring operation, containing slot index and operation ID
pub type OpUid = u32;
#[derive(Copy, Clone, Debug, Default)]
pub struct OpId {
    uid: u32,
    slot: u16,
}

impl OpId {
    const OP_UID_MASK: u64 = u32::MAX as u64;
    const RESERVED_UID_COUNT: u32 = 2;
    const MAX_OP_UID: u32 = u32::MAX - Self::RESERVED_UID_COUNT;
    const OP_UID_SHIFT: u64 = 32;

    const fn get_reserved_opid(idx: u32) -> u64 {
        if idx >= Self::MAX_OP_UID {
            panic!()
        }
        Self {
            uid: u32::MAX - idx,
            slot: u16::MAX,
        }
        .into_user_data()
    }

    /// Create a new OpId from slot and uid (internal use only)
    #[inline]
    pub(super) const fn new(slot: u16, uid: OpUid) -> Self {
        Self { uid, slot }
    }

    /// Pack into user_data for io_uring submission
    #[inline]
    pub(super) const fn into_user_data(self) -> u64 {
        ((self.uid as u64 & Self::OP_UID_MASK) << Self::OP_UID_SHIFT) | (self.slot as u64)
    }

    /// Unpack user_data into OpId
    #[inline]
    const fn from_user_data(user_data: u64) -> Self {
        let slot = (user_data & 0xFFFF) as u16;
        let uid = (user_data >> Self::OP_UID_SHIFT) as u32;
        Self { slot, uid }
    }

    /// Get the slot index (internal use only)
    #[inline]
    pub(super) fn slot(self) -> u16 {
        self.slot
    }

    /// Get the operation ID (internal use only)
    #[inline]
    pub(super) fn uid(self) -> OpUid {
        self.uid
    }
}

////////////////////////////////////////////////////////////////////////////////
#[derive(Debug, Clone, Copy)]
pub enum OpCompletionAction {
    ReuseSlot, // the consumer will(has) reuse(d) the slot; do nothing with it
    TryWake,   // we should try to use the waker to wake the task
    Drop,      // we should drop the pending op without waking
}

#[derive(Debug, Clone, Copy)]
pub enum UringPollMode {
    AutoPoll { idle_time: Duration, cpu_affinity: Option<u32> },
    ManualPoll,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum UringTaskRunMode {
    #[default]
    Default,
    Defer,
    Coop,
}

#[derive(Clone, Debug)]
pub struct UringConfig {
    pub poll_mode: UringPollMode,
    pub taskrun_mode: UringTaskRunMode,
    pub sq_entries: u32,
    pub cq_entries: u32,
    pub min_submit_frequency_us: usize,
    pub syscall_timeout_us: usize,
    pub allow_parking: bool,
    pub print_slow_submits: bool,
}
impl UringConfig {
    pub fn with<F: FnOnce(&mut Self)>(mut self, f: F) -> Self {
        f(&mut self);
        self
    }
    pub fn default_with<F: FnOnce(&mut Self)>(f: F) -> Self {
        let mut x = Self::default();
        f(&mut x);
        x
    }

    pub fn normalize(&mut self) {
        self.sq_entries = self.sq_entries.min(4096);
        self.cq_entries = self.sq_entries.min(65536 / 2);
    }
}

impl Default for UringConfig {
    fn default() -> Self {
        Self {
            poll_mode: UringPollMode::ManualPoll,
            // poll_mode: UringPollMode::AutoPoll {
            //     idle_time: Duration::from_millis(100),
            //     cpu_affinity: None,
            // },
            taskrun_mode: Default::default(),
            sq_entries: 1024,
            cq_entries: 4096,
            min_submit_frequency_us: 5,
            syscall_timeout_us: 100,
            allow_parking: true,
            print_slow_submits: true,
        }
    }
}

impl From<(crate::config::IOMode, usize)> for UringPollMode {
    fn from((mode, core): (crate::config::IOMode, usize)) -> Self {
        use crate::config::IOMode::*;
        match mode {
            ManualPoll => UringPollMode::ManualPoll,
            Coop => UringPollMode::ManualPoll,
            AutoPoll => UringPollMode::AutoPoll {
                idle_time: Duration::from_millis(100),
                cpu_affinity: None,
            },
            AutoPollPinned => UringPollMode::AutoPoll {
                idle_time: Duration::from_millis(100),
                cpu_affinity: Some(crate::config::get_hyperthread_for_core(core) as u32),
            },
        }
    }
}

impl From<(&IOConfig, usize)> for UringConfig {
    fn from((cfg, core): (&IOConfig, usize)) -> Self {
        Self::default_with(|s| {
            s.sq_entries = cfg.io_depth as u32;
            s.cq_entries = (cfg.io_depth * 2) as u32;
            s.poll_mode = (cfg.io_mode, core).into();
            if cfg.io_mode == crate::config::IOMode::Coop {
                s.taskrun_mode = UringTaskRunMode::Coop;
            }
            s.allow_parking = cfg.io_wait_threshold > 0;
            s.print_slow_submits = false;
        })
    }
}

impl From<u32> for UringConfig {
    fn from(value: u32) -> Self {
        assert!(value <= 4096, "uring size must be <= 4096");
        Self::default_with(|s| {
            s.sq_entries = value;
            s.cq_entries = 2 * value;
        })
    }
}

////////////////////////////////////////////////////////////////////////////////

// when entering the kernel, we need to know:
// - are we allowed to wait? (prevent indefinete waites)
// - should we enter if it isn't necessary?
#[derive(Debug, Clone, Copy)]
pub enum IOEnterIntent {
    Poll,    // we're just checking whether to submit anything or anything is completed
    Submit,  // we want to submit something, without checking completions
    Starved, // we don't have tasks to process - enter kernel and wait if possible
}
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct IOStats {
    global_start_time: reqtrace::MicrosecondMeasurement,
    last_submit: reqtrace::MicrosecondMeasurement,
    outstanding_ios: u16,
    submission_epoch: usize,
    syscalls: usize,
    syscall_errors: usize,
    intents_starved: usize,
    intents_polling: usize,
    intents_submitting: usize,
    total_submitted: usize,
    waits: usize,
    doorbell_rings: usize,
    enter_timeouts: usize,
    total_syscall_us: f64,
    last_known_cq_batch_size: usize
}

struct DoorbellState {
    fd: OwnedFd,
    eventfd_buffer: [u8; 8],
    eventfd_armed: bool,
}
impl DoorbellState {
    fn new() -> io::Result<Self> {
        let fd = unsafe { libc::eventfd(0, 0) };
        if fd < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(Self {
                fd: unsafe { OwnedFd::from_raw_fd(fd) },
                eventfd_buffer: [0u8; 8],
                eventfd_armed: false,
            })
        }
    }

    fn create_op(&mut self, ud: u64) -> Option<squeue::Entry> {
        if self.eventfd_armed {
            None
        } else {
            let ptr = self.eventfd_buffer.as_mut_ptr();
            self.eventfd_armed = true;
            Some(opcode::Read::new(types::Fd(self.fd.as_raw_fd()), ptr, 8).build().user_data(ud))
        }
    }
}

/// Wrapper around io_uring that integrates with async runtimes
pub struct UringContext {
    ring: IoUring,
    config: UringConfig,
    /// Fixed-size array of pending operations (size = uring entries)
    /// op_id == 0 indicates an empty/free slot
    slots: Vec<PendingOp>,
    /// Stack of free slot indices
    free_slots: Vec<u16>,
    /// Monotonically increasing operation ID counter (starts at 1, 0 is reserved for empty slots)
    next_op_id: OpUid,
    /// Precalculated threshold for manual submit mode
    submit_threshold: usize,
    /// IO state
    stats: IOStats,
    /// EventFd buffer & state
    doorbell: DoorbellState,
    /// Allow waking once objects have been submitted
    submission_wakers: WakerList,
    /// idempotency for completion handling
    am_in_completion: Cell<bool>
}

impl Drop for UringContext {
    fn drop(&mut self) {
        self.print_stats();
    }
}

impl UringContext {
    const MAX_SUBMIT_MICROS: f64 = 5.0;
    const EVENTFD_USER_DATA: u64 = OpId::get_reserved_opid(0);

    fn new(mut cfg: UringConfig) -> io::Result<Self> {
        cfg.normalize();
        // Initialize fixed-size slots array with empty PendingOps (uid = 0)
        let mut slots = Vec::with_capacity(cfg.cq_entries as usize);
        for _ in 0..cfg.cq_entries {
            slots.push(PendingOp::empty(0));
        }

        // Initialize free slots stack with all indices
        let free_slots: Vec<u16> = (0..cfg.cq_entries as u16).collect();

        let mut builder = IoUring::builder();

        // we're always single issuer
        builder.setup_single_issuer();
        // might have more CQ than SQ entries
        if cfg.cq_entries >= cfg.sq_entries {
            builder.setup_cqsize(cfg.cq_entries);
        }
        // submit all requests, even if one errors
        builder.setup_submit_all();

        // set poll mode
        match cfg.poll_mode {
            UringPollMode::ManualPoll => {}
            UringPollMode::AutoPoll { idle_time, cpu_affinity } => {
                builder.setup_sqpoll(idle_time.as_millis() as u32);
                if let Some(cpu) = cpu_affinity {
                    builder.setup_sqpoll_cpu(cpu);
                }
            }
        }

        // set task run mode
        match cfg.taskrun_mode {
            UringTaskRunMode::Default => {}
            UringTaskRunMode::Defer => {
                builder.setup_defer_taskrun();
            }
            UringTaskRunMode::Coop => {
                builder.setup_coop_taskrun();
            }
        }
        let submit_threshold = (cfg.sq_entries / 4).min(1) as usize;
        let mut res = Self {
            ring: builder.build(cfg.sq_entries)?,
            config: cfg,
            slots,
            free_slots,
            next_op_id: 1, // Start at 1, 0 is reserved for empty slots
            submit_threshold,
            stats: IOStats::default(),
            doorbell: DoorbellState::new()?,
            submission_wakers: WakerList::new(),
            am_in_completion: Cell::new(false)
        };
        res.stats.global_start_time.start_measurement();
        Ok(res)
    }

    pub(super) fn iodepth(&self) -> usize {
        self.config.cq_entries as usize
    }

    fn try_arm_eventfd(&mut self) -> bool {
        if let Some(sqe) = self.doorbell.create_op(Self::EVENTFD_USER_DATA) {
            // enqueue to uring (not using enqueue, b/c it would update statistics about ops)
            // enqueue failed
            unsafe { self.ring.submission().push(&sqe) }.is_ok()
        } else {
            // already armed
            true
        }
    }

    fn print_stats(&self) {
        let s = &self.stats;
        let now = s.global_start_time.stop_measurement();
        log::debug!(
            "[T{:?}] uring stats: {} syscalls, {} waiting ({} poll, {} starve, {} submit), {} timed out, {} errors, {} doorbell rings, {} submitted (= {} per call) totaling {}us of {}us ({}%, {}us per call)",
            std::thread::current().id(),
            s.syscalls,
            s.waits,
            s.intents_polling,
            s.intents_starved,
            s.intents_submitting,
            s.enter_timeouts,
            s.syscall_errors,
            s.doorbell_rings,
            s.total_submitted,
            s.total_submitted as f64 / s.syscalls as f64,
            s.total_syscall_us,
            now,
            100.0 * (s.total_syscall_us / now),
            s.total_syscall_us / s.syscalls as f64
        );
    }

    #[inline]
    fn submit_inner(&mut self, _intent: IOEnterIntent, wait: usize) -> io::Result<usize> {
        self.stats.last_submit.start_measurement(); // rdtsc; required for time_based enter
        self.stats.syscalls += 1;
        let res = if std::hint::unlikely(wait != 0 && self.try_arm_eventfd()) {
            self.stats.waits += 1;
            // self.ring.submitter().submit()
            self.ring.submitter().submit_and_wait(1)
            // const SUBMIT_TIMEOUT: types::Timespec = types::Timespec::new().sec(0).nsec(1000 * 50);
            // let args = SubmitArgs::new().timespec(&SUBMIT_TIMEOUT);
            // match self.ring.submitter().submit_with_args(wait, &args) {
            //     Err(timeout_err) if timeout_err.raw_os_error() == Some(libc::ETIME) => {
            //         self.stats.enter_timeouts += 1;
            //         Ok(0)
            //     }
            //     x => x,
            // }
        } else {
            // arming eventfd failed or wait == 0 -> submit w/o wait
            self.ring.submitter().submit()
        };
        self.stats.submission_epoch += 1;
        self.submission_wakers.wake_all();
        if let Ok(submitted) = res {
            self.stats.total_submitted += submitted;
        } else {
            self.stats.syscall_errors += 1;
        }
        #[cfg(feature = "trace-requests")]
        {
            let syscall_duration = self.stats.last_submit.stop_measurement();
            self.stats.total_syscall_us += syscall_duration;
            if self.config.print_slow_submits && wait == 0 && syscall_duration > 40.0 {
                log::trace!(
                    "long uring syscall #{}: {}us, {:?}, {}, {:?}",
                    self.stats.syscalls,
                    syscall_duration,
                    _intent,
                    wait,
                    res
                );
            }
        }
        // if wait > 0 || self.stats.syscalls % (10 * 1000) == 0 {
        //     self.print_stats();
        // }
        res
    }

    /// Submit operations without blocking
    pub fn enter(&mut self, intent: IOEnterIntent) -> io::Result<usize> {
        match intent {
            IOEnterIntent::Poll => {
                self.stats.intents_polling += 1;
                let had_completions = self.poll_completions()?;
                let enter = match self.config.poll_mode {
                    UringPollMode::AutoPoll {
                        idle_time: _,
                        cpu_affinity: _,
                    } => had_completions == 0,
                    UringPollMode::ManualPoll => {
                        let batch_based = self.ring.submission().len() >= self.submit_threshold;
                        // XXX do this? don't enter if there were completions even if other conditions fulfilled?
                        let time_based = self.stats.last_submit.stop_measurement() >= Self::MAX_SUBMIT_MICROS;
                        batch_based || time_based
                    }
                };
                if enter { self.submit_inner(intent, 0) } else { Ok(0) }
            }
            IOEnterIntent::Starved => {
                self.stats.intents_starved += 1;
                if self.poll_completions()? > 0 {
                    return Ok(0);
                }
                let will_be_woken = if self.config.allow_parking {
                    self.stats.outstanding_ios
                } else {
                    0
                };
                self.submit_inner(intent, will_be_woken as usize)
            }
            IOEnterIntent::Submit => {
                self.stats.intents_submitting += 1;
                self.submit_inner(intent, 0)
            }
        }
    }

    fn request_unpark(&self) {
        // XXX async on other thread? how fast is eventfd write?
        let mut buf = [1u8; 8];
        let res = unsafe { libc::write(self.doorbell.fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, 8) };
        if res < 0 {
            panic!("eventfd write error error while unparking thread: {:?}", std::io::Error::last_os_error());
        }
    }

    #[inline]
    pub(super) fn enqueue(&mut self, e: squeue::Entry) -> Result<(), squeue::Entry> {
        match unsafe { self.ring.submission().push(&e) } {
            Ok(_) => {
                self.stats.outstanding_ios += 1;
                Ok(())
            }
            Err(_) => Err(e),
        }
    }

    #[inline]
    pub(super) fn enqueue_retry_once(&mut self, e: squeue::Entry) -> Result<(), squeue::Entry> {
        if let Err(e) = self.enqueue(e) {
            // couldn't submit; force flush, retry
            log::debug!("uring enqueuing failed with {:?}, retrying once", e);
            self.ring
                .submitter()
                .squeue_wait()
                .expect("error waiting for submission queue to have free entries");
            self.enqueue(e)
        } else {
            Ok(())
        }
    }

    /// Poll for completions without blocking
    #[inline]
    pub(super) fn poll_completions_callback<F>(&mut self, f: F) -> io::Result<usize>
    where
        F: Fn(OpId, i32, &mut PendingOp) -> OpCompletionAction,
    {
        // idempotency
        if self.am_in_completion.replace(true) {
            log::warn!("entering CQ while harvesting!");
            return Ok(0)
        }
        // Process all available completions without blocking
        let mut cq = self.ring.completion();
        let len = cq.len();
        self.stats.last_known_cq_batch_size = len;
        #[cfg(feature = "trace-requests")]
        let harvest_start_time = CycleMeasurement::now();
        while let Some(cqe) = cq.next() {
            let user_data = cqe.user_data();
            // check whether it's an internal completion
            if std::hint::unlikely(user_data == Self::EVENTFD_USER_DATA) {
                self.doorbell.eventfd_armed = false;
                self.stats.doorbell_rings += 1;
                continue;
            }
            // it's regular I/O; update tracking, cast to I/O slot, callback
            self.stats.outstanding_ios -= 1;
            let result = cqe.result();
            let op_id = OpId::from_user_data(user_data);
            debug_assert!(op_id.uid() > 0 && op_id.uid() <= OpId::MAX_OP_UID);
            // Direct access to the slot
            if let Some(pending_op) = self.slots.get_mut(op_id.slot() as usize) {
                // Check if slot is occupied (op_id != 0) and matches
                if pending_op.uid == op_id.uid() {
                    // Op ID matches - store the result
                    pending_op.result = Some(result);
                    #[cfg(feature = "trace-requests")]
                    {
                        pending_op.duration_trace = harvest_start_time - pending_op.duration_trace;
                    }
                    match f(op_id, result, pending_op) {
                        OpCompletionAction::ReuseSlot => {
                            #[cfg(feature = "trace-requests")]
                            {
                                pending_op.duration_trace = harvest_start_time;
                            }
                        }
                        OpCompletionAction::TryWake => {
                            if let Some(waker) = pending_op.waker.take() {
                                waker.wake();
                            } else {
                                log::error!("Task {:?} is done ({}) without having a waker registered yet", op_id, result);
                            }
                        }
                        OpCompletionAction::Drop => {
                            let _ = std::mem::replace(pending_op, PendingOp::empty(self.stats.submission_epoch));
                            self.free_slots.push(op_id.slot());
                        }
                    }
                } else {
                    // Op was cancelled or slot was reused or empty - ignore the result
                    if result != -libc::ETIME {
                        // only print a warning for non-timeout ops;
                        // timeouts should regularly be dropped before completion
                        // in case they race with other operations (select!(timeout, desired_future));
                        // XXX I should really implement proper timeouts with sqe chaining
                        log::warn!(
                            "Ignoring completion for cancelled/stale/empty operation: slot={}, expected_op_id={}, got_op_id={}",
                            op_id.slot(),
                            pending_op.uid,
                            op_id.uid()
                        );
                    }
                }
            } else {
                // Slot index out of bounds - shouldn't happen but log it in debug
                #[cfg(debug_assertions)]
                log::error!("Completion for invalid slot index: slot={}, op_id={}", op_id.slot(), op_id.uid());
            }
        }
        let _x = self.am_in_completion.replace(false);
        debug_assert_eq!(_x, true);
        Ok(len)
    }

    #[inline]
    pub fn poll_completions(&mut self) -> io::Result<usize> {
        self.poll_completions_callback(|_opid, result, opref| {
            if result > 0 {
                if let Some(buf) = opref.buffer_ref() {
                    buf.mark_used(result as usize);
                }
            }
            OpCompletionAction::TryWake
        })
    }

    #[inline]
    fn register_new_pending_op(&mut self) -> Option<(OpId, &mut PendingOp)> {
        // Pop a free slot from the stack
        let slot = self.free_slots.pop()?;
        // Generate new unique op_id (guaranteed > 0)
        let op_id = self.next_op_id;
        self.next_op_id = (self.next_op_id % OpId::MAX_OP_UID) + 1;
        // write the op and return a reference
        self.slots[slot as usize] = PendingOp {
            uid: op_id,
            epoch: self.stats.submission_epoch,
            buffer: None,
            waker: None,
            result: None,
            #[cfg(feature = "trace-requests")]
            duration_trace: CycleMeasurement::now(),
        };
        Some((OpId::new(slot, op_id), &mut self.slots[slot as usize]))
    }

    #[inline]
    pub(super) fn register_op(&mut self) -> Option<OpId> {
        self.register_new_pending_op().map(|o| o.0)
    }

    /// Allocate a slot for a new operation and return OpId
    #[inline]
    pub(super) fn register_buffer_op(&mut self, buffer: IoBuf) -> Result<(OpId, &mut IoBuf), IoBuf> {
        match self.register_new_pending_op() {
            Some((id, opref)) => {
                opref.buffer = Some(buffer);
                Ok((id, unsafe { opref.buffer.as_mut().unwrap_unchecked() }))
            }
            None => Err(buffer),
        }
    }

    #[inline]
    pub(super) fn access_pending_op<T, F>(&mut self, op_id: OpId, f: F) -> Option<T>
    where
        F: FnOnce(&mut PendingOp) -> (T, OpCompletionAction),
    {
        let slot_idx = op_id.slot();
        if let Some(pending_op) = self.slots.get_mut(slot_idx as usize) {
            // Verify op_id matches (operation wasn't cancelled/replaced)
            if pending_op.uid != op_id.uid() {
                return None;
            }
            let (res, action) = f(pending_op);
            match action {
                OpCompletionAction::ReuseSlot => {
                    #[cfg(feature = "trace-requests")]
                    {
                        pending_op.duration_trace = CycleMeasurement::now();
                    }
                }
                OpCompletionAction::TryWake => {
                    log::error!("instructed to wake pending op in access action");
                }
                OpCompletionAction::Drop => {
                    let _old = std::mem::replace(pending_op, PendingOp::empty(self.stats.submission_epoch));
                    self.free_slots.push(slot_idx);
                }
            };
            Some(res)
        } else {
            None
        }
    }

    #[inline]
    pub(super) fn get_op_stage(&mut self, op_id: OpId) -> OpStage {
        match self.slots.get(op_id.slot as usize) {
            Some(pending_op) => pending_op.get_stage(op_id.uid, self.stats.submission_epoch),
            None => OpStage::Invalid,
        }
    }

    #[inline]
    pub(super) fn drop_registered_op(&mut self, op_id: OpId) {
        self.access_pending_op(op_id, |_ref| ((), OpCompletionAction::Drop));
    }

    #[inline]
    pub(super) fn register_waker(&mut self, op_id: OpId, waker: Waker) -> bool {
        let slot_idx = op_id.slot() as usize;
        if let Some(pending_op) = self.slots.get_mut(slot_idx) {
            // Verify op_id matches (operation wasn't cancelled/replaced)
            if pending_op.uid != op_id.uid() {
                return false;
            }
            pending_op.set_waker(waker);
            return true;
        }
        return false;
    }

    #[inline]
    pub(super) fn take_result(&mut self, op_id: OpId, waker: &Waker) -> Poll<((io::Result<i32>, Option<IoBuf>), u64)> {
        let slot_idx = op_id.slot() as usize;

        if let Some(pending_op) = self.slots.get_mut(slot_idx) {
            // Verify op_id matches (operation wasn't cancelled/replaced)
            if pending_op.uid != op_id.uid() {
                return Poll::Ready((
                    (Err(io::Error::new(io::ErrorKind::Other, "Operation was cancelled or replaced")), None),
                    0,
                ));
            }

            // Check if result is ready
            if let Some(result) = pending_op.result {
                // Free the slot by setting op_id to 0 and returning to free_slots stack
                let old = std::mem::replace(pending_op, PendingOp::empty(self.stats.submission_epoch));
                self.free_slots.push(op_id.slot());

                #[cfg(feature = "trace-requests")] // measured by completion queue loop
                let cycles = {
                    more_asserts::debug_assert_gt!(old.duration_trace, 0);
                    old.duration_trace
                };
                #[cfg(not(feature = "trace-requests"))]
                let cycles = 0;

                if result < 0 && result != -libc::ETIME {
                    Poll::Ready(((Err(io::Error::from_raw_os_error(-result)), old.buffer), cycles))
                } else {
                    Poll::Ready(((Ok(result), old.buffer), cycles))
                }
            } else {
                // Store waker for when result is ready
                pending_op.waker = Some(waker.clone());
                Poll::Pending
            }
        } else {
            Poll::Ready(((Err(io::Error::new(io::ErrorKind::Other, "Invalid slot index")), None), 0))
        }
    }

    #[inline]
    pub(super) unsafe fn take_result_with_unchecked_buffer(
        &mut self,
        op_id: OpId,
        waker: &Waker,
    ) -> Poll<((io::Result<usize>, IoBuf), u64)> {
        match self.take_result(op_id, waker) {
            Poll::Ready(((Ok(n), buf), cycles)) => Poll::Ready(((Ok(n as usize), unsafe { buf.unwrap_unchecked() }), cycles)),
            Poll::Ready(((Err(e), buf), cycles)) => Poll::Ready(((Err(e), unsafe { buf.unwrap_unchecked() }), cycles)),
            Poll::Pending => Poll::Pending,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct ThreadUring {
    inner: Rc<UnsafeCell<UringContext>>,
}
impl ThreadUring {
    pub fn new(cfg: UringConfig) -> io::Result<Self> {
        Ok(Self {
            inner: Rc::new(UnsafeCell::new(UringContext::new(cfg)?)),
        })
    }

    pub fn new_with<F: FnOnce(&mut UringConfig)>(f: F) -> io::Result<Self> {
        Self::new(UringConfig::default_with(f))
    }

    // F cannot be async -> no potential RefCell::borrow_mut() across await boundaries
    #[inline]
    pub fn access<T, F: FnOnce(&mut UringContext) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.inner.get() })
    }

    #[inline]
    pub fn current_outstanding(&self) -> usize {
        self.access(|uring| uring.stats.outstanding_ios) as usize
    }

    #[inline]
    pub fn last_known_cq_batch_size(&self) -> usize {
        self.access(|uring| uring.stats.last_known_cq_batch_size) as usize
    }


    pub fn enter(&self, intent: IOEnterIntent) {
        let _ = self.access(|uring| uring.enter(intent));
    }

    pub fn unpark(&self) {
        let _ = self.access(|uring| uring.request_unpark());
    }

    #[allow(dead_code)]
    pub(super) fn new_spurious_poller(&self) -> SpuriousPoller {
        SpuriousPoller {
            ring_ref: Rc::downgrade(&self.inner),
        }
    }

    #[inline]
    pub(crate) fn iodepth(&self) -> usize {
        self.access(|d| d.iodepth())
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub(super) struct SpuriousPoller {
    ring_ref: std::rc::Weak<UnsafeCell<UringContext>>,
}

impl Future for SpuriousPoller {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if let Some(ring) = self.ring_ref.upgrade() {
            let r = unsafe { &mut *ring.get() };
            #[cfg(debug_assertions)]
            log::debug!("spuriously polling completions {}", Rc::strong_count(&ring));
            let _ = r.enter(IOEnterIntent::Poll);
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
#[derive(PartialEq, Eq, Clone, Copy, Debug, derive_more::Display)]
pub(crate) enum RequestTracingType {
    Close,
    Fallocate,
    Fsync,
    IoCtl,
    Open,
    Other,
    Read,
    Recv,
    RecvMsg,
    Send,
    Sleep,
    StatX,
    Write,
}

impl RequestTracingType {
    #[inline]
    fn do_log(&self, cycles: u64) -> Option<f64> {
        match self {
            RequestTracingType::Recv => None,
            RequestTracingType::RecvMsg => None,
            RequestTracingType::Write => None,
            RequestTracingType::Sleep => None,
            _ => {
                let micros = reqtrace::MicroScale::scale_cycles(cycles);
                if micros > 100.0 { Some(micros) } else { None }
            }
        }
    }
}

#[inline]
fn log_if_slow_completion<T>(_trace_type: RequestTracingType, (res, _cycles): (T, u64)) -> T {
    #[cfg(feature = "trace-requests")]
    if let Some(micros) = _trace_type.do_log(_cycles) {
        log::debug!("SlowIO :type {:?} :micros {}", _trace_type, micros);
    }
    res
}

////////////////////////////////////////////////////////////////////////////////
#[allow(async_fn_in_trait)]
pub trait CompletionBasedFuture: Sized {
    fn op_id(&self) -> OpId;
    fn context(&self) -> &ThreadUring;
    fn deregister_op(&self) {
        self.context().access(|ctx| ctx.drop_registered_op(self.op_id()));
    }

    fn fire_and_forget(self) -> FireAndForget<Self> {
        FireAndForget::new(self)
    }
}

// - return ready() once underlying future is submitted (at which point uring guarantees that passed ptr etc aren't required anymore)
// - but keep underlying PendingOp around until completion (for the buffer)
pin_project_lite::pin_project! {
    pub struct FireAndForget<Fut: CompletionBasedFuture> {
        inner: Fut,
        #[pin] wake_list: WakerGuard<'static>
    }
}

impl<Fut: CompletionBasedFuture> FireAndForget<Fut> {
    fn new(inner: Fut) -> Self {
        // SAFETY: inner holds Rc to uring, so the context wakelist ptr must also be valid
        // during that time
        let raw_list: &'static WakerList = unsafe {
            let ptr = inner.context().access(|uring| &uring.submission_wakers as *const WakerList);
            &*ptr
        };
        let wake_list = WakerGuard::new(raw_list);
        Self { inner, wake_list }
    }
}

impl<Fut: CompletionBasedFuture> Future for FireAndForget<Fut> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let uring = this.inner.context();
        let op_id = this.inner.op_id();
        uring.access(|ctx| {
            match ctx.get_op_stage(op_id) {
                OpStage::Unsubmitted => {
                    // wait until submission to ensure  resources are kept around
                    this.wake_list.register(cx.waker());
                    unsafe { ctx.slots.get_unchecked_mut(op_id.slot as usize) }.set_waker(cx.waker().clone());
                    Poll::Pending
                }
                OpStage::Submitted => {
                    // keep pending op around until completion, then drop it
                    let waker = DroppingWaker::new_rc_waker(&uring.inner, op_id.slot);
                    unsafe { ctx.slots.get_unchecked_mut(op_id.slot as usize) }.set_waker(waker);
                    Poll::Ready(())
                }
                OpStage::Completed => Poll::Ready(()),
                OpStage::Invalid => Poll::Ready(()),
            }
        })
    }
}

struct DroppingWaker;
impl crate::runtime::rcwaker::RcWaker for DroppingWaker {
    type Handler = UnsafeCell<UringContext>;

    fn on_wake(handler: Rc<Self::Handler>, tag: u16) {
        // SAFETY: this can only be called from the pending op waker,
        // meaning the op is in the slot indicated by `tag`
        unsafe {
            let handler = &mut *handler.get();
            let op = handler.slots.get_unchecked_mut(tag as usize).uid;
            handler.drop_registered_op(OpId::new(tag, op))
        };
    }

    fn on_wake_by_ref(handler: &Rc<Self::Handler>, tag: u16) {
        // SAFETY: this can only be called from the pending op waker,
        // meaning the op is in the slot indicated by `tag`
        unsafe {
            let handler = &mut *handler.get();
            let op = handler.slots.get_unchecked_mut(tag as usize).uid;
            handler.drop_registered_op(OpId::new(tag, op))
        };
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct PendingBufferOp {
    trace_type: RequestTracingType,
    context: ThreadUring,
    op_id: OpId,
}

impl CompletionBasedFuture for PendingBufferOp {
    fn op_id(&self) -> OpId {
        self.op_id
    }

    fn context(&self) -> &ThreadUring {
        &self.context
    }
}

impl Drop for PendingBufferOp {
    fn drop(&mut self) {
        self.deregister_op();
    }
}

impl Future for PendingBufferOp {
    type Output = (io::Result<usize>, IoBuf);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.context.access(|context| {
            // We assume that the main I/O driver (runtime) does this
            // let _ = context.poll_completions();
            // Check if our operation completed, asserting that there must be a buffer
            unsafe { context.take_result_with_unchecked_buffer(self.op_id, cx.waker()) }
                .map(|res| log_if_slow_completion(self.trace_type, res))
        })
    }
}

////////////////////////////////////////////////////////////////////////////////

pub struct PendingSimpleOp {
    trace_type: RequestTracingType,
    context: ThreadUring,
    op_id: OpId,
}

impl CompletionBasedFuture for PendingSimpleOp {
    fn op_id(&self) -> OpId {
        self.op_id
    }

    fn context(&self) -> &ThreadUring {
        &self.context
    }
}

impl Drop for PendingSimpleOp {
    fn drop(&mut self) {
        self.deregister_op();
    }
}

impl Future for PendingSimpleOp {
    type Output = io::Result<i32>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.context.access(|context| {
            // We assume that the main I/O driver (runtime) does this
            // let _ = context.poll_completions();
            // Check if our operation completed, asserting that there is no buffer

            context
                .take_result(self.op_id, cx.waker())
                .map(|res| log_if_slow_completion(self.trace_type, res))
                .map(|(result, _buf)| {
                    debug_assert!(matches!(_buf, None));
                    result
                })
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//  utility functions

impl ThreadUring {
    #[inline]
    pub(crate) fn generic_buffer_op<Init: FnOnce(OpId, &mut IoBuf) -> squeue::Entry>(
        &self,
        trace_type: RequestTracingType,
        buf: IoBuf,
        init: Init,
    ) -> PendingBufferOp {
        self.access(|context| {
            // Allocate a slot and get op_id
            let (op_id, buf) = context.register_buffer_op(buf).expect("No free slots available");
            // Prepare operation
            let op = init(op_id, buf).user_data(op_id.into_user_data());
            // enqueue to uring
            context.enqueue_retry_once(op).expect("couldn't enqueue uring op after 2 tries");
            // let _ = context.submit_non_blocking(false);
            PendingBufferOp {
                trace_type,
                context: self.clone(),
                op_id,
            }
        })
    }

    #[inline]
    pub(crate) fn generic_simple_op<Init: FnOnce(OpId) -> squeue::Entry>(
        &self,
        trace_type: RequestTracingType,
        init: Init,
    ) -> PendingSimpleOp {
        self.access(|context| {
            // Allocate a slot and get op_id
            let op_id = context.register_op().expect("No slots available");
            // Prepare operation
            let op = init(op_id).user_data(op_id.into_user_data());
            // enqueue to uring
            context.enqueue_retry_once(op).expect("couldn't enqueue uring op after 2 tries");
            // let _ = context.submit_non_blocking(false);
            PendingSimpleOp {
                trace_type,
                context: self.clone(),
                op_id,
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////
//  often-used operations

#[derive(Debug)]
pub enum FileSizeType {
    Regular,
    Block,
}

impl ThreadUring {
    pub fn fdatasync(&self, fd: impl AsRawFd) -> PendingSimpleOp {
        self.generic_simple_op(RequestTracingType::Fsync, |_| {
            opcode::Fsync::new(types::Fd(fd.as_raw_fd())).flags(FsyncFlags::DATASYNC).build()
        })
    }

    pub async fn open<A: AsRef<Path>>(&self, path: A, flags: i32, mode: Option<u32>) -> std::io::Result<RawFd> {
        let path_str_c = CString::new(path.as_ref().as_os_str().as_bytes()).unwrap();
        let fd = self
            .generic_simple_op(RequestTracingType::Open, |_| {
                opcode::OpenAt::new(types::Fd(-1), path_str_c.as_ptr()).flags(flags).mode(mode.unwrap_or(0)).build()
            })
            .await?;
        Ok(fd)
    }

    pub fn fallocate(&self, fd: impl AsRawFd, mode: i32, offset: u64, len: u64) -> PendingSimpleOp {
        self.generic_simple_op(RequestTracingType::Fallocate, |_| {
            opcode::Fallocate::new(types::Fd(fd.as_raw_fd()), len).mode(mode).offset(offset).build()
        })
    }

    pub fn close(&self, fd: impl AsRawFd) -> PendingSimpleOp {
        self.generic_simple_op(RequestTracingType::Close, |_| opcode::Close::new(types::Fd(fd.as_raw_fd())).build())
    }

    pub fn write(&self, fd: impl AsRawFd, offset: u64, buf: IoBuf) -> PendingBufferOp {
        let fd = fd.as_raw_fd();
        self.generic_buffer_op(RequestTracingType::Write, buf, |_op_id, buf| {
            opcode::Write::new(types::Fd(fd), buf.ptr(), buf.used_bytes() as u32).offset(offset).build()
        })
    }

    pub fn read(&self, fd: impl AsRawFd, offset: u64, buf: IoBuf) -> PendingBufferOp {
        let fd = fd.as_raw_fd();
        self.generic_buffer_op(RequestTracingType::Read, buf, |_op_id, buf| {
            opcode::Read::new(types::Fd(fd), buf.ptr_mut(), buf.used_bytes() as u32).offset(offset).build()
        })
    }

    pub async fn file_statx(&self, fd: impl AsRawFd) -> Result<libc::statx, std::io::Error> {
        static EMPTY: &str = "\0";
        let fd = fd.as_raw_fd();
        let mut statx = unsafe { std::mem::zeroed::<libc::statx>() };
        let result = self
            .generic_simple_op(RequestTracingType::StatX, |_op_id| {
                opcode::Statx::new(
                    types::Fd(fd),
                    EMPTY.as_ptr() as *const libc::c_char,
                    &mut statx as *mut libc::statx as *mut types::statx,
                )
                .flags(libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH)
                .mask(libc::STATX_BASIC_STATS)
                .build()
            })
            .await;
        result.map(|_code| statx)
    }

    // XXX doesn't work, (22 invalid arg), and basically no documentation.
    // disabled for now.
    /// Get block device size in bytes using BLKGETSIZE64 ioctl via io_uring
    #[cfg(test)]
    pub async fn blkgetsize64(&self, fd: impl AsRawFd) -> Result<u64, std::io::Error> {
        // BLKGETSIZE64 = _IOR(0x12, 114, size_t) on Linux
        const BLKGETSIZE64: u32 = 0x80081272;
        let fd = fd.as_raw_fd();
        let mut output: UnsafeCell<u64> = UnsafeCell::new(0);
        let output_addr = (output.get()) as usize;
        let mut ioctl_data = [0u8; 16];
        ioctl_data[8..16].copy_from_slice(&output_addr.to_ne_bytes());
        println!("ioctl_data is {:?}", ioctl_data);
        let result = self
            .generic_simple_op(RequestTracingType::IoCtl, |_op_id| {
                opcode::UringCmd16::new(types::Fd(fd), BLKGETSIZE64).cmd(ioctl_data).build()
            })
            .await;

        match result {
            Ok(res) => {
                println!("blk ret code {} ", res);
                Ok(*output.get_mut())
            }
            Err(e) => {
                log::error!("error during BLKGETSIZE64 ioctl: {} (errno: {:?})", e, e.raw_os_error());
                Err(e)
            }
        }
    }

    pub async fn get_file_size(&self, fd: impl AsRawFd) -> std::io::Result<(u64, FileSizeType)> {
        let fd = fd.as_raw_fd();
        // try async statx first
        let st = self.file_statx(fd).await?;
        let ftype = (st.stx_mode as u32) & libc::S_IFMT;
        match ftype {
            libc::S_IFREG => Ok((st.stx_size, FileSizeType::Regular)),
            libc::S_IFBLK => {
                let mut bytes: u64 = 0;
                const BLKGETSIZE64: libc::c_ulong = 0x80081272;
                if unsafe { libc::ioctl(fd, BLKGETSIZE64, &mut bytes) } != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok((bytes, FileSizeType::Block))
            }
            _ => Err(io::Error::other("unknown file type")),
        }
    }

    pub fn recv(&self, sock: impl AsRawFd, buf: IoBuf) -> PendingBufferOp {
        let fd = sock.as_raw_fd();
        self.generic_buffer_op(RequestTracingType::Recv, buf, |_op_id, buf| {
            opcode::Recv::new(types::Fd(fd), buf.ptr_mut(), buf.used_bytes() as u32).build()
        })
    }

    pub async fn recv_from(
        &self,
        sock: impl AsRawFd,
        buf: IoBuf,
        flags: Option<u32>,
    ) -> (io::Result<(usize, SocketAddr)>, IoBuf) {
        let mut out_addr = unsafe { std::mem::zeroed::<libc::sockaddr_in>() };
        let mut iovec = libc::iovec {
            iov_base: buf.ptr_mut() as *mut libc::c_void,
            iov_len: buf.capacity(),
        };
        let mut msg = libc::msghdr {
            msg_name: &mut out_addr as *mut libc::sockaddr_in as *mut libc::c_void,
            msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as u32,
            msg_iov: &mut iovec as *mut libc::iovec,
            msg_iovlen: 1,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        };
        let fd = sock.as_raw_fd();
        let (res, mut buf) = self
            .generic_buffer_op(RequestTracingType::RecvMsg, buf, |_op_id, _buf| {
                opcode::RecvMsg::new(types::Fd(fd), (&mut msg) as *mut libc::msghdr)
                    .flags(flags.unwrap_or(libc::MSG_CMSG_CLOEXEC as u32))
                    .build()
            })
            .await;
        match res {
            Ok(len) => {
                buf.mark_used(len);
                let ip = IpAddr::V4(Ipv4Addr::from(u32::from_be(out_addr.sin_addr.s_addr)));
                let from = ((ip, u16::from_be(out_addr.sin_port))).into();
                (Ok((len, from)), buf)
            }
            Err(e) => (Err(e), buf),
        }
    }

    pub fn send(&self, sock: impl AsRawFd, to: (*const libc::sockaddr, usize), buf: IoBuf) -> PendingBufferOp {
        let fd = sock.as_raw_fd();
        self.generic_buffer_op(RequestTracingType::Send, buf, |_op_id, buf| {
            opcode::Send::new(types::Fd(fd), buf.ptr(), buf.used_bytes() as u32)
                .dest_addr(to.0 as *const libc::sockaddr)
                .dest_addr_len(to.1 as u32)
                .build()
        })
    }

    pub async fn send_to(&self, sock: impl AsRawFd, addr: SocketAddr, buf: IoBuf) -> (io::Result<usize>, IoBuf) {
        let port = addr.port();
        match addr {
            SocketAddr::V4(v4) => {
                let to = libc::sockaddr_in {
                    sin_family: libc::AF_INET as u16,
                    sin_port: port.to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: v4.ip().to_bits().to_be(),
                    },
                    sin_zero: [0u8; 8],
                };
                let to_raw = (&to as *const libc::sockaddr_in as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in>());
                self.send(sock, to_raw, buf).await
            }
            SocketAddr::V6(v6) => {
                const FLOWINFO: u32 = 4269;
                let to = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET as u16,
                    sin6_port: port.to_be(),
                    sin6_flowinfo: FLOWINFO,
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: 0,
                };
                let to_raw =
                    (&to as *const libc::sockaddr_in6 as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in6>());
                self.send(sock, to_raw, buf).await
            }
        }
    }

    pub async fn ff_send_to(&self, sock: impl AsRawFd, addr: SocketAddr, buf: IoBuf) {
        let port = addr.port();
        match addr {
            SocketAddr::V4(v4) => {
                let to = libc::sockaddr_in {
                    sin_family: libc::AF_INET as u16,
                    sin_port: port.to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: v4.ip().to_bits().to_be(),
                    },
                    sin_zero: [0u8; 8],
                };
                let to_raw = (&to as *const libc::sockaddr_in as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in>());
                self.send(sock, to_raw, buf).fire_and_forget().await;
            }
            SocketAddr::V6(v6) => {
                const FLOWINFO: u32 = 4269;
                let to = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET as u16,
                    sin6_port: port.to_be(),
                    sin6_flowinfo: FLOWINFO,
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: 0,
                };
                let to_raw =
                    (&to as *const libc::sockaddr_in6 as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_in6>());
                self.send(sock, to_raw, buf).fire_and_forget().await;
            }
        }
    }

    pub fn udp_bind(&self, addr: impl Into<SocketAddr>) -> io::Result<std::net::UdpSocket> {
        let socket = Socket::new(Domain::IPV4, Type::DGRAM, None)?;
        socket.set_reuse_address(true)?;
        socket.set_reuse_port(true)?;
        socket.bind(&SockAddr::from(addr.into()))?;
        Ok(socket.into())
    }

    pub async fn sleep(&self, time: Duration) -> io::Result<()> {
        let timespec = libc::timespec {
            tv_sec: time.as_secs() as i64,
            tv_nsec: time.subsec_nanos() as i64,
        };
        let result = self
            .generic_simple_op(RequestTracingType::Sleep, |_op_id| {
                opcode::Timeout::new(&timespec as *const libc::timespec as *const types::Timespec)
                    // .flags(TimeoutFlags::BOOTTIME)
                    .build()
            })
            .await?;
        debug_assert!(result == 0 || result == -libc::ETIME); // etime_success
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::*;
    use std::time::Instant;

    /// Non-blocking socket using io_uring
    struct UringSocket {
        socket: Socket,
        _context: ThreadUring,
    }

    impl UringSocket {
        fn new(_context: ThreadUring) -> io::Result<Self> {
            let socket = Socket::new(Domain::IPV4, Type::STREAM, None)?;
            socket.set_nonblocking(true)?;

            Ok(Self { socket, _context })
        }

        fn bind(&self, addr: &SocketAddr) -> io::Result<()> {
            self.socket.set_reuse_address(true)?;
            self.socket.set_reuse_port(true)?;
            self.socket.bind(&(*addr).into())
        }

        fn listen(&self, backlog: i32) -> io::Result<()> {
            self.socket.listen(backlog)
        }
    }

    #[test]
    fn test_sleep() {
        let uring = ThreadUring::new_with(|cfg| {
            cfg.sq_entries = 64;
            cfg.cq_entries = 64;
            cfg.poll_mode = UringPollMode::AutoPoll {
                idle_time: Duration::from_millis(10),
                cpu_affinity: None,
            };
        })
        .expect("Failed to create io_uring");
        reqtrace::calibrate_cheap_clock();
        let mut c_start = reqtrace::MicrosecondMeasurement::new();
        let t = Duration::from_millis(10);
        let mut ctx = std::task::Context::from_waker(std::task::Waker::noop());
        let t_start = Instant::now();
        c_start.start_measurement();
        let mut timeout = std::pin::pin!(uring.sleep(t));
        while let Poll::Pending = timeout.as_mut().poll(&mut ctx) {
            uring.access(|c| c.enter(IOEnterIntent::Submit)).expect("error during uring submit");
        }
        let micros = c_start.stop_measurement();
        let elapsed = t_start.elapsed();
        println!("elapsed {:?}", elapsed); // + 40us
        println!("elapsed micros {:?}", micros); // - 50us
    }

    #[apply(async_test)]
    fn test_file_statx() {
        println!("Starting file_statx test");

        // Create io_uring context
        let context = ThreadUring::new_with(|cfg| {
            cfg.sq_entries = 64;
            cfg.cq_entries = 64;
            cfg.poll_mode = UringPollMode::AutoPoll {
                idle_time: Duration::from_millis(10),
                cpu_affinity: None,
            };
        })
        .expect("Failed to create io_uring");

        test_exec().spawn(context.new_spurious_poller());

        // Create a temporary file for testing
        let temp_dir = std::env::temp_dir();
        let test_file = temp_dir.join("uring_statx_test.txt");
        let test_content = b"Hello, statx test!";

        std::fs::write(&test_file, test_content).expect("Failed to create test file");

        // Open the file
        let file = std::fs::File::open(&test_file).expect("Failed to open test file");
        let fd = file.as_raw_fd();
        println!("Opened file with fd: {}", fd);

        // First, try regular statx call to verify the fd is valid
        let mut regular_statx: libc::statx = unsafe { std::mem::zeroed() };
        let statx_result = unsafe {
            libc::statx(
                fd,
                std::ptr::null(),
                libc::AT_STATX_SYNC_AS_STAT | libc::AT_EMPTY_PATH,
                libc::STATX_BASIC_STATS,
                &mut regular_statx as *mut _,
            )
        };
        println!("Regular statx call result: {}", statx_result);
        if statx_result == 0 {
            println!("Regular statx succeeded, file size: {}", regular_statx.stx_size);
        }

        let res = context.file_statx(fd).await;
        let statx = match &res {
            Ok(statx) => statx,
            Err(err) => panic!("error {:?}", err),
        };

        debug_assert_eq!(regular_statx.stx_size, statx.stx_size);
        debug_assert_eq!(regular_statx.stx_blksize, statx.stx_blksize);
        debug_assert_eq!(regular_statx.stx_blocks, statx.stx_blocks);

        // Cleanup
        drop(file);
        std::fs::remove_file(&test_file).ok();
    }

    #[apply(async_test)]
    #[ignore]
    fn test_get_file_size() {
        println!("Starting get_file_size test");

        // Create io_uring context
        let context = ThreadUring::new_with(|cfg| {
            cfg.sq_entries = 64;
            cfg.cq_entries = 64;
            cfg.poll_mode = UringPollMode::AutoPoll {
                idle_time: Duration::from_millis(10),
                cpu_affinity: None,
            };
        })
        .expect("Failed to create io_uring");
        test_exec().spawn(context.new_spurious_poller());
        let test_file = "/dev/sda";
        let file = std::fs::OpenOptions::new().read(true).open(&test_file).expect("Failed to open test file");
        let fd = file.as_raw_fd();

        let blksize = context.get_file_size(fd).await;
        println!("got {:?}", blksize);
        assert!(matches!(blksize, Ok((_, FileSizeType::Block))));
    }
}

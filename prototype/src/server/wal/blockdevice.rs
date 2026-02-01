use super::{SharedWAL, WAL, buffer::WalBuffer, location::DiskLocation, stats::WalStats};
use crate::{
    config::ServerConfig,
    io::{ThreadUring, buffer::IoBuffer},
    runtime::waker_chain::{WakerList, sleep_or_wait, yield_or_wait},
    types::message::JournalMetadata,
};
use smallvec::SmallVec;
use std::{
    cell::{Cell, RefCell},
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    rc::Rc,
    sync::{Arc, atomic::AtomicUsize},
    time::Duration,
};

#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
enum FlushState {
    #[default]
    Free,
    InUse,
    Flushing,
    Flushed,
}

struct ThreadBlockDevice {
    buffer: RefCell<WalBuffer>, // XXX unsafecell?
    io: ThreadUring,
    device: OwnedFd,
    device_range: (u64, u64),
    flush_interval: Duration,
    flush_min_bytes: u64,
    // tracking of concurrent flushes; we want this to be limited to
    // (1) limit max I/O concurrency ourselves (instead of overloading SSD) and
    // (2) avoid heap allocs for tracking (and waiting on) each flush
    // XXX make io_depth runtime-configurable to enable as parameter?
    flush_slots: [Cell<FlushState>; Self::IO_DEPTH],
    flush_waits: [WakerList; Self::IO_DEPTH],
    flush_results: [Cell<Result<DiskLocation, ()>>; Self::IO_DEPTH],
    slot_waiter: WakerList,
    stats: Cell<WalStats>,
}

struct FlushTask {
    buffer: IoBuffer,
    location: DiskLocation,
    flush_slot: usize,
}

impl ThreadBlockDevice {
    const MAX_BLOCK_BYTES: u64 = 1024 * 64;
    // 16 per core should be plenty on current aws instances, but should make configurable
    // https://cloudspecs.fyi/?state=N4IgzgjgNgigrgUwE4E8QC4QGUCiAZHAYQBUACASwDswAXAQ0oGMEAaUxgeyQTDdq7oBzBAH0A7knI1R5DgAcwFeb1IAdSqU1bSSDnEoATABT8kQ0RKkzlAejmTmIgBZ6kASiUKRc5CIMcoKDokFnVtLV19Y1NzcUlpEVkFG05uMA8ksG9fVNYdPUMjAEYEADYbGOE4q0TlNgAmDzgsnyRa0I1w-KjigAZem2bsttkGjI4DBDkaJzYAWjnSIv7m0gQADx9GaQNSS2lSIOkmFHVVGgiC41lJ6acUrh4xpVuZ4ZFc9QAxACUAeQAsqQ6GIsgAzOgAW3IUFOlDETmQCFIlRkWUoADdIQh1FxJkhSAAjFAvKZvVofR6kSZgRggFggJCECYIDAgGgcERgDGCUgAHkW3MEtEklEERjE5AMM1IAF5SHoaHI4DQAHSS6VOVVURizUiI8iCJwXeWK5Vqg1GtU6vW0uhQKi8+VFNxnSgAYk9pBmyMYcCQ3EoF3ohKgyPIikJV29HG9iNIGOC5DooeRAHIDGC07iVeb+YtBII5FAODQjJm2HQeEY3B4ANRhYGUSilujSIzCDiQuWkNPSdY0NNsdY93psEnyseHFMIKA9tMABRLFzAXYQM0dAEI067KOpi6WjGaVW4ANzUhAY1UcMFgmvnjlcnk10juhW5lUxuPI42Qudycx6RAIIUEVDBQBoFAfDZEMwxAABfeCgA
    const IO_DEPTH: usize = 32;

    async fn new(
        io: ThreadUring,
        myid: u64,
        partitions: u64,
        flush_interval: Duration,
        flush_min_bytes: u64,
        files: &[String],
        prev: Option<DiskLocation>,
    ) -> Self {
        let blocksize = DiskLocation::GRANULARITY;
        let mydev = &files[myid as usize % files.len()];

        // XXX assumes all devices have the same size
        let per_dev_partitions = ((partitions as f64) / files.len() as f64).ceil() as u64;
        let device = io
            .open(mydev, libc::O_RDWR | libc::O_DIRECT | libc::O_DSYNC, None)
            .await
            .expect(format!("error opening file {}", mydev).as_str());
        let (device_bytes, _device_type) = io.get_file_size(device).await.expect("error getting file size");
        if device_bytes == 0 {
            panic!("Device size is 0; cannot write WAL to it");
        }
        let chunksize = device_bytes / per_dev_partitions;
        let ondev_id = myid % per_dev_partitions;
        let device_range = (
            (ondev_id * chunksize).next_multiple_of(blocksize),
            ((ondev_id + 1) * chunksize).next_multiple_of(blocksize).saturating_sub(blocksize),
        );
        let loc = DiskLocation::new(myid as u16, Self::MAX_BLOCK_BYTES as u32, device_range.0);
        let buffer = WalBuffer::new(loc, prev.unwrap_or(DiskLocation::invalid()));
        let flush_slots: [Cell<FlushState>; Self::IO_DEPTH] = Default::default();
        flush_slots[0].set(FlushState::InUse);
        ThreadBlockDevice {
            buffer: RefCell::new(buffer),
            io,
            device: unsafe { OwnedFd::from_raw_fd(device) },
            device_range,
            flush_interval,
            flush_min_bytes,
            flush_slots,
            flush_waits: Default::default(),
            flush_results: [const { Cell::new(Err(())) }; Self::IO_DEPTH],
            slot_waiter: WakerList::new(),
            stats: Cell::new(WalStats::default()),
        }
    }

    #[inline]
    fn trace<T, F: FnOnce(&mut WalStats) -> T>(&self, f: F) -> T {
        // SAFETY: we're only on one thread, and the reference can't
        // escape the non-async function passed
        f(unsafe { &mut *self.stats.as_ptr() })
    }

    #[inline]
    fn find_free_flush_slot(&self) -> Option<usize> {
        self.flush_slots.iter().position(|x| x.get() == FlushState::Free)
    }

    #[inline]
    async fn wait_for_available_slots(&self) -> (usize, usize) {
        return loop {
            match self.find_free_flush_slot() {
                Some(f) => break (f, self.find_current_flush_slot()),
                None => {
                    self.slot_waiter.wait_for_ping().await;
                    continue;
                }
            }
        };
    }

    #[inline]
    fn find_current_flush_slot(&self) -> usize {
        self.flush_slots
            .iter()
            .position(|x| x.get() == FlushState::InUse)
            .expect("There must alwasy be an in-use flush slot")
    }

    // can't be async, needs to be 'atomic' from the view of async tasks
    fn replace_buffer(&self, cur_slot: usize, free_slot: usize) -> FlushTask {
        // compact old buffer
        let mut next_loc = Default::default();
        let old = self.buffer.replace_with(|old| {
            let old_loc = old.compact();
            next_loc = old_loc.next_block_wrapping(self.device_range).with_size(Self::MAX_BLOCK_BYTES as u32);
            let newbuf = WalBuffer::new(next_loc, old_loc);
            newbuf
        });
        let (buffer, location) = old.finalize_compacted(next_loc);
        self.flush_slots[cur_slot].set(FlushState::Flushing);
        self.flush_slots[free_slot].set(FlushState::InUse);
        FlushTask {
            buffer,
            location,
            flush_slot: cur_slot,
        }
    }

    fn on_woken_from_flush(&self, cur_slot: usize, wake_chain: &WakerList) -> Result<DiskLocation, ()> {
        if wake_chain.len() == 0 {
            // we're the last to wake - free flush slot
            self.flush_slots[cur_slot].set(FlushState::Free);
            // schedule procs potentially waiting on empty slots
            self.slot_waiter.wake_all_pessimistic();
        }
        self.flush_results[cur_slot].get()
    }

    async fn wait_for_flush(self: &Rc<Self>, free_slot: usize, cur_slot: usize) -> Result<DiskLocation, ()> {
        return loop {
            debug_assert!(self.flush_slots[cur_slot].get() == FlushState::InUse);
            let (used_bytes, age) = self.buffer.borrow().fetch_stats();
            // - if the buffer is old or full enough, then immediately flush it ourselves
            if age >= self.flush_interval || (used_bytes as u64) >= self.flush_min_bytes {
                self.trace(|s| s.record_sync_buffer_flush(age, used_bytes));
                // println!("flushing after {:?} (vs {:?}) with {} bytes", age, ThreadBlockDevice::FLUSH_INTERVAL, used_bytes);
                let to_flush = self.replace_buffer(cur_slot, free_slot);
                break self.flush_await(to_flush).await;
            }
            // else, we want to batch writes; wait for as briefly as possible by yielding
            // (timeouts in the runtime have >= 1ms granularity, so not useful)
            let wake_chain = &self.flush_waits[cur_slot];
            // let _was_woken = yield_or_wait(wake_chain).await;
            let _was_poller = sleep_or_wait(wake_chain, &self.io, self.flush_interval).await;
            if _was_poller {
                self.trace(WalStats::on_yield);
            }
            // now, someone else might have flushed the buffer or is flushing the buffer
            match self.flush_slots[cur_slot].get() {
                FlushState::Free => panic!("flush state changed to free?!"),
                FlushState::InUse => continue, // re-check conditions
                FlushState::Flushing => panic!("we were woken but aren't already flushed?"),
                FlushState::Flushed => break self.on_woken_from_flush(cur_slot, wake_chain),
            };
        };
    }

    #[must_use]
    async fn push(self: &Rc<Self>, meta: &JournalMetadata, data: &[u8]) -> Result<DiskLocation, ()> {
        self.trace(WalStats::on_push);
        // ensure that there is a free flush lane to flush to;
        // could be more optimistic here (we don't always need to flush),
        // but that massively complicates the code below and, with I/O overload
        // (when we likely actually need to wait), doesn't really make a difference,
        // since is the same as setting IO_DEPTH += 1 in the steady state
        let (free_slot, cur_slot) = self.wait_for_available_slots().await;
        // CASE A: there is space in buffer, push succeeded; need to flush or wait for someone else to flush
        if self.buffer.borrow_mut().try_push(meta, data) {
            return self.wait_for_flush(free_slot, cur_slot).await;
        }
        // CASE B: no space in buffer, need to
        // (1) alloc new buffer,
        // (2) flush old buffer and
        // (3) wait for new buffer to be flushed
        let to_flush = self.replace_buffer(cur_slot, free_slot);
        // push to new buffer (should always succeed if our data size assumptions hold)
        if !self.buffer.borrow_mut().try_push(meta, data) {
            panic!(
                "Can't push {} bytes to empty buffer of size {}!?",
                data.len(),
                self.buffer.borrow().capacity_bytes()
            );
        }
        // flush the old buffer (we don't care about it b/c we are pushing to the new buffer)
        self.flush_in_background(to_flush);
        // recompute slots, wait for flush of current page
        let (free_slot, cur_slot) = self.wait_for_available_slots().await;
        self.wait_for_flush(free_slot, cur_slot).await
    }

    fn flush_in_background(self: &Rc<Self>, task: FlushTask) {
        let this = self.clone();
        crate::runtime::rt().spawn(async move {
            this.trace(WalStats::on_background_flush);
            let _ = this.flush_await(task).await;
        });
    }

    async fn flush_await(&self, f: FlushTask) -> Result<DiskLocation, ()> {
        // check preconditions
        debug_assert_eq!(f.location.size() as usize, f.buffer.used_bytes());
        debug_assert!(f.flush_slot < self.flush_slots.len());
        debug_assert_eq!(self.flush_slots[f.flush_slot].get(), FlushState::Flushing);
        let state = &self.flush_slots[f.flush_slot];
        // slot is already marked as Flushing in replace_buffer
        let t_start = std::time::Instant::now();
        let (res, _buf) = self.io.write(self.device.as_raw_fd(), f.location.offset(), f.buffer).await;
        // SAFETY: this is only ever used from a single thread and isn't async
        self.trace(|s| s.record_io(t_start, f.location.size() as usize));
        let res = match res {
            Ok(_) => Ok(f.location),
            Err(_e) => {
                log::error!("error while flushing: {:?}", _e);
                Err(())
            }
        };
        self.flush_results[f.flush_slot].set(res);
        let woken = self.flush_waits[f.flush_slot].wake_all_pessimistic();
        self.trace(|s| s.record_chain_length(woken));
        // if no one else was waiting, set to `free` immediately; otherwise, set to flushed
        // and let the last woken waiter set the state to `free` again
        let _old = state.replace(if woken == 0 { FlushState::Free } else { FlushState::Flushed });
        debug_assert_eq!(_old, FlushState::Flushing);
        res
    }
}

impl WAL for Rc<ThreadBlockDevice> {
    async fn write(&self, meta: &JournalMetadata, data: &[u8]) -> DiskLocation {
        self.push(meta, data).await.expect("error while flushing")
    }

    async fn flush(&self) -> DiskLocation {
        let (free, cur) = self.wait_for_available_slots().await;
        let task = self.replace_buffer(cur, free);
        let res = self.flush_await(task).await.expect("error while flushing");
        self.trace(WalStats::on_explicit_flush);
        res
    }

    fn take_wal_stats(&self) -> WalStats {
        self.stats.take()
    }
}

////////////////////////////////////////////////////////////////////////////////
//

pub(super) struct LocalBlockdeviceWAL {
    files: SmallVec<[String; 8]>,
    assigned: AtomicUsize,
    partitions: u64,
    flush_interval: Duration,
    flush_min_bytes: u64,
}

impl LocalBlockdeviceWAL {
    pub(super) fn new(cfg: &ServerConfig, files: impl IntoIterator<Item = String>) -> Self {
        let partitions = cfg.journal_threads() as u64;
        let files = SmallVec::from_iter(files);
        Self {
            files,
            assigned: AtomicUsize::new(0),
            partitions,
            flush_interval: Duration::from_micros(cfg.wal_flush_interval_us),
            flush_min_bytes: cfg.wal_flush_min_bytes,
        }
    }
}

impl SharedWAL for Arc<LocalBlockdeviceWAL> {
    async fn local(self) -> impl WAL {
        let id = self.assigned.fetch_add(1, std::sync::atomic::Ordering::Acquire);
        debug_assert!(id < self.partitions as usize);
        Rc::new(
            ThreadBlockDevice::new(
                crate::runtime::rt().io().clone(),
                id as u64,
                self.partitions as u64,
                self.flush_interval,
                self.flush_min_bytes,
                self.files.as_slice(),
                None,
            )
            .await,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::*;
    use std::fs;

    fn create_test_metadata(journal_id: u64, lsn: u64, offset: u64) -> JournalMetadata {
        use uuid::Uuid;
        let journal_uuid = Uuid::from_u128(journal_id as u128);
        JournalMetadata {
            id: journal_uuid.into(),
            max_known_lsn: lsn,
            max_known_offset: offset,
            current_epoch: 1,
        }
    }

    fn create_temp_device_files(name: &str, count: usize, size_mb: u64) -> (std::path::PathBuf, Vec<String>) {
        let temp_dir = std::env::temp_dir().join(format!("blockdevice_test_{}", name));
        std::fs::create_dir_all(&temp_dir).expect("Failed to create temp directory");
        let mut files = Vec::new();

        for i in 0..count {
            let file_path = temp_dir.join(format!("test_device_{}.dat", i));
            let file_path_str = file_path.to_string_lossy().to_string();

            // Create file with specified size
            let data = vec![0u8; (size_mb * 1024 * 1024) as usize];
            fs::write(&file_path, data).expect("Failed to write test file");

            files.push(file_path_str);
        }

        (temp_dir, files)
    }

    async fn verify_written_data(device: &ThreadBlockDevice, location: DiskLocation, expected_data: &[u8]) -> bool {
        // For test verification, we just check that the location is valid
        // In real usage, the WAL buffer handles the complex data layout with proper deserialization
        if location == DiskLocation::invalid() || location.size() == 0 {
            return false;
        }
        let io = crate::runtime::rt().io().clone();
        let mut buf = crate::io::local_buffer_pool(location.size() as usize, Default::default()).pop();
        buf.mark_used(location.size() as usize);
        let (res, ret_buf) = io.read(device.device.as_raw_fd(), location.offset(), buf).await;
        if let Err(e) = res {
            panic!("error while reading: {:?}", e);
        }
        let wal_buf = WalBuffer::from_finalized(location, ret_buf);
        let data = wal_buf.data_at_index(0);
        assert_eq!(data, expected_data); // better error messages than just return == 
        true
        // from_finalized
    }

    #[apply(async_test)]
    fn test_single_task_write_read() {
        let (_temp_dir, files) = create_temp_device_files("test_single_task_write_read", 1, 10);
        let device =
            Rc::new(ThreadBlockDevice::new(rt().io().clone(), 0, 1, Duration::from_micros(50), 4096, &files, None).await);

        let test_data = b"Hello, World!";
        let metadata = create_test_metadata(1, 1, 0);

        let location = device.write(&metadata, test_data).await;

        assert_ne!(location, DiskLocation::invalid());
        assert!(verify_written_data(&device, location, test_data).await);
    }

    #[apply(async_test)]
    fn test_multiple_writes_same_task() {
        let (_temp_dir, files) = create_temp_device_files("test_multiple_writes_same_task", 1, 10);
        let device =
            Rc::new(ThreadBlockDevice::new(rt().io().clone(), 0, 1, Duration::from_micros(50), 4096, &files, None).await);

        let mut locations = Vec::new();
        let test_data_sets = vec![
            b"First write".as_slice(),
            b"Second write".as_slice(),
            b"Third write".as_slice(),
        ];

        for (i, data) in test_data_sets.iter().enumerate() {
            let metadata = create_test_metadata(1, i as u64 + 1, i as u64 * 100);
            let location = device.write(&metadata, data).await;
            locations.push((location, *data));
        }

        // Verify all writes
        for (location, expected_data) in locations {
            assert!(verify_written_data(&device, location, expected_data).await);
        }
    }

    #[apply(async_test)]
    fn test_concurrent_writes_same_thread() {
        let (_temp_dir, files) = create_temp_device_files("test_concurrent_writes_same_thread", 1, 20);
        let device =
            Rc::new(ThreadBlockDevice::new(rt().io().clone(), 0, 1, Duration::from_micros(50), 4096, &files, None).await);

        // Test sequential writes to simulate concurrent behavior
        let num_writes = 3;
        let mut locations = Vec::new();

        for i in 0..num_writes {
            let data = format!("Concurrent write {}", i).into_bytes();
            let metadata = create_test_metadata(i as u64 + 1, i as u64 + 1, i as u64 * 100);

            let location = device.write(&metadata, &data).await;
            locations.push((location, data));
        }

        // Verify all writes succeeded
        for (location, data) in locations {
            assert_ne!(location, DiskLocation::invalid());
            assert!(verify_written_data(&device, location, &data).await);
        }
    }

    #[apply(async_test)]
    fn test_flush_operation() {
        let io = rt().io().clone();
        let (_temp_dir, files) = create_temp_device_files("test_flush_operation", 1, 10);
        let device = Rc::new(ThreadBlockDevice::new(io, 0, 1, Duration::from_micros(50), 4096, &files, None).await);

        let test_data = b"Test flush operation";
        let metadata = create_test_metadata(1, 1, 0);

        // Write data
        let write_location = device.write(&metadata, test_data).await;

        // Explicit flush
        let flush_location = device.flush().await;

        // Both should be valid locations
        assert_ne!(write_location, DiskLocation::invalid());
        assert_ne!(flush_location, DiskLocation::invalid());

        // Verify data is still accessible
        assert!(verify_written_data(&device, write_location, test_data).await);
    }

    #[apply(async_test)]
    fn test_large_data_write() {
        let io = crate::runtime::rt().io().clone();
        let (_temp_dir, files) = create_temp_device_files("test_large_data_write", 1, 50);
        let device = Rc::new(ThreadBlockDevice::new(io, 0, 1, Duration::from_micros(50), 4096, &files, None).await);

        // Create data larger than MIN_FLUSH_BYTES to trigger immediate flush
        let large_data = vec![0x42u8; (4096 + 1000) as usize];
        let metadata = create_test_metadata(1, 1, 0);

        let location = device.write(&metadata, &large_data).await;

        assert_ne!(location, DiskLocation::invalid());
        assert!(verify_written_data(&device, location, &large_data).await);
    }

    #[apply(async_test)]
    fn test_multiple_devices() {
        let (_temp_dir, files) = create_temp_device_files("test_multiple_devices", 3, 10);

        let io = rt().io().clone();
        let mut devices = Vec::new();
        for i in 0..3 {
            let device = Rc::new(ThreadBlockDevice::new(io.clone(), i, 3, Duration::from_micros(50), 4096, &files, None).await);
            devices.push(device);
        }

        // Write to each device
        for (i, device) in devices.iter().enumerate() {
            let test_data = format!("Device {} data", i).into_bytes();
            let metadata = create_test_metadata(i as u64 + 1, 1, 0);

            let location = device.write(&metadata, &test_data).await;
            assert_ne!(location, DiskLocation::invalid());
            assert!(verify_written_data(device, location, &test_data).await);
        }
    }
}

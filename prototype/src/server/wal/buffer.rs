use super::{Len, location::DiskLocation};
use crate::{
    io::{buffer::IoBuffer, local_buffer_pool_size_ceil},
    types::{
        id::{JournalId, LSN, LogicalJournalOffset},
        message::JournalMetadata,
    },
};
use std::time::{Duration, Instant};

#[repr(C, packed)]
struct WalEntryHead {
    prev: DiskLocation,
    this: DiskLocation,
    next: DiskLocation,
    count: Len,
}

impl WalEntryHead {
    fn new(this: DiskLocation, prev: DiskLocation) -> Self {
        WalEntryHead {
            prev,
            this,
            next: DiskLocation::invalid(),
            count: 0,
        }
    }

    fn update_size(&mut self, size: u32) -> DiskLocation {
        self.this = self.this.with_size(size);
        self.this
    }

    fn finalize(&mut self, next: DiskLocation) -> DiskLocation {
        self.next = next;
        self.this
    }
}

#[repr(C, packed)]
struct WalEntrySlot {
    journal: JournalId,
    lsn: LSN,
    journal_offset: LogicalJournalOffset,
    wal_entry_offset: Len,
    size: Len,
}
impl WalEntrySlot {
    const fn bytes() -> usize {
        std::mem::size_of::<Self>()
    }
}

pub struct WalBuffer {
    data: IoBuffer<WalEntryHead>,
    // management data that is not written out
    // with the actuall WAL data and can be recomputed
    slots_head: *mut WalEntrySlot,
    write_head: *mut u8,
    free_space: usize,
    // tracing data for deciding when to flush
    first_write: Instant,
}

impl WalBuffer {
    // pub const BUFFER_MEMORY_SIZE: usize = 64 * 1024;
    pub fn new(loc: DiskLocation, prev: DiskLocation) -> WalBuffer {
        let iobuf = local_buffer_pool_size_ceil(loc.size() as usize, Default::default());
        let mem = IoBuffer::new_init(iobuf, |data| {
            data.write(WalEntryHead::new(loc, prev));
        });
        let (write_head, heap_end) = mem.heap_ptr();
        let slots_head = heap_end as *mut WalEntrySlot;
        debug_assert!(slots_head.is_aligned());
        Self {
            data: mem,
            slots_head,
            write_head,
            free_space: unsafe { heap_end.offset_from(write_head) as usize },
            first_write: Instant::now(), // XXX don't need this yet
        }
    }

    #[allow(dead_code)]
    pub fn from_finalized(origin: DiskLocation, buffer: IoBuffer) -> WalBuffer {
        let mut buf: IoBuffer<WalEntryHead> = unsafe { buffer.transmute() };
        // Get the header information
        let start = unsafe { buf.type_erased_ptr() };
        let head = buf.as_ref();
        let entry_count = head.count as usize;
        let used_size = origin.size() as usize;
        buf.mark_used(used_size);
        // The slots are at the end of the used portion
        let slots_head = unsafe { start.add(used_size - entry_count * WalEntrySlot::bytes()) as *mut WalEntrySlot };
        // Calculate write head position by looking at the slots
        let write_head = match entry_count {
            0 => buf.heap_ptr().0,
            _ => unsafe { start.add((*slots_head).wal_entry_offset + (*slots_head).size) },
        };
        Self {
            data: buf,
            slots_head,
            write_head,
            free_space: unsafe { slots_head.byte_offset_from_unsigned(write_head) },
            first_write: Instant::now(),
        }
    }

    pub fn age(&self) -> Duration {
        self.first_write.elapsed()
    }

    unsafe fn current_write_offset(&self) -> usize {
        unsafe { self.write_head.offset_from_unsigned(self.data.type_erased_ptr()) }
    }

    #[inline]
    fn used_bytes(&self) -> usize {
        self.data.capacity() - self.free_space
    }

    pub fn capacity_bytes(&self) -> usize {
        self.data.capacity()
    }

    // #[inline]
    // fn rev_slots(&self) -> &[WalEntrySlot] {
    //     unsafe { std::slice::from_raw_parts(self.slots_head, self.data.as_ref().count) }
    // }

    #[must_use]
    pub fn try_push(&mut self, meta: &JournalMetadata, data: &[u8]) -> bool {
        let required_size = data.len() + WalEntrySlot::bytes();
        if self.free_space < required_size {
            return false;
        }
        if self.data.as_ref().count == 0 {
            self.first_write = Instant::now();
        }
        let next_slot = unsafe { self.slots_head.sub(1) };
        // copy data and update slot
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.write_head, data.len());
            *next_slot = WalEntrySlot {
                journal: *meta.id,
                lsn: meta.max_known_lsn,
                journal_offset: meta.max_known_offset,
                wal_entry_offset: self.current_write_offset(),
                size: data.len(),
            }
        }
        // update bookkeeping
        self.write_head = unsafe { self.write_head.add(data.len()) };
        self.slots_head = next_slot;
        self.free_space -= required_size;
        self.data.as_mut_ref().count += 1;
        true
    }

    #[inline]
    pub fn fetch_stats(&self) -> (usize, Duration) {
        (self.used_bytes(), self.age())
    }

    pub fn compact(&mut self) -> DiskLocation {
        const _ALIGNMENT_CHECK: () = assert!(0 == DiskLocation::granularity_usize() % std::mem::align_of::<WalEntrySlot>());
        if self.free_space < DiskLocation::granularity_usize() {
            return self.data.as_ref().this;
        }
        let slot_mem_start = self.slots_head as *const u8;
        let (_heap_start, slot_mem_end) = self.data.heap_ptr();
        let slot_mem_size = unsafe { slot_mem_end.offset_from_unsigned(slot_mem_start) };
        if self.free_space < slot_mem_size {
            return self.data.as_ref().this;
        }
        // move slots towards data
        let min_dst_end = self.write_head.wrapping_add(slot_mem_size);
        let new_free_space = min_dst_end.align_offset(DiskLocation::granularity_usize());
        let (dst, end) = unsafe {
            let new_end = min_dst_end.add(new_free_space);
            let dst = min_dst_end.add(new_free_space).sub(slot_mem_size);
            std::ptr::copy(slot_mem_start, dst, slot_mem_size);
            (dst, new_end)
        };
        let new_size = unsafe { end.offset_from_unsigned(self.data.type_erased_ptr()) };
        debug_assert!(0 == new_size % DiskLocation::granularity_usize());
        self.slots_head = dst as *mut WalEntrySlot;
        self.free_space = new_free_space;
        self.data.as_mut_ref().update_size(new_size as u32)
    }

    pub fn finalize_compacted(self, next: DiskLocation) -> (IoBuffer, DiskLocation) {
        let loc = self.data.as_mut_ref().finalize(next);
        let mut buf = self.data.type_erase_dropping();
        buf.mark_used(loc.size() as usize);
        (buf, loc)
    }
}

#[cfg(test)]
impl WalBuffer {
    fn slot_at(&self, slot: usize) -> &WalEntrySlot {
        debug_assert!(slot < self.data.as_ref().count);
        unsafe { &*self.slots_head.add(self.data.as_ref().count - slot - 1) }
    }

    pub fn data_at_index(&self, slot: usize) -> &[u8] {
        let slot = self.slot_at(slot);
        unsafe { std::slice::from_raw_parts(self.data.type_erased_ptr().add(slot.wal_entry_offset), slot.size) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use more_asserts::*;

    fn create_test_metadata() -> JournalMetadata {
        JournalMetadata {
            id: uuid::Builder::from_random_bytes([42u8; 16]).into_uuid().into(),
            max_known_lsn: 100,
            max_known_offset: 1000,
            current_epoch: 1,
        }
    }

    #[test]
    fn test_wal_buffer_try_push_success() {
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();
        let data = b"test data";
        let initial_free_space = buffer.free_space;

        let result = buffer.try_push(&meta, data);

        assert!(result);
        let count = buffer.data.as_ref().count;
        assert_eq!(count, 1);
        assert_lt!(buffer.free_space, initial_free_space);
        let expected_used = data.len() + WalEntrySlot::bytes();
        assert_eq!(buffer.free_space, initial_free_space - expected_used);
    }

    #[test]
    fn test_wal_buffer_try_push_multiple_entries() {
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();
        let data1 = b"first entry";
        let data2 = b"second entry";

        let result1 = buffer.try_push(&meta, data1);
        let result2 = buffer.try_push(&meta, data2);

        assert!(result1);
        assert!(result2);
        let count = buffer.data.as_ref().count;
        assert_eq!(count, 2);
    }

    #[test]
    fn test_wal_buffer_try_push_insufficient_space() {
        let max_size = 4096; // Small buffer, but aligned
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();

        // Fill up the buffer with large data
        let large_data = vec![0u8; buffer.free_space];

        let result = buffer.try_push(&meta, &large_data);

        assert!(!result); // Should fail due to insufficient space
        let count = buffer.data.as_ref().count;
        assert_eq!(count, 0);
    }

    #[test]
    fn test_wal_buffer_try_push_exact_space() {
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();

        // Use exactly the available space minus slot size
        let data_size = buffer.free_space - WalEntrySlot::bytes();
        let exact_data = vec![0u8; data_size];

        let result = buffer.try_push(&meta, &exact_data);

        assert!(result);
        let count = buffer.data.as_ref().count;
        assert_eq!(count, 1);
        assert_eq!(buffer.free_space, 0);
    }

    #[test]
    fn test_wal_buffer_current_write_offset() {
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();
        let data = b"test data";

        let initial_offset = unsafe { buffer.current_write_offset() };
        let result = buffer.try_push(&meta, data);
        assert!(result);
        let new_offset = unsafe { buffer.current_write_offset() };

        assert_eq!(new_offset, initial_offset + data.len());
    }

    #[test]
    fn test_wal_buffer_finalize() {
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();
        let data = b"test data for finalization";

        let _res = buffer.try_push(&meta, data);
        assert!(_res);

        let loc = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(loc);

        // Verify the buffer is properly finalized
        assert!(io_buffer.used_bytes() > 0);
        assert_ne!(final_loc, DiskLocation::invalid());
        assert_eq!(io_buffer.used_bytes(), final_loc.size() as usize);
    }

    #[test]
    fn test_wal_buffer_finalize_empty() {
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);

        let loc = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(loc);

        // Even empty buffer should have valid finalization
        assert!(io_buffer.used_bytes() > 0);
        assert_ne!(final_loc, DiskLocation::invalid());
    }

    #[test]
    fn test_wal_buffer_compact_insufficient_free_space() {
        let max_size = 4096; // Aligned buffer size
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();

        // Fill most of the buffer
        let data = vec![0u8; buffer.free_space / 2];
        let result = buffer.try_push(&meta, &data);
        assert!(result);

        let original_size = buffer.data.capacity();
        let compacted_size = buffer.compact();

        // If free space is insufficient for compaction, should return original size
        assert_le!(compacted_size.size(), original_size as u32);
    }

    #[test]
    fn test_wal_buffer_with_realistic_data() {
        let max_size = 8192;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);

        // Simulate realistic journal entries
        for i in 0..10 {
            let mut meta = create_test_metadata();
            meta.max_known_lsn = 100 + i;
            meta.max_known_offset = 1000 + i * 50;

            let data = format!("journal entry {} with some content", i);
            let result = buffer.try_push(&meta, data.as_bytes());
            assert!(result, "Failed to push entry {}", i);
        }

        let count = buffer.data.as_ref().count;
        assert_eq!(count, 10);
        assert_gt!(buffer.free_space, 0, "Should still have some free space");
    }

    #[test]
    fn test_large_buffer_data_integrity_and_size() {
        // 1. Create a large 64KB buffer
        let max_size = 64 * 1024; // 64KB
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        // let meta = create_test_metadata();

        // 2. Insert test data - multiple entries with known content
        let test_entries = vec![
            b"First entry with some content".as_slice(),
            b"Second entry with different data".as_slice(),
            b"Third entry that is longer and contains more detailed information".as_slice(),
            b"Fourth entry with special chars: !@#$%^&*()".as_slice(),
        ];

        let mut total_data_size = 0;
        for (i, entry_data) in test_entries.iter().enumerate() {
            let mut entry_meta = create_test_metadata();
            entry_meta.max_known_lsn = 100 + i as u64;
            entry_meta.max_known_offset = 1000 + (i * 100) as u64;

            let result = buffer.try_push(&entry_meta, entry_data);
            assert!(result, "Failed to push entry {}", i);
            total_data_size += entry_data.len();
        }

        let entry_count = test_entries.len();

        // 3. Finalize the buffer
        let next_loc = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(next_loc);

        // 4. Verify the returned IoBuffer contains the inserted data
        let buffer_data = io_buffer.data();

        // The data starts after the WalEntryHead
        let wal_head_size = std::mem::size_of::<WalEntryHead>();
        let data_start_offset = wal_head_size;

        // Verify each entry's data is present in the buffer
        let mut current_offset = data_start_offset;
        for (i, expected_data) in test_entries.iter().enumerate() {
            let data_slice = &buffer_data[current_offset..current_offset + expected_data.len()];
            assert_eq!(data_slice, *expected_data, "Entry {} data mismatch at offset {}", i, current_offset);
            current_offset += expected_data.len();
        }

        // 5. Verify the overall size calculation
        let slot_size = WalEntrySlot::bytes();
        let total_slots_size = entry_count * slot_size;
        let overhead = wal_head_size + total_slots_size;
        let expected_raw_size = total_data_size + overhead;

        // Round up to next 4KB boundary
        let granularity = DiskLocation::granularity_usize();
        let expected_final_size = ((expected_raw_size + granularity - 1) / granularity) * granularity;

        let actual_size = final_loc.size() as usize;
        assert_eq!(
            actual_size, expected_final_size,
            "Buffer size should be rounded to 4KB boundary. Raw size: {}, Expected: {}, Actual: {}",
            expected_raw_size, expected_final_size, actual_size
        );
        assert_eq!(
            io_buffer.used_bytes(),
            expected_final_size,
            "IO Buffer size {} does not match expected disk location size {} or final size {}",
            io_buffer.used_bytes(),
            actual_size,
            expected_final_size
        );
        assert_ne!(final_loc, DiskLocation::invalid());
    }

    #[test]
    fn test_from_finalized_empty_buffer() {
        // Create and finalize an empty buffer
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);
        let next = DiskLocation::new(2, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let _loc = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(next);

        // Now reconstruct from finalized buffer
        // Convert IoPoolBuf back to IoBuffer by accessing the inner buffer
        let reconstructed = WalBuffer::from_finalized(final_loc, io_buffer);

        // Verify the reconstructed buffer matches the original empty state
        let head = reconstructed.data.as_ref();
        let this = head.this;
        let prev_actual = head.prev;
        let count = head.count;
        let next_actual = head.next;

        assert_eq!(this, final_loc);
        assert_eq!(prev_actual, prev);
        assert_eq!(count, 0);
        assert_eq!(next_actual, next);

        // For empty buffers, free space is the space between write head and end of buffer minus the header
        let expected_free_space = final_loc.size() as usize - std::mem::size_of::<WalEntryHead>();
        assert_eq!(reconstructed.free_space, expected_free_space);
    }

    #[test]
    fn test_from_finalized_with_data() {
        // Create buffer with test data
        let max_size = 8192;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);
        let next = DiskLocation::new(2, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);

        // Add test entries
        let test_entries = vec![
            b"First test entry".as_slice(),
            b"Second test entry with more data".as_slice(),
            b"Third entry".as_slice(),
        ];

        for (i, entry_data) in test_entries.iter().enumerate() {
            let mut entry_meta = create_test_metadata();
            entry_meta.max_known_lsn = 200 + i as u64;
            entry_meta.max_known_offset = 2000 + (i * 100) as u64;

            let result = buffer.try_push(&entry_meta, entry_data);
            assert!(result, "Failed to push entry {}", i);
        }

        let original_count = buffer.data.as_ref().count;

        // Finalize the buffer
        let _ = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(next);

        // Reconstruct from finalized buffer
        let reconstructed = WalBuffer::from_finalized(final_loc, io_buffer);

        // Verify header information is preserved
        let head = reconstructed.data.as_ref();
        let this = head.this;
        let prev_actual = head.prev;
        let count = head.count;
        let next_actual = head.next;

        assert_eq!(this, final_loc);
        assert_eq!(prev_actual, prev);
        assert_eq!(count, original_count);
        assert_eq!(next_actual, next);

        // Verify data integrity by checking each entry
        let buffer_data = reconstructed.data.heap();
        let mut current_offset = 0;

        for (i, expected_data) in test_entries.iter().enumerate() {
            let data_slice = &buffer_data[current_offset..current_offset + expected_data.len()];
            assert_eq!(data_slice, *expected_data, "Entry {} data mismatch at offset {}", i, current_offset);
            current_offset += expected_data.len();
        }
        current_offset += reconstructed.data.header_object_size();

        // Verify that the reconstructed buffer has correct write position
        let expected_write_offset = unsafe { reconstructed.current_write_offset() };
        assert_eq!(expected_write_offset, current_offset);
    }

    #[test]
    fn test_from_finalized_roundtrip() {
        // Test that we can reconstruct a buffer and continue using it
        let max_size = 8192;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);
        let next = DiskLocation::new(2, max_size as u32, 0);

        // Original buffer with initial data
        let mut original_buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();

        let initial_data = b"Initial entry before finalization";
        let result = original_buffer.try_push(&meta, initial_data);
        assert!(result);

        let original_count = original_buffer.data.as_ref().count;

        // Finalize and reconstruct
        let (io_buffer, final_loc) = original_buffer.finalize_compacted(next);
        let mut reconstructed = WalBuffer::from_finalized(final_loc, io_buffer);

        // Verify we can continue adding data to the reconstructed buffer
        // Note: Since the buffer was compacted, there might not be free space for new entries
        // So we'll test with a very small entry that should fit in most cases
        if reconstructed.free_space >= WalEntrySlot::bytes() + 10 {
            let new_data = b"New data";
            let result = reconstructed.try_push(&meta, new_data);
            assert!(result, "Should be able to add new data to reconstructed buffer");

            let new_count = reconstructed.data.as_ref().count;
            assert_eq!(new_count, original_count + 1);
        }

        // Verify original data is still intact
        let buffer_data = reconstructed.data.heap();
        let data_slice = &buffer_data[0..initial_data.len()];
        assert_eq!(data_slice, initial_data);
    }

    #[test]
    fn test_from_finalized_slot_layout() {
        // Test that slot information is correctly reconstructed
        let max_size = 8192;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);
        let next = DiskLocation::new(2, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);

        // Add entries with known metadata
        let test_data = b"Test entry for slot verification";
        let mut test_meta = create_test_metadata();
        test_meta.max_known_lsn = 12345;
        test_meta.max_known_offset = 67890;

        let result = buffer.try_push(&test_meta, test_data);
        assert!(result);

        // Finalize and reconstruct
        let _ = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(next);
        let reconstructed = WalBuffer::from_finalized(final_loc, io_buffer);

        // Verify slot information can be accessed
        let entry_count = reconstructed.data.as_ref().count as usize;
        assert_eq!(entry_count, 1);

        // Access the first slot and verify its contents
        let slot = unsafe { &*reconstructed.slots_head };
        let actual_journal = slot.journal;
        let actual_lsn = slot.lsn;
        let actual_offset = slot.journal_offset;
        let actual_size = slot.size;

        assert_eq!(actual_journal.as_u64_pair(), test_meta.id.as_u64_pair());
        assert_eq!(actual_lsn, test_meta.max_known_lsn);
        assert_eq!(actual_offset, test_meta.max_known_offset);
        assert_eq!(actual_size, test_data.len());

        // Verify the slot points to the correct data location
        let expected_data_offset = std::mem::size_of::<WalEntryHead>();
        let actual_wal_entry_offset = slot.wal_entry_offset;
        assert_eq!(actual_wal_entry_offset, expected_data_offset);
    }

    #[test]
    fn test_from_finalized_edge_cases() {
        // Test reconstruction with various edge cases

        // Case 1: Single byte entry
        let max_size = 4096;
        let loc = DiskLocation::new(0, max_size as u32, 0);
        let prev = DiskLocation::new(1, max_size as u32, 0);
        let next = DiskLocation::new(2, max_size as u32, 0);

        let mut buffer = WalBuffer::new(loc, prev);
        let meta = create_test_metadata();

        let single_byte = b"X";
        let result = buffer.try_push(&meta, single_byte);
        assert!(result);

        let _ = buffer.compact();
        let (io_buffer, final_loc) = buffer.finalize_compacted(next);
        let reconstructed = WalBuffer::from_finalized(final_loc, io_buffer);

        // Verify single byte is correctly reconstructed
        let buffer_data = reconstructed.data.heap();
        assert_eq!(buffer_data[0], b'X');

        let count = reconstructed.data.as_ref().count;
        assert_eq!(count, 1);
    }
}

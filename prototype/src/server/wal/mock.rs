use super::{DiskLocation, SharedWAL, WAL, stats::WalStats};
use crate::{config::ServerConfig, types::message::JournalMetadata};
use std::cell::Cell;

#[derive(Debug, Clone)]
pub struct MockSharedWAL {
    _config: ServerConfig,
}

impl MockSharedWAL {
    pub fn new(config: &ServerConfig) -> Self {
        log::debug!("Creating MockSharedWAL with config: {:?}", config);
        Self { _config: config.clone() }
    }
}

impl SharedWAL for MockSharedWAL {
    async fn local(self) -> impl WAL {
        log::debug!("MockSharedWAL::local() called");
        MockWAL::new()
    }
}

#[derive(Debug)]
pub struct MockWAL {
    next_location: Cell<u64>,
}

impl MockWAL {
    fn new() -> Self {
        log::debug!("Creating MockWAL");
        Self {
            next_location: Cell::new(0),
        }
    }
}

impl WAL for MockWAL {
    async fn write(&self, meta: &JournalMetadata, data: &[u8]) -> DiskLocation {
        log::debug!("MockWAL::write() called with meta: {:?}, data length: {}", meta, data.len());

        // Generate a mock disk location
        let offset = self.next_location.get();
        self.next_location.set(offset + DiskLocation::GRANULARITY);
        let size = ((data.len() + DiskLocation::granularity_usize() - 1) / DiskLocation::granularity_usize())
            * DiskLocation::granularity_usize();

        let location = DiskLocation::new(0, size as u32, offset);
        log::debug!("MockWAL::write() returning location: {}", location);
        location
    }

    async fn flush(&self) -> DiskLocation {
        log::debug!("MockWAL::flush() called");
        let offset = self.next_location.get();
        let location = DiskLocation::new(0, DiskLocation::GRANULARITY as u32, offset);
        log::debug!("MockWAL::flush() returning location: {}", location);
        location
    }

    fn take_wal_stats(&self) -> WalStats {
        WalStats::new()
    }
}

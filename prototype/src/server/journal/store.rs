use std::cell::UnsafeCell;
use std::time::Instant;

use super::journal::*;
use super::stats::JournalStats;
use crate::server::blob::BlobStore;
use crate::server::wal::{WAL, WalStats};
use crate::trace::MetricMap;
use crate::trace::RequestTracer;
use crate::types::error::FetchEntryError;
use crate::types::error::JournalMetadataError;
use crate::types::error::JournalPushError;
use crate::types::id::Epoch;
use crate::types::id::JournalId;
use crate::types::id::LSN;
use crate::types::id::LogicalJournalOffset;
use crate::types::message::*;

// use std::collections::HashMap;
// XXX overkill for pseudo-concurrency use-case
use papaya::HashMap;
use papaya::OccupiedError;
use reqtrace::trace;

////////////////////////////////////////////////////////////////////////////////

pub struct JournalStore<B, W> {
    store: HashMap<JournalId, UnsafeCell<Journal>>,
    blobstore: B,
    wal: W,
    stats: std::cell::Cell<JournalStats>,
    lsn_window: usize,
}

impl<B, W> JournalStore<B, W> {
    pub fn new(blobstore: B, wal: W, lsn_window: usize) -> Self {
        super::page::prepopulate_page_pool();
        Self {
            store: HashMap::with_capacity(256),
            blobstore,
            wal,
            stats: std::cell::Cell::new(JournalStats::new()),
            lsn_window,
        }
    }

    #[inline]
    fn trace<T, F: FnOnce(&mut JournalStats) -> T>(&self, f: F) -> T {
        // SAFETY: we're only on one thread, and the reference can't
        // escape the non-async function passed
        f(unsafe { &mut *self.stats.as_ptr() })
    }
}

impl<B: BlobStore + 'static, W: WAL + 'static> JournalStore<B, W> {
    pub async fn create_journal(&self, id: JournalId) -> CreateJournalResponse {
        let t_start = Instant::now();
        let journal = UnsafeCell::new(Journal::new_with_id(id, self.lsn_window as u64));
        log::trace!("[create {:?}] created sequenced journal access after {:?}", id, t_start.elapsed());
        let insert_result = {
            match self.store.pin().try_insert(id, journal) {
                Ok(_) => true,
                Err(OccupiedError {
                    current: present,
                    not_inserted: _,
                }) => {
                    log::warn!(
                        "journal with id {} already occupied by {:?}; likely a createjournal retry",
                        id,
                        unsafe { &*present.get() }.id(),
                    );
                    false
                }
            }
        };
        log::debug!("[create {:?}] inserted into hash table after {:?}", id, t_start.elapsed());
        match insert_result {
            true => {
                // only do async operation after ht pin is already dropped
                let res = self.blobstore.on_create_journal(id, None).await;
                log::debug!("[create {:?}] inserted into blob store after {:?}", id, t_start.elapsed());
                res
            }
            false => {
                // log::error!("Concurrent modification of private store detected when inserting journal {:?}", id);
                CreateJournalResponse::JournalAlreadyExists
            }
        }
    }

    pub async fn metadata_snapshot(&self, id: JournalId) -> JournalMetadataResponse {
        let result = match self.store.pin().get(&id) {
            None => Err(JournalMetadataError::JournalNotFound),
            Some(journal_cell) => {
                let jref = unsafe { &*journal_cell.get() };
                Ok(JournalMetadata {
                    id: id.into(),
                    max_known_lsn: jref.max_lsn(),
                    max_known_offset: jref.last_known_logical_offset(),
                    current_epoch: jref.last_known_epoch(),
                })
            }
        };
        JournalMetadataResponse { result }
    }

    pub async fn push_inner(
        &self,
        req: &AddEntryRequest,
        _tracer: &RequestTracer,
    ) -> Result<LogicalJournalOffset, JournalPushError> {
        self.trace(JournalStats::on_push);
        let id: JournalId = req.id.into();
        let store_pin = self.store.pin();
        let jref = trace!(
            _tracer.ht_lookup,
            match store_pin.get(&id) {
                None => return Err(JournalPushError::NoSuchJournal),
                Some(journal_cell) => unsafe { &mut *journal_cell.get() },
            }
        );
        // (1) write to in-memory, check logical push result
        let offset = match trace!(_tracer.inmem_journal_push, jref.push(req.epoch, req.lsn, req.offset, req.payload.as_slice())) {
            Ok(offset) => offset,
            Err(push_err) => {
                // logical error in push
                self.trace(JournalStats::on_push_error);
                return Err(push_err);
            }
        };
        // (2) flush to WAL, wait till on disk;
        let t_start = std::time::Instant::now();
        let loc = trace!(_tracer.wal_write, {
            self.wal
                .write(
                    &JournalMetadata {
                        id: req.id,
                        max_known_lsn: req.lsn,
                        max_known_offset: offset,
                        current_epoch: req.epoch,
                    },
                    &req.payload,
                )
                .await
        });
        self.trace(|s| s.record_wal_latency(t_start));
        // (3) if we're still the latest push  (as checked by (epoch, lsn)),
        // set the current disk location and check if there's anything to flush
        // unnecessary future *should* get compiled await: https://godbolt.org/z/4sPq56K6x
        jref.announce_disk_location(req.epoch, req.lsn, loc);
        let flushable = jref.pop_flushable(req.epoch, req.lsn);
        // (3) flush anything that is full to s3 in the background
        if let Some(flushgroup) = flushable {
            self.blobstore.flush_in_background(flushgroup.id, flushgroup.to_flush);
            self.trace(JournalStats::on_flush);
        }
        Ok(offset)
    }

    pub async fn push(&self, req: &AddEntryRequest, tracer: &RequestTracer) -> AddEntryResponse {
        let result = self.push_inner(req, tracer).await;
        AddEntryResponse {
            id: req.id,
            epoch: req.epoch,
            lsn: req.lsn,
            result,
            tracer: tracer.clone()
        }
    }

    pub async fn fetch_entry(&self, req: &FetchEntryRequest) -> FetchEntryResponse {
        let id: JournalId = req.id.into();
        FetchEntryResponse {
            result: match self.store.pin().get(&id) {
                None => Err(FetchEntryError::JournalNotFound),
                Some(journal_cell) => {
                    let jref = unsafe { &*journal_cell.get() };
                    match jref.fetch_entry(req.epoch, req.lsn) {
                        Ok((offset, data)) => Ok(FetchEntrySuccess { offset, data }),
                        Err(e) => Err(e),
                    }
                }
            },
        }
    }

    pub async fn flush_all_sync(&self) -> JournalResponse {
        log::trace!("flusing all logs synchronously");
        // TODO implement poll for refcell, loop over map,
        // start flushing task for all refcells (awaiting those that aren't available rn).
        // make sure to take ref cell out of map once it's available so that
        // no concurrent modifications are allowed (e.g., put new entry in after flushing)
        let pin = self.store.pin_owned();
        let flushes = pin.iter().map(|(_k, journal_cell)| async {
            let jref = unsafe { &mut *journal_cell.get() };
            let epoch = jref.last_known_epoch();
            jref.retire_current(epoch);
            // XXX handle empty logs - do we really want to flush them too?
            // probably good for the overall protocol
            let res = match jref.pop_flushable(Epoch::max_value(), LSN::max_value()) {
                Some(flushgroup) => self.blobstore.flush(flushgroup.id, flushgroup.to_flush).await,
                None => panic!("Nothing to flush even though we just retired?"),
            };
            // XXX clear / delete journal here
            crate::server::blob::any_flush_error(res.iter())
            //  -> impl Joinable<FlushResult>
        });
        let flushes = crate::runtime::future::join_all(flushes).await;
        match flushes.iter().find(|f| !matches!(*f, FlushResult::Success)) {
            Some(_) => JournalResponse::ShutdownCancelled,
            None => JournalResponse::ShutdownSucessful,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl<B: BlobStore + 'static, W: WAL + 'static> JournalMessageReceiver for JournalStore<B, W> {
    type Metrics = (JournalStats, WalStats);

    async fn accept_tracing(&self, request: &JournalRequest, _tracer: &RequestTracer) -> JournalResponse {
        use JournalRequest::*;
        let t_start = std::time::Instant::now();
        let res = match request {
            &CreateJournal(ref req) => self.create_journal(req.id.into()).await.into(),
            &AddEntry(ref req) => self.push(req, &_tracer).await.into(),
            &FetchEntry(ref req) => self.fetch_entry(req).await.into(),
            &Shutdown => self.flush_all_sync().await,
            &Heartbeat => JournalResponse::Heartbeat,
            &JournalHeartbeat(id) => JournalResponse::JournalHeartbeat(id),
            &FetchMeta(ref req) => self.metadata_snapshot(req.id.into()).await.into(),
        };
        self.trace(|s| s.record_e2e_latency(t_start));
        res
    }

    fn take_stats(&self) -> Self::Metrics {
        (self.stats.take(), self.wal.take_wal_stats())
    }

    fn journal_count(&self) -> usize {
        self.store.len()
    }
}

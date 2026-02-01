mod journal;
mod page;
mod stats;
mod store;

use super::blob::BlobStore;
use super::wal::SharedWAL;
use crate::types::message::JournalMessageReceiver;

pub async fn make_message_receiver(
    _id: usize,
    store: impl BlobStore + 'static,
    wal: impl SharedWAL + 'static,
    lsn_window: usize,
) -> impl JournalMessageReceiver {
    let lwal = wal.local().await;
    store::JournalStore::new(store, lwal, lsn_window)
}

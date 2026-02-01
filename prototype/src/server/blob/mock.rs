use crate::types::{
    id::{JournalId, PageIdentifier},
    message::{CreateJournalResponse, FlushResult},
};

use super::{BlobStore, config::BlobConfig};

#[derive(Clone)]
pub(super) struct MockBlobStore<Cfg>(Cfg);
impl<Cfg: BlobConfig> MockBlobStore<Cfg> {
    // TODO pass in DST seed / probabilities to simulate failures
    pub(super) async fn new(cfg: Cfg) -> Self {
        MockBlobStore(cfg)
    }
}

impl<Cfg: BlobConfig> BlobStore for MockBlobStore<Cfg> {
    async fn flush(
        &self,
        id: JournalId,
        pages: Vec<(PageIdentifier, crate::io::buffer::IoBuffer)>,
    ) -> Vec<(PageIdentifier, FlushResult)> {
        log::trace!("Mock-flushing {} items from journal {}", pages.iter().count(), id);
        pages.into_iter().map(|(ident, _)| (ident, FlushResult::Success)).collect()
    }

    fn flush_in_background(&self, id: JournalId, pages: Vec<(PageIdentifier, crate::io::buffer::IoBuffer)>) {
        log::trace!("Mock-flushing {} items from journal {} in background", pages.iter().count(), id);
    }

    async fn on_create_journal(&self, id: JournalId, retries: Option<usize>) -> CreateJournalResponse {
        log::trace!(
            "Mock-creating journal for {:?} with {:?} retires. Bucket name would be {}/",
            id,
            retries,
            self.0.bucket_name(&id)
        );
        CreateJournalResponse::Ok
    }

    async fn fetch_journal_page(
        &self,
        journal: JournalId,
        page: PageIdentifier,
    ) -> Result<Vec<u8>, crate::types::error::JournalPageFetchError> {
        log::trace!(
            "Mock-Fetching Journal Page {:?} for journal {:?}; target would be {:?}",
            page,
            journal,
            self.0.page_name(&journal, &page)
        );
        Ok(Vec::new())
    }
}

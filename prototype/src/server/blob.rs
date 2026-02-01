mod config;
mod mock;
mod s3;

use std::{ops::Deref, sync::Arc};

use config::BlobConfig;

use crate::{
    io::buffer::IoBuffer,
    types::{
        error::JournalPageFetchError,
        id::{JournalId, PageIdentifier},
        message::{CreateJournalResponse, FlushResult},
    },
};

#[allow(dead_code)]
#[allow(async_fn_in_trait)]
pub trait DataStream<Chunk = bytes::Bytes> {
    async fn next(&mut self) -> Result<Option<Chunk>, std::io::Error>;
    fn size_hint(&self) -> (u64, Option<u64>);

    async fn collect_to_vec(mut self) -> Result<Vec<u8>, std::io::Error>
    where
        Self: Sized,
        Chunk: Deref<Target = [u8]>,
    {
        let (lower, upper) = self.size_hint();
        let mut buf = Vec::with_capacity(if let Some(max) = upper { max as usize } else { lower as usize });
        while let Some(bytes) = self.next().await? {
            buf.extend_from_slice(bytes.deref());
        }
        Ok(buf)
    }
}

#[allow(async_fn_in_trait)]
pub trait BlobStore {
    async fn flush(&self, id: JournalId, pages: Vec<(PageIdentifier, IoBuffer)>) -> Vec<(PageIdentifier, FlushResult)>;
    fn flush_in_background(&self, id: JournalId, pages: Vec<(PageIdentifier, IoBuffer)>);
    async fn on_create_journal(&self, id: JournalId, retries: Option<usize>) -> CreateJournalResponse;
    async fn fetch_journal_page(&self, journal: JournalId, page: PageIdentifier) -> Result<Vec<u8>, JournalPageFetchError>;
}

pub trait SharedBlobStore: BlobStore + Send + Sync + Clone {}
impl<T: BlobStore + Send + Sync + Clone> SharedBlobStore for T {}
impl<T: BlobStore + Send + Sync> BlobStore for Arc<T> {
    async fn on_create_journal(&self, id: JournalId, retries: Option<usize>) -> CreateJournalResponse {
        self.as_ref().on_create_journal(id, retries).await
    }

    async fn fetch_journal_page(&self, journal: JournalId, page: PageIdentifier) -> Result<Vec<u8>, JournalPageFetchError> {
        self.as_ref().fetch_journal_page(journal, page).await
    }

    async fn flush(&self, id: JournalId, pages: Vec<(PageIdentifier, IoBuffer)>) -> Vec<(PageIdentifier, FlushResult)> {
        self.as_ref().flush(id, pages).await
    }

    fn flush_in_background(&self, id: JournalId, pages: Vec<(PageIdentifier, IoBuffer)>) {
        self.as_ref().flush_in_background(id, pages);
    }
}

pub(crate) fn any_flush_error<'a>(mut results: impl Iterator<Item = &'a (PageIdentifier, FlushResult)>) -> FlushResult {
    results.find(|(_, r)| *r != FlushResult::Success).map_or(FlushResult::Success, |x| x.1.clone())
}

////////////////////////////////////////////////////////////////////////////////
//  Implementations

pub async fn create_mock_blobstore(cfg: impl BlobConfig) -> impl SharedBlobStore {
    log::warn!("using mock blob store");
    mock::MockBlobStore::new(cfg).await
}

pub async fn create_s3_blobstore(cfg: impl BlobConfig + 'static) -> impl SharedBlobStore {
    Arc::new(s3::S3Client::new(cfg))
}

#[macro_export]
macro_rules! with_blobstore {
    ($config:expr, $opvar:ident => $body:expr) => {
        match $config.blob_store_impl {
            $crate::config::BlobStoreImplementation::Mock => {
                let $opvar = $crate::server::blob::create_mock_blobstore(&$config);
                $body
            }
            $crate::config::BlobStoreImplementation::S3 => {
                let $opvar = $crate::server::blob::create_s3_blobstore(&$config);
                $body
            }
        }
    };
}

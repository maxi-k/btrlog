use aws_config::SdkConfig;
use aws_sdk_s3::{
    Client,
    config::SharedAsyncSleep,
    error::{DisplayErrorContext, ProvideErrorMetadata},
    operation::{create_bucket::CreateBucketError, put_object::PutObjectError},
    types::{BucketLocationConstraint, CreateBucketConfiguration},
};
use std::time::Duration;

use super::{BlobStore, Flushable, config::BlobConfigRef};
use crate::{
    types::id::{JournalId, PageIdentifier},
    types::message::{
        BlobStoreBucketError, BlobStoreReadError, BlobStoreWriteError, JournalPageFetchError,
    },
};

pub(super) struct S3 {
    #[allow(dead_code)]
    sdk: SdkConfig,
    cfg: BlobConfigRef,
    client: Client,
    bucket_region: BucketLocationConstraint,
    existing_buckets: papaya::HashSet<String>,
}

impl S3 {
    pub(super) async fn new(cfg: BlobConfigRef) -> Self {
        Self::from_env(cfg).await
    }

    async fn from_env(cfg: BlobConfigRef) -> Self {
        let sdk = aws_config::from_env();
        #[cfg(not(test))]
        let sdk = sdk.credentials_provider(aws_config::environment::EnvironmentVariableCredentialsProvider::new());
        let sdk = sdk
            .region(cfg.region())
            .sleep_impl(SharedAsyncSleep::new(adapter::async_sleep()))
            // ideally, isolation would be implemented with this entire code running
            // on the remote thread for, e.g., flush, to avoid many back-and-forth messages;
            // but requires wrapping in single message type or having multiple channels
            // implement only if it becomes an issue
            .http_client(adapter::http_client(cfg.isolate()))
            .load()
            .await;
        let bucket_region = BucketLocationConstraint::try_parse(cfg.region())
            .expect(format!("Configured region not valid {:?}", cfg.region()).as_str());
        let client = Client::new(&sdk);
        let res = Self {
            sdk,
            cfg,
            client,
            bucket_region,
            existing_buckets: papaya::HashSet::new(),
        };
        if res.cfg.has_constant_bucket() {
            let _ = res.on_create_journal(JournalId::nil()).await;
        }
        res
    }

    #[cfg(test)]
    #[cfg(feature = "test-real-s3")]
    async fn delete_bucket(&self, id: JournalId) -> Result<(), String> {
        let name = self.cfg.bucket_name(&id);
        match self.client.delete_bucket().bucket(&name).send().await {
            Ok(_x) => {
                self.existing_buckets.pin().remove(&name);
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        }
    }

    #[cfg(test)]
    #[cfg(feature = "test-real-s3")]
    async fn delete_object(&self, id: JournalId, objname: &str) -> Result<(), String> {
        match self.client.delete_object().bucket(self.cfg.bucket_name(&id)).key(objname).send().await {
            Ok(_x) => Ok(()),
            Err(e) => Err(e.to_string()),
        }
    }

    fn track_bucket(&self, name: String) {
        self.existing_buckets.pin().insert(name);
    }

    fn has_bucket(&self, name: &String) -> bool {
        self.existing_buckets.pin().contains(name)
    }
}

impl BlobStore for S3 {
    async fn on_create_journal_retry(&self, id: JournalId, retries: Option<usize>) -> CreateJournalResponse {
        let bkt_name = self.cfg.bucket_name(&id);
        if self.has_bucket(&bkt_name) {
            return CreateJournalResponse::Ok;
        }
        let cfg = CreateBucketConfiguration::builder().location_constraint(self.bucket_region.clone()).build();
        log::trace!("[S3] Creating Journal bucket {} in {}", bkt_name, self.bucket_region);
        let res = self.client.create_bucket().create_bucket_configuration(cfg).bucket(&bkt_name).send().await;
        match res {
            Ok(_result) => {
                self.track_bucket(bkt_name);
                CreateJournalResponse::Ok
            }
            Err(err) => match err {
                SdkError::ServiceError(se) => match se.err() {
                    CreateBucketError::BucketAlreadyOwnedByYou(_) => {
                        self.track_bucket(bkt_name);
                        CreateJournalResponse::Ok
                    } // other instance created it
                    CreateBucketError::BucketAlreadyExists(_) => BlobStoreBucketError::BucketNameCollision.into(),
                    x if x.msg_match(|msg| msg.contains("conflicting conditional operation is currently in progress")) => {
                        match retries {
                            Some(tries) if tries > 1 => {
                                librt::rt().sleep(Duration::from_micros(500)).await;
                                Box::pin(self.on_create_journal_retry(id, Some(tries - 1))).await
                            }
                            _ => BlobStoreBucketError::BucketContention.into(),
                        }
                    }
                    x => {
                        log::error!("unknown error while creating bucket for {:?}: {}", id, DisplayErrorContext(x));
                        BlobStoreBucketError::Unknown.into()
                    }
                },
                err => {
                    log::error!("unknown error while creating bucket for {:?}: {}", id, DisplayErrorContext(err));
                    BlobStoreBucketError::Unknown.into()
                }
            },
        }
    }

    async fn flush<F: Flushable>(&self, toflush: F) -> Vec<(PageIdentifier, FlushResult)> {
        // XXX memory reuse; return _toflush
        let journal = toflush.journal(); // XXX check if bucket exists here too? or only on journal creation?
        let bucketname = self.cfg.bucket_name(&journal);
        log::trace!("[S3] Flushing {} in {}", journal, bucketname);
        let results = futures_util::future::join_all(toflush.into_iter().map(|(ident, page)| {
            let ident = ident.clone();
            let bucketname = bucketname.clone();
            let used_bytes = page.used_bytes();
            async move {
                let objname = self.cfg.page_name(&journal, &ident);
                log::trace!("  [S3] Flushing {:?} to {}/{}", ident, bucketname, objname);
                let res = self
                    .client
                    .put_object()
                    .bucket(bucketname)
                    .if_none_match("*")
                    .key(objname)
                    // .body(Bytes::copy_from_slice(page).into()) // XXX avoid clone? It's 20MB! maybe Bytes::from_owner with custom Send wrapper / variant for iobuf
                    .body(zerocopy::io_buffer_to_bytes(page.into_inner(), Some(used_bytes)).into())
                    .send()
                    .await;
                log::trace!("  [S3] Flush {:?} result {:?}", ident, res);
                (ident, res)
            }
        }))
        .await;
        results
            .into_iter()
            .map(|(ident, res)| {
                (
                    ident,
                    match res {
                        Ok(resp) => {
                            log::trace!("Object {:?} flushed successfully ", resp);
                            FlushResult::Success
                        }
                        Err(req_err) => match req_err {
                            SdkError::ServiceError(se) => match se.err() {
                                // TODO parse out error type (file exists, no such bucket, ...) from message
                                PutObjectError::InvalidRequest(ir) => FlushResult::IOError(ir.to_string()),
                                x if x.code_eq("PreconditionFailed") => BlobStoreWriteError::ObjectNameCollision.into(),
                                x => {
                                    log::warn!("ServiceError while flushing: {:?}: {}", ident, DisplayErrorContext(&x));
                                    FlushResult::IOError(x.to_string())
                                }
                            },
                            SdkError::TimeoutError(_) => BlobStoreWriteError::BlobStoreNotReachable.into(),
                            x => {
                                log::warn!("Unknown Error while flushing: {:?}: {}", ident, DisplayErrorContext(&x));
                                FlushResult::IOError(x.to_string())
                            }
                        },
                    },
                )
            })
            .collect()
    }

    async fn fetch_journal_page(
        &self,
        journal: JournalId,
        page: PageIdentifier,
    ) -> Result<impl super::DataStream, JournalPageFetchError> {
        let (bucketname, objname) = (self.cfg.bucket_name(&journal), self.cfg.page_name(&journal, &page));
        let result = self.client.get_object().bucket(bucketname).key(objname).send().await;
        use aws_sdk_s3::operation::get_object::GetObjectError;
        match result {
            Ok(resp) => Ok(resp),
            Err(e) => Err(match e {
                SdkError::TimeoutError(_) => BlobStoreReadError::BlobStoreNotReachable.into(),
                SdkError::ServiceError(service_error) => match service_error.err() {
                    GetObjectError::NoSuchKey(_) => BlobStoreReadError::NoSuchObject.into(),
                    err => {
                        log::error!(
                            "unknown service error while fetching page ({:?}, {:?}): {}",
                            journal,
                            page,
                            DisplayErrorContext(err)
                        );
                        BlobStoreReadError::Unknown.into()
                    }
                },
                err => {
                    log::error!("unknown error while fetching page ({:?}, {:?}): {}", journal, page, DisplayErrorContext(err));
                    BlobStoreReadError::Unknown.into()
                }
            }),
        }
    }
}

trait MatchReturnCode {
    type CodeRef<'a>; //  = &'a CodeType where CodeType: 'a;
    fn code_match<F: FnOnce(Self::CodeRef<'_>) -> bool>(&self, map: F) -> bool;
    fn code_eq(&self, s: Self::CodeRef<'_>) -> bool;
    fn msg_match<F: FnOnce(&str) -> bool>(&self, f: F) -> bool;
}

impl<T: ProvideErrorMetadata> MatchReturnCode for T {
    type CodeRef<'a> = &'a str;
    fn code_match<F: FnOnce(Self::CodeRef<'_>) -> bool>(&self, map: F) -> bool {
        match self.code() {
            Some(st) => map(st),
            None => false,
        }
    }

    fn code_eq(&self, s: &str) -> bool {
        self.code_match(|code| s == code)
    }

    fn msg_match<F: FnOnce(&str) -> bool>(&self, f: F) -> bool {
        match self.message() {
            Some(msg) => f(msg),
            None => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::config::tests::MockConfig;

    fn init() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn create_s3() {
        init();
        let cfg = MockConfig::global();
        let _s3 = futures_lite::future::block_on(S3::from_env(cfg));
    }

    #[cfg(feature = "test-real-s3")]
    mod real_s3 {
        use super::*;
        use crate::id::{Epoch, LSN, PageIdentifier};
        use iobuf::PooledBuffer as _;

        #[derive(Clone)]
        struct MockFlushGroup {
            id: JournalId,
            data: Vec<(Epoch, LSN, &'static str)>,
        }

        impl Flushable for MockFlushGroup {
            fn journal(&self) -> JournalId {
                self.id
            }

            fn iter<'a>(&'a self) -> impl Iterator<Item = (crate::id::PageIdentifier, &'a [u8])> {
                self.data
                    .iter()
                    .map(|(epoch, max_lsn, datastr)| (PageIdentifier(*epoch, *max_lsn), datastr.as_bytes()))
            }

            fn into_iter(mut self) -> impl Iterator<Item = (PageIdentifier, crate::io::IoPoolBuf<iobuf::IoBuffer>)> {
                self.data.into_iter().map(|(epoch, max_lsn, datastr)| {
                    let bufpool = iobuf::local_buffer_pool(64, Default::default());
                    let iobuf = iobuf::IoBuffer::new(bufpool.clone());
                    let s = datastr.as_bytes();
                    iobuf.mut_data()[0..s.len()].copy_from_slice(s);
                    (PageIdentifier(epoch, max_lsn), crate::io::IoPoolBuf::new_slice(iobuf, s.len()))
                })
            }
        }

        fn run_s3_test<F: FnOnce(S3) -> Fut + Send, Fut: Future<Output = ()>>(f: F) {
            init();
            librt::rt()
                .new_executor(Default::default(), || async move {
                    let cfg = MockConfig::global();
                    let s3 = S3::from_env(cfg).await;
                    f(s3).await;
                })
                .expect("Error spawning executor");
        }

        #[test]
        fn create_bucket() {
            run_s3_test(|s3| async move {
                let uuid = JournalId::nil();
                // succeeds the first time
                let result1 = s3.on_create_journal(uuid).await;
                assert_eq!(result1, CreateJournalResponse::Ok);
                // don't delete bucket; it takes 2-3hrs for the name to be available again.
                // if let Err(e) = s3.delete_bucket(uuid).await {
                //     panic!("Deleting bucket for {} failed: {}", uuid, e);
                // }
            })
        }

        #[test]
        #[allow(dead_code)]
        fn create_object() {
            use crate::{blob::DataStream, id::RandomDST as _};
            run_s3_test(|s3| async move {
                // ensure test bucket exists
                let jid = JournalId::nil();
                match s3.on_create_journal(jid).await {
                    CreateJournalResponse::Ok => {}
                    CreateJournalResponse::JournalAlreadyExists => {}
                    x => panic!("Create journal error {:?}", x),
                }
                // create object
                let toflush = MockFlushGroup {
                    id: jid,
                    data: vec![(0, 100, "Hello World"), (1, 50, "The red fox jumped over me!")],
                };
                let res = s3.flush(toflush.clone()).await;
                assert_eq!(res.len(), 2);
                assert_eq!(res[0], (PageIdentifier(0, 100), FlushResult::Success));
                assert_eq!(res[1], (PageIdentifier(1, 50), FlushResult::Success));
                assert_eq!(S3::any_flush_error(res), FlushResult::Success);
                // next flush fails
                let res = s3.flush(toflush.clone()).await;
                assert_eq!(res.len(), 2);
                assert_eq!(res[0], (PageIdentifier(0, 100), BlobStoreWriteError::ObjectNameCollision.into()));
                assert_eq!(res[1], (PageIdentifier(1, 50), BlobStoreWriteError::ObjectNameCollision.into()));

                for (epoch, lsn, content) in toflush.data {
                    let ident = PageIdentifier(epoch, lsn);
                    // check that content matches
                    let body = s3.fetch_journal_page(jid, ident).await.expect("error while fetching object");
                    let data = body.collect_to_vec().await.expect("error collecting data");
                    assert_eq!(String::from_utf8(data).expect("error interpreting data as utf8"), content.to_string());

                    // delete objects again
                    let delres = s3.delete_object(jid, s3.cfg.page_name(&jid, &ident).as_str()).await;
                    assert_eq!(delres, Ok(()));
                }
            })
        }
    }
}

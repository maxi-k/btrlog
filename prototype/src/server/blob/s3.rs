use std::{rc::Rc, thread::JoinHandle, time::Duration};

use aws_config::SdkConfig;
use aws_sdk_s3::{
    Client,
    error::{DisplayErrorContext, ProvideErrorMetadata, SdkError},
    operation::{create_bucket::CreateBucketError, get_object::GetObjectOutput, put_object::PutObjectError},
    primitives::ByteStream,
    types::{BucketLocationConstraint, CreateBucketConfiguration},
};

use crate::{
    io::{buffer::IoBuffer, send_buffer::SendableIOBuffer},
    runtime::{
        self,
        sync::{mpsc, oneshot},
    },
    types::{
        error::{BlobStoreBucketError, BlobStoreReadError, BlobStoreWriteError, JournalPageFetchError},
        id::{JournalId, PageIdentifier},
        message::{CreateJournalResponse, FlushResult},
    },
};

use super::{BlobStore, DataStream, config::BlobConfig};

// we scrapped our custom http client b/c it was causing issues in edge cases; use tokio
// plus the standard s3 client on a separate thread instead, since we ended up isolating
// the blob store access in its separate thread anyway; it caused request latency spikes
struct S3Worker<Cfg> {
    _sdk: SdkConfig,
    cfg: Cfg,
    client: Client,
    bucket_region: BucketLocationConstraint,
    existing_buckets: papaya::HashSet<String>,
}

pub(super) struct S3Client {
    // workers: Vec<std::thread::JoinHandle<()>>
    workers: Vec<BlobWorkSender>,
    default_retries: usize,
    _worker_handle: Vec<JoinHandle<()>>,
}

////////////////////////////////////////////////////////////////////////////////
//  Message

type BlobWorkSender = mpsc::Sender<(BlobRequest, Option<oneshot::Sender<BlobResponse>>)>;
type BlobWorkReceiver = mpsc::Receiver<(BlobRequest, Option<oneshot::Sender<BlobResponse>>)>;

#[derive(Debug)]
enum BlobRequest {
    Flush {
        id: JournalId,
        pages: Vec<(PageIdentifier, bytes::Bytes)>,
        retries: usize,
    },
    OnCreate {
        id: JournalId,
        retries: usize,
    },
    FetchPage {
        journal: JournalId,
        page: PageIdentifier,
    },
    #[allow(dead_code)]
    DeletePage {
        id: JournalId,
        page: PageIdentifier,
        retries: usize,
    },
    #[allow(dead_code)]
    DropBucket {
        id: JournalId,
        retries: usize,
    },
    Stop,
}

#[derive(Debug)]
enum BlobResponse {
    Flush(Vec<(PageIdentifier, FlushResult)>),
    OnCreate(CreateJournalResponse),
    FetchPage(Result<Vec<u8>, JournalPageFetchError>),
    DeletePage(#[allow(dead_code)] Result<(), String>),
    DropBucket(#[allow(dead_code)] Result<(), String>),
}

////////////////////////////////////////////////////////////////////////////////
//  Client

impl S3Client {
    #[inline]
    async fn send_and_wait(&self, id: JournalId, msg: BlobRequest) -> BlobResponse {
        let (snd, rcv) = oneshot::channel();
        let worker = self.pick_worker(id);
        if let Err(_e) = runtime::rt().with_sync_waker(worker.send((msg, Some(snd)))).await {
            panic!("error sending work to s3 worker: {:?}", _e);
        }
        match runtime::rt().with_sync_waker(rcv).await {
            Ok(resp) => resp,
            Err(_e) => panic!("error receiving s3 worker response: {:?}", _e),
        }
    }

    fn pick_worker(&self, id: JournalId) -> &BlobWorkSender {
        &self.workers[Self::murmur128(id) as usize % self.workers.len()]
    }

    #[inline(always)]
    const fn murmur128(id: JournalId) -> u64 {
        let (mut k1, mut k2) = id.as_u64_pair();
        // MurmurHash64A
        const M: u64 = 0xc6a4a7935bd1e995;
        const R: u32 = 47;
        let mut h: u64 = 0x8445d61a4e774912 ^ (8u64.wrapping_mul(M));
        
        // first key
        k1 = k1.wrapping_mul(M);
        k1 ^= k1 >> R;
        k1 = k1.wrapping_mul(M);
        h ^= k1;
        h = h.wrapping_mul(M);
        
        // second key
        k2 = k2.wrapping_mul(M);
        k2 ^= k2 >> R;
        k2 = k2.wrapping_mul(M);
        h ^= k2;
        h = h.wrapping_mul(M);
        
        // end bit
        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        h
    }
}

impl BlobStore for S3Client {
    async fn flush(&self, id: JournalId, pages: Vec<(PageIdentifier, IoBuffer)>) -> Vec<(PageIdentifier, FlushResult)> {
        let sendable = pages
            .into_iter()
            .map(|(ident, buffer)| (ident, SendableIOBuffer::new(buffer).into_bytes()))
            .collect::<Vec<_>>();
        let msg = BlobRequest::Flush {
            id,
            pages: sendable,
            retries: self.default_retries,
        };
        match self.send_and_wait(id, msg).await {
            BlobResponse::Flush(items) => items,
            _o => panic!("unexpected response: {:?}", _o),
        }
    }

    fn flush_in_background(&self, id: JournalId, pages: Vec<(PageIdentifier, IoBuffer)>) {
        let sendable = pages
            .into_iter()
            .map(|(ident, buffer)| (ident, SendableIOBuffer::new(buffer).into_bytes()))
            .collect::<Vec<_>>();
        let msg = BlobRequest::Flush {
            id,
            pages: sendable,
            retries: self.default_retries,
        };
        let worker = self.pick_worker(id);
        // XXX try-send, fast fail?
        if let Err(_e) = worker.blocking_send((msg, None)) {
            panic!("error sending work to s3 worker: {:?}", _e);
        }
    }

    async fn on_create_journal(&self, id: JournalId, retries: Option<usize>) -> CreateJournalResponse {
        let msg = BlobRequest::OnCreate {
            id,
            retries: retries.unwrap_or(self.default_retries),
        };
        match self.send_and_wait(id, msg).await {
            BlobResponse::OnCreate(x) => x,
            _o => panic!("unexpected s3 worker response {:?}", _o),
        }
    }

    async fn fetch_journal_page(&self, journal: JournalId, page: PageIdentifier) -> Result<Vec<u8>, JournalPageFetchError> {
        let msg = BlobRequest::FetchPage { journal, page };
        match self.send_and_wait(journal, msg).await {
            BlobResponse::FetchPage(x) => x,
            _o => panic!("unexpected s3 worker response {:?}", _o),
        }
    }
}

#[cfg(test)]
#[cfg(feature = "test-real-s3")]
impl S3Client {
    /// Delete a bucket for the given journal ID (test-only)
    pub async fn drop_bucket(&self, id: JournalId) -> Result<(), String> {
        let (snd, rcv) = oneshot::channel();
        let msg = BlobRequest::DropBucket {
            id,
            retries: self.default_retries,
        };
        if let Err(_e) = runtime::rt().with_sync_waker(self.worker.send((msg, Some(snd)))).await {
            return Err(format!("error sending work to s3 worker: {:?}", _e));
        }
        match runtime::rt().with_sync_waker(rcv).await {
            Ok(resp) => match resp {
                BlobResponse::DropBucket(result) => result,
                _o => Err(format!("unexpected response: {:?}", _o)),
            },
            Err(_e) => Err(format!("error receiving s3 worker response: {:?}", _e)),
        }
    }

    /// Delete an object (page) for the given journal ID (test-only)
    pub async fn delete_page(&self, id: JournalId, page: PageIdentifier) -> Result<(), String> {
        let (snd, rcv) = oneshot::channel();
        let msg = BlobRequest::DeletePage {
            id,
            page,
            retries: self.default_retries,
        };
        if let Err(_e) = runtime::rt().with_sync_waker(self.worker.send((msg, Some(snd)))).await {
            return Err(format!("error sending work to s3 worker: {:?}", _e));
        }
        match runtime::rt().with_sync_waker(rcv).await {
            Ok(resp) => match resp {
                BlobResponse::DeletePage(result) => result,
                _o => Err(format!("unexpected response: {:?}", _o)),
            },
            Err(_e) => Err(format!("error receiving s3 worker response: {:?}", _e)),
        }
    }
}

impl DataStream<bytes::Bytes> for GetObjectOutput {
    async fn next(&mut self) -> Result<Option<bytes::Bytes>, std::io::Error> {
        self.body.try_next().await.map_err(|e| std::io::Error::other(e))
    }

    fn size_hint(&self) -> (u64, Option<u64>) {
        ByteStream::size_hint(self.body())
    }
}

impl S3Client {
    pub(super) fn new<Cfg: BlobConfig + Send + 'static>(cfg: Cfg) -> Self {
        let worker_count = cfg.workers();
        let (threads, workers): (Vec<_>, Vec<_>) = (0..worker_count).map(|i| {
            spawn_s3_worker(cfg.clone())
        }).unzip();
        Self {
            workers,
            default_retries: 3, // XXX configurable
            _worker_handle: threads,
        }
    }
}

impl Drop for S3Client {
    fn drop(&mut self) {
        for worker in self.workers.iter() {
            worker.try_send((BlobRequest::Stop, None)).expect("error sending stop signal to s3 worker");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Worker

fn spawn_s3_worker<Cfg>(cfg: Cfg) -> (JoinHandle<()>, BlobWorkSender)
where
    Cfg: BlobConfig + Send + 'static,
{
    let (sender, receiver) = mpsc::channel(128); // XXX configurable / unbounded?
    let handle = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let set = tokio::task::LocalSet::new();
        let local = set.run_until({
            let set = &set;
            async move {
                let worker = S3Worker::new(cfg).await;
                worker.run(set, receiver).await;
            }
        });
        rt.block_on(local);
    });
    (handle, sender)
}

impl<Cfg: BlobConfig + 'static> S3Worker<Cfg> {
    async fn new(cfg: Cfg) -> Rc<Self> {
        let sdk = aws_config::from_env();
        #[cfg(not(test))]
        let sdk = sdk.credentials_provider(aws_config::environment::EnvironmentVariableCredentialsProvider::new());
        let region: &'static str = cfg.region().to_owned().leak(); // XXX
        let sdk = sdk
            .region(region)
            // the rt-tokio feature in aws is enabled,
            // so we don't need to set a custom sleep implementation
            .load()
            .await;
        let bucket_region = BucketLocationConstraint::try_parse(cfg.region())
            .expect(format!("Configured region not valid {:?}", cfg.region()).as_str());
        let client = Client::new(&sdk);
        let res = Rc::new(Self {
            _sdk: sdk,
            cfg,
            client,
            bucket_region,
            existing_buckets: papaya::HashSet::new(),
        });
        if res.cfg.has_constant_bucket() {
            // let _ = res.on_create_journal(JournalId::nil()).await;
        }
        res
    }

    async fn run(self: &Rc<Self>, spawner: &tokio::task::LocalSet, mut chan: BlobWorkReceiver) {
        while let Some((request, reply_to)) = chan.recv().await {
            if matches!(request, BlobRequest::Stop) {
                return;
            }
            let this = self.clone();
            spawner.spawn_local(async move {
                let resp = match request {
                    BlobRequest::Flush { id, pages, retries } => this.flush(id, pages, retries).await,
                    BlobRequest::OnCreate { id, retries } => this.on_create(id, retries).await,
                    BlobRequest::FetchPage { journal, page } => this.fetch_page(journal, page).await,
                    BlobRequest::DeletePage { id, page, retries: _ } => this.delete_page(id, page).await,
                    BlobRequest::DropBucket { id, retries: _ } => this.drop_bucket(id).await,
                    BlobRequest::Stop => return,
                };
                if let Some(chan) = reply_to {
                    if let Err(_e) = chan.send(resp) {
                        log::error!("s3 receive channel closed, couldn't send reply {:?}", _e);
                    }
                }
            });
        }
    }

    fn track_bucket(&self, name: String) {
        self.existing_buckets.pin().insert(name);
    }

    fn has_bucket(&self, name: &String) -> bool {
        self.existing_buckets.pin().contains(name)
    }

    async fn flush(&self, journal: JournalId, pages: Vec<(PageIdentifier, bytes::Bytes)>, _retries: usize) -> BlobResponse {
        let bucketname = self.cfg.bucket_name(&journal);
        log::trace!("[S3] Flushing {} in {}", journal, bucketname);
        let raw_results = futures_util::future::join_all(pages.into_iter().map(|(ident, page)| {
            let ident = ident.clone();
            let bucketname = bucketname.clone();
            async move {
                let objname = self.cfg.page_name(&journal, &ident);
                log::trace!("  [S3] Flushing {:?} to {}/{}", ident, bucketname, objname);
                let res = self
                    .client
                    .put_object()
                    .bucket(bucketname)
                    .if_none_match("*")
                    .key(objname)
                    .body(page.into())
                    .send()
                    .await;
                log::info!("  [S3] Flush {:?} result {:?}", ident, res);
                (ident, res)
            }
        }))
        .await;
        let results = raw_results
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
            .collect();
        BlobResponse::Flush(results)
    }

    async fn on_create(&self, journal: JournalId, retries: usize) -> BlobResponse {
        let bkt_name = self.cfg.bucket_name(&journal);
        if self.has_bucket(&bkt_name) {
            return BlobResponse::OnCreate(CreateJournalResponse::Ok);
        }
        let cfg = CreateBucketConfiguration::builder().location_constraint(self.bucket_region.clone()).build();
        log::trace!("[S3] Creating Journal bucket {} in {}", bkt_name, self.bucket_region);
        let raw_result = self.client.create_bucket().create_bucket_configuration(cfg).bucket(&bkt_name).send().await;
        let result = match raw_result {
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
                        if retries > 1 {
                            tokio::time::sleep(Duration::from_micros(500)).await;
                            return Box::pin(self.on_create(journal, retries - 1)).await;
                        } else {
                            BlobStoreBucketError::BucketContention.into()
                        }
                    }
                    x => {
                        log::error!("unknown error while creating bucket for {:?}: {}", journal, DisplayErrorContext(x));
                        BlobStoreBucketError::Unknown.into()
                    }
                },
                err => {
                    log::error!("unknown error while creating bucket for {:?}: {}", journal, DisplayErrorContext(err));
                    BlobStoreBucketError::Unknown.into()
                }
            },
        };
        BlobResponse::OnCreate(result)
    }

    async fn fetch_page(&self, journal: JournalId, page: PageIdentifier) -> BlobResponse {
        let (bucketname, objname) = (self.cfg.bucket_name(&journal), self.cfg.page_name(&journal, &page));
        let raw_result = self.client.get_object().bucket(bucketname).key(objname).send().await;
        use aws_sdk_s3::operation::get_object::GetObjectError;
        let result = match raw_result {
            Ok(resp) => {
                // we can't pass the 'byte stream' back here, because `await`ing
                // on more bytes to arrive from the calling thread means
                // cross-thread runtime futures, which we can't really do; for
                // example, this will internally `sleep()` sometimes, which is
                // `tokio` sleep, not our custom runtime impl sleep.
                resp.collect_to_vec().await.map_err(|_e| {
                    log::trace!("error while reading byte stream from blob store {:?}", _e);
                    BlobStoreReadError::BrokenByteStream.into()
                })
            }
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
        };
        BlobResponse::FetchPage(result)
    }

    async fn drop_bucket(&self, id: JournalId) -> BlobResponse {
        let name = self.cfg.bucket_name(&id);
        let res = match self.client.delete_bucket().bucket(&name).send().await {
            Ok(_x) => {
                self.existing_buckets.pin().remove(&name);
                Ok(())
            }
            Err(e) => Err(e.to_string()),
        };
        BlobResponse::DropBucket(res)
    }

    async fn delete_page(&self, id: JournalId, page: PageIdentifier) -> BlobResponse {
        let objname = self.cfg.page_name(&id, &page);
        let res = match self.client.delete_object().bucket(self.cfg.bucket_name(&id)).key(objname).send().await {
            Ok(_x) => Ok(()),
            Err(e) => Err(e.to_string()),
        };
        BlobResponse::DeletePage(res)
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
    use crate::server::blob::config::tests::MockConfig;

    use super::*;

    fn init() {
        // let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn create_s3() {
        init();
        let _s3 = S3Client::new(MockConfig {});
    }
}

#[cfg(test)]
#[cfg(feature = "test-real-s3")]
mod real_s3 {
    use super::*;
    use crate::{
        io::local_buffer_pool,
        runtime::*,
        server::blob::{any_flush_error, config::tests::MockConfig},
    };

    #[apply(async_test)]
    async fn create_bucket() {
        let s3 = S3Client::new(MockConfig {});
        let uuid = JournalId::nil();
        // succeeds the first time
        let result1 = s3.on_create_journal(uuid, None).await;
        assert_eq!(result1, CreateJournalResponse::Ok);
        // don't delete bucket; it takes 2-3hrs for the name to be available again.
        // if let Err(e) = s3.delete_bucket(uuid).await {
        //     panic!("Deleting bucket for {} failed: {}", uuid, e);
        // }
    }

    fn make_test_iobuf(content: &str) -> IoBuffer {
        let pool = local_buffer_pool(content.len(), Default::default());
        let mut buf = pool.pop();
        buf.data_mut()[0..content.len()].copy_from_slice(content.as_bytes());
        buf.mark_used(content.len());
        buf
    }

    #[apply(async_test)]
    fn create_object() {
        let s3 = S3Client::new(MockConfig {});
        // ensure test bucket exists
        let jid = JournalId::nil();
        match s3.on_create_journal(jid, None).await {
            CreateJournalResponse::Ok => {}
            CreateJournalResponse::JournalAlreadyExists => {}
            x => panic!("Create journal error {:?}", x),
        }
        // create object
        let toflush = vec![
            (PageIdentifier(0, 100), make_test_iobuf("Hello World")),
            (PageIdentifier(1, 50), make_test_iobuf("The red fox jumped over me!")),
        ];
        let res = s3.flush(jid, toflush.clone()).await;
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], (PageIdentifier(0, 100), FlushResult::Success));
        assert_eq!(res[1], (PageIdentifier(1, 50), FlushResult::Success));
        assert_eq!(any_flush_error(res.iter()), FlushResult::Success);
        // next flush fails
        let res = s3.flush(jid, toflush.clone()).await;
        assert_eq!(res.len(), 2);
        assert_eq!(res[0], (PageIdentifier(0, 100), BlobStoreWriteError::ObjectNameCollision.into()));
        assert_eq!(res[1], (PageIdentifier(1, 50), BlobStoreWriteError::ObjectNameCollision.into()));

        for (ident, content) in toflush.iter() {
            // check that content matches
            let data = s3.fetch_journal_page(jid, *ident).await.expect("error while fetching object");
            assert_eq!(
                String::from_utf8(data).expect("error interpreting data as utf8"),
                String::from_utf8_lossy(content.as_slice()).into_owned()
            );

            // delete objects again
            let delres = s3.delete_page(jid, *ident).await;
            assert_eq!(delres, Ok(()));
        }
    }
}

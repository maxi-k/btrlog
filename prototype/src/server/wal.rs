use crate::{config::ServerConfig, runtime::Executor, types::message::JournalMetadata};
pub use location::DiskLocation;
pub use stats::WalStats;

use super::server_runtime::server_exec;

mod blockdevice;
mod buffer;
pub mod location;
mod mock;
mod stats;

type Len = usize;

#[allow(async_fn_in_trait)]
pub trait WAL {
    async fn write(&self, meta: &JournalMetadata, data: &[u8]) -> DiskLocation;
    async fn flush(&self) -> DiskLocation;
    fn take_wal_stats(&self) -> WalStats;
}

#[allow(async_fn_in_trait)]
pub trait SharedWAL: Send + Sync + Clone {
    async fn local(self) -> impl WAL;
}

pub async fn create_instance_local_storage_wal(cfg: &ServerConfig) -> impl SharedWAL {
    static JOURNAL_SSD_COUNT: &str = "JOURNAL_SSD_COUNT";
    let ssd_count: u32 = match std::env::var(JOURNAL_SSD_COUNT) {
        Ok(cstr) => match cstr.parse() {
            Ok(x) => x,
            Err(e) => panic!("error parsing {} as integer: {:?}", JOURNAL_SSD_COUNT, e),
        },
        Err(_) => panic!("need to set {}", JOURNAL_SSD_COUNT),
    };
    let files = (0..ssd_count).map(|ssdid| format!("/blk/ssd-{}", ssdid));
    std::sync::Arc::new(blockdevice::LocalBlockdeviceWAL::new(cfg, files))
}

pub async fn create_tempfile_wal(cfg: &ServerConfig) -> impl SharedWAL {
    let temp = std::env::temp_dir();
    let files = (0..cfg.journal_threads()).map(|i| {
        let temp = &temp;
        let io = server_exec().io().clone();
        async move {
            let temp_path = temp.join(format!("journal_{}_{}.wal", std::process::id(), i));
            let file_path_str = temp_path.to_string_lossy().to_string();
            const DEFAULT_FILE_SIZE: u64 = 100 * 1024 * 1024;
            let fd = io
                .open(temp_path, libc::O_RDWR | libc::O_CREAT, Some(0b110110110))
                .await
                .expect("error opening temp wal file");
            // XXX blocking I/O, but ok for this initialization code for now
            let falloc_result = io.fallocate(fd, 0, 0, DEFAULT_FILE_SIZE).await.expect("error during fallocate");
            assert_eq!(0, falloc_result);
            file_path_str
        }
    });
    let files = crate::runtime::future::join_all(files).await;
    std::sync::Arc::new(blockdevice::LocalBlockdeviceWAL::new(cfg, files))
}

pub async fn create_mock_wal(cfg: &ServerConfig) -> impl SharedWAL {
    log::debug!("create_mock_wal() called with config: {:?}", cfg);
    mock::MockSharedWAL::new(cfg)
}

// better than many switch/cases at many callsites, but not super nice either
#[macro_export]
macro_rules! with_wal {
    ($config:expr, $opvar:ident => $body:expr) => {
        match $config.wal {
            $crate::config::WALConfig::Mock => {
                let $opvar = $crate::server::wal::create_mock_wal(&$config);
                $body
            }
            $crate::config::WALConfig::TempFile => {
                let $opvar = $crate::server::wal::create_tempfile_wal(&$config);
                $body
            }
            $crate::config::WALConfig::InstanceLocalStorage => {
                let $opvar = $crate::server::wal::create_instance_local_storage_wal(&$config);
                $body
            }
        }
    };
}

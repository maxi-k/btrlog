use std::sync::LazyLock;

use clap::{Parser, ValueEnum as _};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////

#[derive(Parser, Debug, Clone, Copy, clap::ValueEnum, Serialize, Deserialize, Default)]
pub enum Platform {
    #[default]
    Local,
    Aws,
}

static PLATFORM_ENV_VAR: &'static str = "JOURNAL_PLATFORM";
static PLATFORM: LazyLock<Platform> = LazyLock::new(|| match std::env::var(PLATFORM_ENV_VAR) {
    Ok(p) => match Platform::from_str(p.as_str(), true) {
        Ok(v) => v,
        Err(e) => panic!("{}", e),
    },
    Err(e) => match e {
        std::env::VarError::NotPresent => Platform::default(),
        std::env::VarError::NotUnicode(d) => panic!("invalid platform: {:?}", d),
    },
});

pub trait PlatformDependent: Sized {
    fn for_platform(platform: Platform) -> Self;
}

pub fn get_platform() -> Platform {
    *PLATFORM
}

pub fn platform_dependent<T: PlatformDependent>() -> T {
    T::for_platform(*PLATFORM)
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Parser, Debug, Clone, Copy, clap::ValueEnum, Serialize, Deserialize)]
pub enum WALConfig {
    TempFile,
    InstanceLocalStorage,
    Mock,
}

impl PlatformDependent for WALConfig {
    fn for_platform(platform: Platform) -> Self {
        match platform {
            Platform::Local => Self::TempFile,
            Platform::Aws => Self::InstanceLocalStorage,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct JournalPageMemoryConfig {
    pub(crate) virtual_memory_size: usize,
    pub(crate) preallocated_page_count: usize,
}
impl PlatformDependent for JournalPageMemoryConfig {
    fn for_platform(platform: Platform) -> Self {
        match platform {
            Platform::Local => Self {
                virtual_memory_size: 8 * 1024 * 1024 * 1024,
                preallocated_page_count: 16,
            },
            Platform::Aws => Self {
                virtual_memory_size: 32 * 1024 * 1024 * 1024,
                preallocated_page_count: 96,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Parser, Debug, Clone, Copy, clap::ValueEnum, Serialize, Deserialize)]
pub enum ThreadPlacement {
    Pinned,
    Unpinned,
}

pub(crate) fn get_hyperthread_for_core(core: usize) -> usize {
    thread_local! {
        static TOPO: std::cell::LazyCell<(usize, usize)> = std::cell::LazyCell::new(|| {
            (num_cpus::get_physical(), num_cpus::get())
        })
    }
    // assumes logical cpu numbers are distributed as in [hw cores...] [hyperthreads...]
    // alternative is [(c0, c0ht), (c1, c1ht), ...], but i've never seen this
    // XXX better way to do this
    TOPO.with(|cell| {
        let (physical, logical) = **cell;
        // we're in a 'normal' hyperthreading topo
        if logical == 2 * physical { core + physical } else { core }
    })
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Parser, Debug, Clone, Copy, clap::ValueEnum, Serialize, Deserialize)]
pub enum BlobStoreImplementation {
    Mock,
    S3,
}

impl PlatformDependent for BlobStoreImplementation {
    fn for_platform(platform: Platform) -> Self {
        match platform {
            Platform::Local => Self::Mock,
            Platform::Aws => Self::S3,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Journal Service IO")]
pub struct IOConfig {
    /// [io] I/O ring depth
    #[arg(long, env, default_value_t = 512)]
    pub io_depth: usize,

    /// [io] Uring IO Mode
    #[clap(long, value_enum, default_value_t = IOMode::ManualPoll)]
    pub io_mode: IOMode,

    /// [io] deprecated; how little progress per loop will result in a blocking uring_enter syscall
    /// 0 means to never wait
    #[arg(long, env, default_value_t = 0)]
    pub io_wait_threshold: usize,

    /// [io] how many outstanding push I/Os is the client allowed to have?
    /// conversely, how many LSNs does the server need to keep around before flushing?
    #[arg(long, env, default_value_t = 128)]
    pub lsn_window: usize,
}

impl IOConfig {
    pub fn from_cli_args() -> IOConfig {
        IOConfig::parse()
    }

    pub fn from_env() -> IOConfig {
        // parse from an empty iterator, which will apply the fallback
        // (= read from the environment) for every required argument
        let empty_args: &[&'static str] = &[];
        IOConfig::parse_from(empty_args)
    }
}

////////////////////////////////////////////////////////////////////////////////
#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Journal Service Server")]
pub struct ServerConfig {
    /// [server] where to read the cluster configuration from
    #[arg(long, env, value_enum, default_value_t = Platform::default())]
    pub journal_platform: Platform,

    /// [server] journal thread count. 0: use cores-network_threads (N=cores)
    #[arg(long, env, default_value = None)]
    journal_threads: Option<usize>,

    /// [server] network (http) thread count.
    #[arg(long, env, default_value_t = 0)]
    network_threads: usize,

    /// [server] Channel Depth
    #[arg(long, env, default_value_t = 128)]
    pub chan_depth: usize,

    /// [server] how to place threads on cpus
    #[arg(long, env, value_enum, default_value_t = ThreadPlacement::Unpinned)]
    pub thread_placement: ThreadPlacement,

    /// [server] the default (thread 0) networking port
    #[arg(long, env, default_value_t = 3000)]
    pub port: u16,

    /// [server] whether to use the mock blob store or a real blob store
    #[arg(long, env, value_enum, default_value_t = platform_dependent())]
    pub blob_store_impl: BlobStoreImplementation,

    #[arg(long, env, default_value_t = 4)]
    pub blob_store_workers: usize,

    /// [server] where to write the WAL to?
    #[arg(long, env, value_enum, default_value_t = platform_dependent())]
    pub wal: WALConfig,

    #[arg(long, env, default_value_t = 30)]
    pub wal_flush_interval_us: u64,

    #[arg(long, env, default_value_t = 3000)]
    pub wal_flush_min_bytes: u64,

    /// [server] AWS region
    #[arg(long, env, default_value = "eu-central-1")]
    pub aws_region: String,

    /// [server] S3 bucket name pattern
    #[arg(long, env, default_value = "journal-service-test-{region}")]
    pub bucket_name_pattern: String,

    /// [server] S3 object name pattern
    #[arg(long, env, default_value = "log-{logid}-page-{epoch}-{max_tid}")]
    pub object_prefix_pattern: String,

    /// [server] pritn event loop stats
    #[arg(long, env, default_value_t = false)]
    pub print_event_loop_stats: bool,
}

impl ServerConfig {
    pub fn from_cli_args() -> ServerConfig {
        ServerConfig::parse()
    }

    pub fn leak_from_cli_args() -> &'static ServerConfig {
        Box::leak(Box::new(Self::from_cli_args()))
    }

    pub fn journal_threads(&self) -> usize {
        self.journal_threads.unwrap_or_else(|| match get_platform() {
            Platform::Local => 1,
            Platform::Aws => num_cpus::get_physical(),
        })
    }

    pub fn journal_threads_opt(&self) -> &Option<usize> {
        &self.journal_threads
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Parser, Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
pub enum IOMode {
    ManualPoll,
    Coop,
    AutoPoll,
    AutoPollPinned,
}

////////////////////////////////////////////////////////////////////////////////
//  Client

#[derive(Parser, Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum, Serialize, Deserialize)]
pub enum ReplMode {
    None,
    KeepOpen,
    Close,
}

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Journal Service Client")]
pub struct ClientConfig {
    /// [client] the default (thread 0) networking port on servers
    #[arg(long, env, default_value_t = 3000)]
    pub server_port: u16,

    /// [client] Per-message retries
    #[clap(long, env, default_value_t = 10)]
    pub per_message_retries: usize,

    /// [client] Per-message timeout (send and receive) for regular messages
    #[clap(long, env, default_value_t = 30)]
    pub per_message_timeout_ms: u64,

    /// [client] Per-message required majoriy; set to the cluster majority (n/2+1) by default
    #[clap(long, env, default_value_t = 0)]
    pub required_majority: usize,

    /// [client] Per-message retries
    #[clap(long, env, default_value_t = 100)]
    pub create_journal_message_retries: usize,

    /// [client] Per-message timeout (send and receive) for create-journal messages
    #[clap(long, env, default_value_t = 10)]
    pub create_journal_timeout_ms: u64,

    /// [client] Cluster start- and stop timeout - how long to wait after shutdown until force-stopping
    #[clap(long, env, default_value_t = 100)]
    pub cluster_state_timeout_ms: u64,

    /// [client] Per-message retries
    #[clap(long, env, default_value_t = 1.5)]
    pub retry_backoff_factor: f64,

    /// [client] Internal timeout for recv calls before warning that no message was received on sock
    #[clap(long, env, default_value_t = 1000)]
    pub internal_receive_op_timeout: u64,

    /// [client] Start a REPL on the primary node for interactive debugging
    #[clap(long, env, value_enum, default_value_t = ReplMode::None)]
    pub repl: ReplMode,
}

impl ClientConfig {
    pub fn from_cli_args() -> ClientConfig {
        ClientConfig::parse()
    }

    pub fn from_env() -> ClientConfig {
        // parse from an empty iterator, which will apply the fallback
        // (= read from the environment) for every required argument
        let empty_args: &[&'static str] = &[];
        ClientConfig::parse_from(empty_args)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Thread Configs

pub(crate) struct ServerThreadConfig {
    pub server: &'static ServerConfig,
    pub io: &'static IOConfig,
    pub tid: usize,
}

pub struct ClientThreadConfig {
    pub client: &'static ClientConfig,
    pub io: &'static IOConfig,
    pub tid: usize,
}

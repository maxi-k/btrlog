use std::net::{IpAddr, SocketAddr, ToSocketAddrs as _};

use crate::config::PlatformDependent;

pub(super) static CLUSTER_SIZE_VAR: &str = "JOURNAL_CLUSTER_SIZE";
pub(super) static LOGGER_COUNT_VAR: &str = "JOURNAL_LOGGER_COUNT";
pub(super) static CLUSTER_SELF_ID: &str = "JOURNAL_NODE_ID";
pub(super) static CLUSTER_HEARTBEAT_TIMEOUT: &str = "JOURNAL_CLUSTER_HEARTBEAT_TIMEOUT";
pub(super) static CLUSTER_HEARTBEAT_INTERVAL: &str = "JOURNAL_CLUSTER_HEARTBEAT_INTERVAL";
pub(super) static NODE_PORT: &str = "JOURNAL_CLUSTER_PORT";
pub(super) static NODE_IP_PREFIX: &str = "JOURNAL_NODE_IP_";
pub(super) static NODE_NAME_PREFIX: &str = "JOURNAL_NODE_NAME_";
pub(super) static NODE_PORT_PREFIX: &str = "JOURNAL_NODE_PORT_";

pub(super) static DEFAULT_NODE_PORT: u16 = 1984;
pub(super) static DEFAULT_HEARTBEAT_TIMEOUT: usize = 5 * 1000;
pub(super) static DEFAULT_HEARTBEAT_INTERVAL: usize = 1 * 1000;

pub fn node_id_from_env() -> Result<(usize, bool), anyhow::Error> {
    use std::env;
    let self_id: usize = env::var(CLUSTER_SELF_ID)?.parse()?;
    let cluster_size: usize = env::var(CLUSTER_SIZE_VAR)?.parse()?;
    Ok((self_id, self_id == cluster_size - 1))
}

////////////////////////////////////////////////////////////////////////////////
//

#[derive(Debug)]
pub struct ClusterInfo {
    pub cluster_size: usize,
    pub logger_count: usize,
    pub other_nodes_send: Vec<SocketAddr>,
    pub other_nodes_recv: Vec<SocketAddr>,
    pub heartbeat_timeout: std::time::Duration,
    pub heartbeat_interval: std::time::Duration,
    pub my_id: usize,
    pub my_ip: IpAddr,
    pub heartbeat_port: u16,
}

impl PlatformDependent for ClusterInfo {
    fn for_platform(_platform: crate::config::Platform) -> Self {
        Self::from_env().expect("Failed to read cluster info from env")
    }
}

impl ClusterInfo {
    pub(super) fn from_env() -> Result<Self, anyhow::Error> {
        use std::env::{self, VarError};
        use std::str::FromStr as _;
        use std::time::Duration;

        fn getenv_with_default<T: std::str::FromStr, K: AsRef<std::ffi::OsStr>>(
            varname: K,
            default: T,
        ) -> Result<T, anyhow::Error>
        where
            T::Err: std::error::Error + Send + Sync + 'static,
        {
            let val = match env::var(varname) {
                Ok(v) => v,
                Err(VarError::NotUnicode(_)) => return Err(anyhow::anyhow!("Invalid environment variable")),
                Err(VarError::NotPresent) => return Ok(default),
            };
            let parsed = val.parse()?;
            Ok(parsed)
        }
        fn getenv<K: AsRef<std::ffi::OsStr>>(varname: K) -> Result<String, std::env::VarError> {
            env::var(varname)
        }
        let cluster_size: usize = getenv(CLUSTER_SIZE_VAR)?.parse()?;
        let logger_count: usize = getenv_with_default(LOGGER_COUNT_VAR, cluster_size - 1)?;
        let self_id: usize = getenv(CLUSTER_SELF_ID)?.parse()?;
        let self_ip = IpAddr::from_str(getenv(format!("{}{}", NODE_IP_PREFIX, self_id))?.as_str())?;
        let my_port: u16 = getenv_with_default(NODE_PORT, DEFAULT_NODE_PORT)?;

        let mut send_nodes = Vec::with_capacity(logger_count - 1);
        let mut recv_nodes = Vec::with_capacity(logger_count - 1);
        // logger nodes always use ids 0-N, non-logger nodes use higher ids
        for node in 0..logger_count {
            if node == self_id {
                continue;
            }
            let node_addr = match getenv(format!("{}{}", NODE_NAME_PREFIX, node)) {
                // if name var is set, use it
                Err(std::env::VarError::NotPresent) => getenv(format!("{}{}", NODE_IP_PREFIX, node)), // otherwise, use node ip
                x => x,
            }?;
            let node_port = getenv_with_default(format!("{}{}", NODE_PORT_PREFIX, node), DEFAULT_NODE_PORT)?;
            recv_nodes.push(
                (node_addr.as_str(), node_port)
                    .to_socket_addrs()
                    .and_then(|mut it| it.next().ok_or_else(|| std::io::Error::other("Name Lookup failed")))?,
            );
            send_nodes.push(
                (node_addr.as_str(), node_port)
                    .to_socket_addrs()
                    .and_then(|mut it| it.next().ok_or_else(|| std::io::Error::other("Name Lookup failed")))?,
            );
        }

        let timeout = getenv_with_default(CLUSTER_HEARTBEAT_TIMEOUT, DEFAULT_HEARTBEAT_TIMEOUT)?;
        let interval = getenv_with_default(CLUSTER_HEARTBEAT_INTERVAL, DEFAULT_HEARTBEAT_INTERVAL)?;

        Ok(Self {
            cluster_size,
            logger_count,
            other_nodes_send: send_nodes,
            other_nodes_recv: recv_nodes,
            heartbeat_timeout: Duration::from_millis(timeout as u64),
            heartbeat_interval: Duration::from_millis(interval as u64),
            my_id: self_id,
            my_ip: self_ip,
            heartbeat_port: my_port,
        })
    }

    pub fn am_primary(&self) -> bool {
        self.my_id >= self.logger_count
    }

    pub fn am_logger(&self) -> bool {
        self.my_id < self.logger_count
    }

    pub fn heartbeat_addr(&self) -> SocketAddr {
        SocketAddr::from((self.my_ip, self.heartbeat_port))
    }
}

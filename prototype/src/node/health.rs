// XXX reimplement this without use of Notify/Await/Sleep
// - check cancellation for uring
// - uring chained timeouts
use std::{
    cell::Cell,
    net::{IpAddr, SocketAddr, UdpSocket},
    os::fd::{AsRawFd, RawFd},
    rc::Rc,
    time::{Duration, Instant, SystemTime},
};

use more_asserts::debug_assert_le;

use crate::{
    config::ServerConfig,
    io::local_buffer_pool,
    runtime,
    types::{
        message::{HeartbeatMessage, NodeState},
        wire::{BoundedSizeWireMessage as _, WireMessage as _},
    },
};
use crate::{
    io::{ThreadUring, buffer::IoBuffer},
    runtime::{Executor, sync::Notify},
};

use super::cluster::ClusterInfo;

#[derive(Clone, Copy, Debug)]
pub struct NodeHealthData {
    pub time: Instant,
    pub alive_nodes: usize, // nodes that other node thinks are alive
    pub state: NodeState,
}
impl NodeHealthData {
    pub fn initial(initial_time: Instant) -> Self {
        Self {
            time: initial_time,
            alive_nodes: 0,
            state: NodeState::Init,
        }
    }

    pub fn alive(&self, now: Instant, timeout: Duration) -> bool {
        now.duration_since(self.time) <= timeout
    }

    pub fn unreachable(&self, now: Instant, timeout: Duration) -> bool {
        now.duration_since(self.time) > timeout
    }

    pub fn connected(&self, csize: usize) -> bool {
        self.alive_nodes == csize
    }
}

impl From<HeartbeatMessage> for NodeHealthData {
    fn from(value: HeartbeatMessage) -> Self {
        Self {
            time: Instant::now(),
            alive_nodes: value.alive_nodes,
            state: value.state,
        }
    }
}

#[derive(Debug)]
pub struct ClusterHealth {
    info: &'static ClusterInfo,
    last_heartbeat: Vec<Cell<NodeHealthData>>,
    primary: Cell<(NodeHealthData, Option<SocketAddr>)>,
    send_heartbeat_chan: Notify,
    recv_heartbeat_chan: Notify,
    timeout_chan: Notify,
    my_state: Cell<NodeState>,
}

impl Drop for ClusterHealth {
    fn drop(&mut self) {
        // send a heartbeat when dropping
        self.send_heartbeat_chan.notify_one();
    }
}

impl ClusterHealth {
    pub fn cluster_info(&self) -> &ClusterInfo {
        self.info
    }

    pub fn id(&self) -> usize {
        self.info.my_id
    }

    pub fn ip(&self) -> IpAddr {
        self.info.my_ip
    }

    pub fn other_logger_count(&self) -> usize {
        self.info.other_nodes_send.len()
    }

    pub fn other_logger_ips(&self) -> impl Iterator<Item = IpAddr> {
        self.info.other_nodes_send.iter().map(|addr| addr.ip())
    }

    pub fn heartbeat_receiver_count(&self) -> usize {
        self.info.other_nodes_recv.len()
    }

    pub fn node_alive_timeout(&self) -> Duration {
        self.info.heartbeat_timeout
    }

    #[inline]
    pub fn nodes_matching<F: Fn(&NodeHealthData, Instant, usize) -> bool>(&self, f: F) -> usize {
        let now = Instant::now();
        let csize = self.info.logger_count;
        self.last_heartbeat.iter().fold(0, |acc, c| acc + (f(&c.get(), now, csize) as usize))
    }

    pub fn alive_other_node_count(&self) -> usize {
        self.nodes_matching(|n, now, _| n.alive(now, self.info.heartbeat_timeout))
    }

    pub fn connected_node_count(&self) -> usize {
        self.nodes_matching(|n, now, csize| n.connected(csize) && n.alive(now, self.info.heartbeat_timeout))
    }

    pub fn nodes_in_state(&self, s: NodeState) -> usize {
        self.nodes_matching(|n, _, _| n.state == s)
    }

    pub fn has_initialized(&self) -> bool {
        self.connected_node_count() == self.other_logger_count()
    }

    pub fn set_state(&self, state: NodeState) -> NodeState {
        self.my_state.replace(state)
    }

    pub fn current_state(&self) -> NodeState {
        self.my_state.get()
    }

    pub async fn await_state(&self, state: NodeState) {
        loop {
            if self.my_state.get() == state {
                break;
            }
            let notifier = self.recv_heartbeat_chan.notified();
            notifier.await;
        }
    }

    pub async fn await_cluster_state<F: Fn(&NodeHealthData, Instant, usize) -> bool>(&self, min: usize, f: F) {
        loop {
            let matches = self.nodes_matching(&f);
            if matches >= min {
                log::trace!("[await_cluster_state][{}] {} >= {} nodes match, done!", self.info.my_id, matches, min);
                break;
            }
            log::trace!(
                "[await_cluster_state][{}] only {}/{} nodes matching, continuing to wait {:?}",
                self.info.my_id,
                matches,
                min,
                self.last_heartbeat
            );
            runtime::select! {
                _ = self.recv_heartbeat_chan.notified() => {},
                _ = self.timeout_chan.notified() => {}
            };
        }
    }

    pub async fn await_all_state<F: Fn(&NodeHealthData, Instant, usize) -> bool>(&self, f: F) {
        self.await_cluster_state(self.other_logger_count(), f).await
    }

    pub async fn await_all_ready(&self) {
        self.await_all_state(|n, _time, _size| n.state == NodeState::Ready).await
    }

    #[must_use]
    pub async fn await_cluster_stopped(&self, timeout: Option<Duration>) -> bool {
        let io = runtime::rt().io().clone();
        let now = Instant::now();
        let fut = self
            .await_all_state(|n, _time, _size| n.state == NodeState::Stopping || n.unreachable(now, self.node_alive_timeout()));
        if let Some(timeout) = timeout {
            let timer = io.sleep(timeout);
            runtime::select! {
                _ = fut => return true,
                _ = timer => return false
            }
        } else {
            fut.await;
            return true;
        }
    }

    async fn send_heartbeat_to(&self, io: &ThreadUring, addr: SocketAddr, send_fd: RawFd, sndbuf: IoBuffer) -> IoBuffer {
        let (res, mut retbuf) = io.send_to(send_fd, addr, sndbuf).await;
        assert!(res.expect("error sending heartbeats") == retbuf.used_bytes());
        retbuf.mark_all_used();
        retbuf
    }

    async fn send_heartbeats(&self, send_sock: Rc<UdpSocket>, as_primary: bool) -> Result<(), anyhow::Error> {
        let io = runtime::rt().io().clone();
        let send_fd = send_sock.as_raw_fd();
        let mut sndbuf = local_buffer_pool(HeartbeatMessage::WIRE_SIZE_BOUND, Default::default()).pop();
        loop {
            log::trace!("sending heartbeats...");
            let known_healthy_nodes = self.alive_other_node_count();
            let msg = HeartbeatMessage {
                node_id: self.id(),
                alive_nodes: known_healthy_nodes + 1, // the others plus us
                state: self.my_state.get(),
                _padding: Default::default(),
            };
            log::trace!("{} --> {:?}", self.info.my_id, msg);
            let msglen = msg.clone().encode_into(sndbuf.as_mut_slice())?;
            sndbuf.mark_used(msglen);
            debug_assert_le!(msglen, HeartbeatMessage::WIRE_SIZE_BOUND);
            for addr in self.info.other_nodes_recv.iter() {
                sndbuf = self.send_heartbeat_to(&io, *addr, send_fd, sndbuf).await;
            }
            if let Some(addr) = self.primary.get().1 {
                log::debug!("sending heartbeat to primary node at {:?}", addr);
                sndbuf = self.send_heartbeat_to(&io, addr, send_fd, sndbuf).await;
            }
            let sleep_time = if known_healthy_nodes == self.other_logger_count() {
                self.info.heartbeat_interval
            } else {
                self.info.heartbeat_interval / 10
            };
            if as_primary {
                if known_healthy_nodes == self.other_logger_count() {
                    // primary only sends heartbeats when requested when cluster is alive
                    self.send_heartbeat_chan.notified().await;
                    log::trace!("primary: sending heartbeats due to chan notify");
                } else {
                    // except if cluster is initializing - send heartbeats
                    // regularly to register ourselves with the logger nodes
                    // until we receive heartbeats in turn
                    let _ = io.sleep(sleep_time).await;
                    log::trace!("primary: sending heartbeats due to initialization phase");
                }
            } else {
                // logger nodes send heartbeats regularly
                runtime::select! {
                    _ = io.sleep(sleep_time) => {},
                    _ = self.send_heartbeat_chan.notified() => {
                        log::trace!("{}: sending early heartbeats due to chan notify {:?}", self.info.my_id, self.my_state.get());
                    }
                }
            }
        }
    }

    pub fn trigger_heartbeat_send(&self) {
        self.send_heartbeat_chan.notify_one();
    }

    async fn receive_heartbeats(&self, recv_sock: Rc<UdpSocket>) -> Result<(), anyhow::Error> {
        let io = runtime::rt().io().clone();
        let recv_fd = recv_sock.as_raw_fd();
        let mut buf = local_buffer_pool(1024, Default::default()).pop();
        loop {
            // XXX timeout with select! or futures_lite::or
            log::trace!("receiving heartbeats on...");
            let (res, retbuf) = io.recv_from(recv_fd, buf, None).await;
            buf = retbuf;
            let (recv_bytes, from) = match res {
                Ok(package) => package,
                // TODO
                // Err(ServerError::Timeout) => {
                //     log::warn!("timeout while trying to receive heartbeats");
                //     self.timeout_chan.notify_waiters();
                //     continue;
                // }
                Err(e) => {
                    log::error!("error during heartbeat receive {}", e);
                    continue;
                }
            };
            debug_assert_le!(recv_bytes, HeartbeatMessage::WIRE_SIZE_BOUND);
            let origin = self.info.other_nodes_send.iter().enumerate().find(|(_idx, addr)| **addr == from); // XXX send node id with heartbeat?
            let msg = match HeartbeatMessage::decode_from(&buf.as_slice()[0..recv_bytes]) {
                Ok((msg, _bytes)) => msg,
                Err(e) => {
                    log::warn!("error parsing heartbeat message {:?}", e);
                    continue;
                }
            };
            match origin {
                Some((idx, _)) => {
                    // log::trace!("putting heartbeat {:?} from {} to {} into {} of {}", msg, from, self.my_ip, idx, self.last_heartbeat.len());
                    self.last_heartbeat[idx].replace(NodeHealthData::from(msg));
                    self.recv_heartbeat_chan.notify_waiters();
                    log::trace!("{:?} <-- Heartbeat from {} (idx {})", self.info.my_id, msg.node_id, idx);
                }
                None => {
                    if msg.node_id == self.info.logger_count {
                        log::trace!("received heartbeat from primary {:?}, setting info data", from);
                        self.primary.replace((NodeHealthData::from(msg), Some(from)));
                    } else {
                        log::warn!("received heartbeat from unknown node {:?}", from);
                    }
                }
            }
        }
    }

    pub fn new(info: &'static ClusterInfo) -> Self {
        let very_old_time =
            Instant::now() - (SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).expect("running before 1970"));
        Self {
            info,
            last_heartbeat: vec![Cell::new(NodeHealthData::initial(very_old_time)); info.other_nodes_send.len()],
            primary: Cell::new((NodeHealthData::initial(very_old_time), None)),
            send_heartbeat_chan: Notify::const_new(),
            recv_heartbeat_chan: Notify::const_new(),
            timeout_chan: Notify::const_new(),
            my_state: Cell::new(NodeState::Init),
        }
    }

    pub fn spawn_heartbeat_tasks(&'static self, sock: Rc<UdpSocket>, exec: &dyn Executor, as_primary: bool) {
        let sock2 = sock.clone();
        exec.spawn_generic(Box::pin(async move {
            let _ = self.receive_heartbeats(sock).await;
        }));
        exec.spawn_generic(Box::pin(async move {
            let _ = self.send_heartbeats(sock2, as_primary).await;
        }));
    }

    pub async fn initialize(self: &'static ClusterHealth, as_primary: bool) -> Result<(), anyhow::Error> {
        let rt = runtime::rt();
        let sock = Rc::new(rt.io().udp_bind(self.info.heartbeat_addr())?);
        self.spawn_heartbeat_tasks(sock, &*rt, as_primary);
        // wait for cluster to be ready
        loop {
            let mut chan = std::pin::pin!(self.recv_heartbeat_chan.notified());
            let mut timer = std::pin::pin!(rt.sleep(self.info.heartbeat_timeout * 3));
            runtime::select! {
                _ = &mut timer => return Err(anyhow::anyhow!("cluster init timeout")),
                _ = &mut chan => {
                    if self.has_initialized() {
                        log::trace!("[{}] all other nodes initialized, setting cluster state to 'alive'", self.info.my_id);
                        self.set_state(NodeState::Alive);
                        return Ok(());
                    } else {
                        log::trace!("[{}] cluster not ready yet, continuing to wait: {:?}", self.info.my_id, self.last_heartbeat);
                    }
                }
            }
        }
    }

    pub async fn join_cluster_as_client() -> Result<&'static ClusterHealth, anyhow::Error> {
        let info = Box::leak(Box::new(ClusterInfo::from_env()?));
        let cluster = Box::leak(Box::new(ClusterHealth::new(info)));
        cluster.initialize(true).await?;
        Ok(cluster)
    }

    pub async fn init_cluster(_cfg: &ServerConfig) -> Result<&'static ClusterHealth, anyhow::Error> {
        let info = Box::leak(Box::new(ClusterInfo::from_env()?));
        let cluster = Box::leak(Box::new(ClusterHealth::new(info)));
        cluster.initialize(false).await?;
        Ok(cluster)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::node::cluster::*;

    fn init_env_vars_id(id: usize, size: usize) {
        use std::env;
        unsafe {
            env::set_var(CLUSTER_SIZE_VAR, format!("{}", size));
            env::set_var(CLUSTER_SELF_ID, format!("{}", id));
            for i in 0..size {
                env::set_var(format!("{}{}", NODE_IP_PREFIX, i), test_node_ip_for(i));
                // env::set_var(format!("{}{}", NODE_SEND_PORT_PREFIX, other), if am_child { "1982" } else { "1983"});
                // env::set_var(format!("{}{}", NODE_RECV_PORT_PREFIX, other), if am_child { "1984" } else { "1985"});
            }
        }
    }

    fn test_node_ip_for(id: usize) -> String {
        static PREFIX: &str = "127.0.1.";
        format!("{}{}", PREFIX, id + 1)
    }

    #[test]
    #[ignore]
    fn test_from_env_vars() {
        // don't try to init env vars from parent and child simultaneously
        // https://stackoverflow.com/questions/61976745/why-does-rust-rwlock-behave-unexpectedly-with-fork
        init_env_vars_id(0, 2);
        let pid = unsafe { libc::fork() };
        let id = if pid == 0 { 1 } else { 0 };
        if pid == 0 {
            init_env_vars_id(id, 2);
        }
        let cluster_info = Box::leak(Box::new(ClusterInfo::from_env().expect("Error creating cluster")));
        let cluster = ClusterHealth::new(cluster_info);
        let test_vars = || {
            assert_eq!(cluster.info.my_id, id);
            assert_eq!(cluster.info.heartbeat_port, DEFAULT_NODE_PORT);
            if pid == 0 {
                assert_eq!(cluster.info.other_nodes_recv[0].to_string(), "127.0.1.1:1984");
            } else {
                assert_eq!(cluster.info.other_nodes_recv[0].to_string(), "127.0.1.2:1984");
            }
        };
        if pid == 0 {
            match std::panic::catch_unwind(test_vars) {
                Ok(_) => std::process::exit(0),
                Err(e) => {
                    eprintln!("error in child {:?}", e);
                    std::process::exit(1)
                }
            }
        } else {
            test_vars();
            let mut stat: i32 = 0;
            let _ret = unsafe { libc::waitpid(pid, &mut stat, 0) };
            assert!(libc::WIFEXITED(stat));
            assert_eq!(0, libc::WEXITSTATUS(stat));
        }
    }

    /*
        #[test]
        #[ignore]
        fn test_cluster_init() {
        // env_logger::init_from_env("RUST_LOG");
        let nlog = 3;
        let cfg = ServerConfig::leak_from_cli_args();
        unsafe {
        // make test run faster
        std::env::set_var(CLUSTER_HEARTBEAT_INTERVAL, format!("{}", 10));
        std::env::set_var(CLUSTER_HEARTBEAT_TIMEOUT, format!("{}", 1000));
    }
        let test_heartbeats = || {
        runtime::test_rt().with_executor(Default::default(), || async move {
        let res = ClusterHealth::init_cluster(cfg).await;
        let now = std::time::Instant::now();
        match res {
        Ok(res) => {
        for cell in res.last_heartbeat.iter() {
        let ts = cell.get();
        assert!(now.duration_since(ts.time) < res.info.heartbeat_timeout);
        assert!(ts.alive_nodes == nlog);
    }
        assert_eq!(res.current_state(), NodeState::Alive);
        log::info!("node {} is done initializing; waiting for all nodes to have alive state", res.info.my_id);
        res.await_all_state(|n, _now, _csize| n.state == NodeState::Alive).await;
        log::info!("node {} is done creating; initiating shutdown", res.info.my_id);
        res.set_state(NodeState::Stopping);
        res.await_all_state(|n, now, _csize| {
        n.state == NodeState::Stopping || !n.alive(now, res.info.heartbeat_timeout)
    })
        .await;
        let now = std::time::Instant::now();
        for cell in res.last_heartbeat.iter() {
        let ts = cell.get();
        assert!(now.duration_since(ts.time) > res.info.heartbeat_timeout || ts.state == NodeState::Stopping);
    }
        log::info!("node {} SHUTTING DOWN", res.info.my_id);
        // librt::rt().sleep(res.heartbeat_timeout*20).await;
    }
        Err(e) => panic!("error creating cluster: {}", e),
    }
    })
    };
        let my_role = run_cluster(nlog, |role| {
        init_env_vars_role(role, nlog + 1);
        test_heartbeats().expect("error in librt executor");
    });
        init_env_vars_role(&my_role, nlog + 1);
        test_heartbeats().expect("error in librt executor");
    }*/
}

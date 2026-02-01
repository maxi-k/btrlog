// simplified version of the glommio mesh features.
// for now, uses N * MPSC queues instead of N * N * SPSC like glommio
// TODO benchmark comparison
pub use super::sync::{
    Barrier, OnceCell,
    mpsc::{self as mailbox, error::SendError, error::TryRecvError, error::TrySendError},
};
use super::{ExecutorConfig, ThreadRuntime};
use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////
//  Mesh configuration

#[derive(Debug, Clone)]
pub struct MeshConfig {
    pub channel_buffer: usize,
    pub executor_config: ExecutorConfig,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            channel_buffer: 128,
            executor_config: Default::default(),
        }
    }
}

impl MeshConfig {
    crate::util::integrated_builder!();
}

#[derive(Debug, thiserror::Error)]
pub enum MeshError {
    SpawnError,
    JoinError(Box<dyn std::any::Any>),
    ExecutionError,
}

impl std::fmt::Display for MeshError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeshError::SpawnError => f.write_str("SpawnError"),
            MeshError::JoinError(_any) => f.write_str("JoinError"),
            MeshError::ExecutionError => f.write_str("ExecutionError"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  mesh senders configuration

pub struct Senders<Msg>(Vec<(mailbox::Sender<Msg>, usize)>);
impl<Msg> From<Vec<(mailbox::Sender<Msg>, usize)>> for Senders<Msg> {
    fn from(value: Vec<(mailbox::Sender<Msg>, usize)>) -> Self {
        Self(value)
    }
}

impl<Msg: Send> Senders<Msg> {
    pub fn iter(&self) -> impl Iterator<Item = &(mailbox::Sender<Msg>, usize)> {
        self.0.iter()
    }

    pub fn broadcast_iter(
        &self,
        myid: usize,
        msg: Msg,
    ) -> impl Iterator<Item = impl Future<Output = Result<usize, mailbox::error::SendError<Msg>>>>
    where
        Msg: Clone,
    {
        self.0.iter().enumerate().filter_map(move |(id, sender)| {
            if id == myid {
                None
            } else {
                let msg = msg.clone();
                Some(async move {
                    sender.0.send(msg).await?;
                    Ok(id)
                })
            }
        })
    }

    pub async fn broadcast(&self, myid: usize, msg: Msg) -> Vec<Result<usize, mailbox::error::SendError<Msg>>>
    where
        Msg: Clone,
    {
        super::future::join_all(self.broadcast_iter(myid, msg)).await
    }

    pub async fn send_to(&self, id: usize, msg: Msg) -> Result<(), mailbox::error::SendError<Msg>> {
        debug_assert!(id < self.0.len());
        let sender = &self.0[id];
        // debug_assert_eq!(sender.1, id);
        sender.0.send(msg).await
    }

    pub fn try_send_to(&self, id: usize, msg: Msg) -> Result<(), mailbox::error::TrySendError<Msg>> {
        debug_assert!(id < self.0.len());
        self.0[id].0.try_send(msg)
    }

    #[inline]
    pub fn pick_receiver<T: Into<u64>>(&self, id: T) -> usize {
        (murmur64(id.into()) as usize) % self.0.len()
    }

    #[inline]
    pub fn pick_receiver_uuid(&self, id: uuid::Uuid) -> usize {
        let p = id.as_u64_pair();
        (murmur128(p.0, p.1) as usize) % self.0.len()
    }

    pub fn find_slot_of(&self, thread_id: usize) -> Option<usize> {
        self.0.iter().position(|(_, id)| *id == thread_id)
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    pub fn is_closed(&self, id: usize) -> bool {
        debug_assert!(id < self.0.len());
        self.0[id].0.is_closed()
    }

    pub fn close_all(self) {}
}

////////////////////////////////////////////////////////////////////////////////
//  Full (NxN) Mesh configuration

struct InitializingMesh<Msg: Send> {
    senders: Vec<OnceCell<(mailbox::Sender<Msg>, usize)>>,
    barrier: Barrier,
}

impl<Msg: Send> InitializingMesh<Msg> {
    fn new(tr: usize) -> Self {
        Self {
            senders: vec![OnceCell::new(); tr],
            barrier: Barrier::new(tr),
        }
    }

    async fn join(self: Arc<Self>, id: usize, cfg: &MeshConfig) -> Mesh<Msg> {
        let (snd, recv) = mailbox::channel::<Msg>(cfg.channel_buffer);
        self.senders[id].set((snd, id)).expect("Mailbox Sender unexpectedly already initialized");
        // wait for all others to set their senders
        self.barrier.wait().await;
        let others = self
            .senders
            .iter()
            .map(|cell| cell.get().expect("Not all Mailbox Senders initialized").clone())
            .collect::<Vec<_>>()
            .into();
        Mesh { me: recv, others }
    }
}

pub struct Mesh<Msg> {
    me: mailbox::Receiver<Msg>,
    others: Senders<Msg>,
}

impl<Msg: Send> Mesh<Msg> {
    pub fn split(self) -> (Senders<Msg>, mailbox::Receiver<Msg>) {
        (self.others, self.me)
    }

    pub fn size(&self) -> usize {
        self.others.size()
    }
}

// TODO also save own-id, drop ref to own sender
// before constructing to avoid 'self-reference'
// debug_assert! in send_to* methods.
////////////////////////////////////////////////////////////////////////////////
//  Create full mesh

pub fn create_full_mesh<W, Fut, Res, MkCfg, Msg>(
    rt: impl ThreadRuntime + Send,
    threads: Option<usize>,
    mkcfg: MkCfg,
    worker: W,
) -> Vec<Result<Res, MeshError>>
where
    MkCfg: Fn(usize) -> MeshConfig + Send + Clone,
    W: Fn(usize, Mesh<Msg>) -> Fut + Clone + Send,
    Fut: Future<Output = Res>,
    Res: Send + 'static,
    Msg: Send + 'static,
{
    super::panic_if_in_runtime();
    let threads = threads.unwrap_or_else(|| num_cpus::get_physical());
    std::thread::scope(|scope| {
        let mesh = Arc::new(InitializingMesh::<Msg>::new(threads));
        let handles = (0..threads)
            .map(|id| {
                let worker = worker.clone();
                let mkcfg = mkcfg.clone();
                let mesh = mesh.clone();
                let rt = rt.clone();
                scope.spawn(move || {
                    let cfg = mkcfg(id);
                    rt.with_executor(cfg.executor_config.clone(), move || async move {
                        // need to do this after the executor is started
                        let local_mesh = mesh.join(id, &cfg).await;
                        worker(id, local_mesh).await
                    })
                })
            })
            .collect::<Vec<_>>();
        // ensure the InitializingMesh instance is dropped. XXX use scope{} instead?
        std::mem::drop(mesh);
        handles.into_iter().map(|t| t.join().map_err(|e| MeshError::JoinError(e))).collect()
    })
}

////////////////////////////////////////////////////////////////////////////////
//  Partial (either sender or receiver) Mesh configuration

struct InitializingPartialMesh<Msg: Send> {
    senders: Vec<OnceCell<(mailbox::Sender<Msg>, usize)>>,
    barrier: Barrier,
}

impl<Msg: Send> InitializingPartialMesh<Msg> {
    fn new(tr: usize) -> Self {
        Self {
            senders: vec![OnceCell::new(); tr],
            barrier: Barrier::new(tr),
        }
    }

    fn publish_mailbox(&self, id: usize, cfg: &MeshConfig) -> mailbox::Receiver<Msg> {
        let (snd, recv) = mailbox::channel::<Msg>(cfg.channel_buffer);
        self.senders[id].set((snd, id)).expect("Mailbox Sender unexpectedly already initialized");
        recv
    }

    async fn join_internal(&self) -> Senders<Msg> {
        self.barrier.wait().await;
        let others = self.senders.iter().filter_map(|cell| cell.get().map(|r| r.clone())).collect::<Vec<_>>().into();
        others
    }
}

pub struct PartialMeshProxy<Msg: Send> {
    inner: Arc<InitializingPartialMesh<Msg>>,
    cfg: MeshConfig,
    id: usize,
}

impl<Msg: Send> PartialMeshProxy<Msg> {
    fn new(inner: Arc<InitializingPartialMesh<Msg>>, cfg: MeshConfig, id: usize) -> Self {
        Self { inner, cfg, id }
    }

    pub async fn join_as_sender(&self) -> Senders<Msg> {
        self.inner.join_internal().await
    }

    pub async fn join_as_receiver(&self) -> mailbox::Receiver<Msg> {
        let recv = self.inner.publish_mailbox(self.id, &self.cfg);
        self.inner.join_internal().await;
        recv
    }

    pub async fn join_as_full_node(&self) -> (mailbox::Receiver<Msg>, Senders<Msg>) {
        let recv = self.inner.publish_mailbox(self.id, &self.cfg);
        let senders = self.inner.join_internal().await;
        (recv, senders)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Create the mesh

pub async fn create_partial_mesh<W, Fut, Res, MkCfg, Msg>(
    rt: impl ThreadRuntime + Send,
    threads: Option<usize>,
    mkconf: MkCfg,
    worker: W,
) -> Vec<Result<Res, MeshError>>
where
    MkCfg: Fn(usize) -> MeshConfig + Send + Clone,
    W: Fn(usize, PartialMeshProxy<Msg>) -> Fut + Clone + Send,
    Fut: Future<Output = Res>,
    Res: Send,
    Msg: Send,
{
    let threads = threads.unwrap_or_else(|| num_cpus::get_physical());
    let mesh = Arc::new(InitializingPartialMesh::<Msg>::new(threads));
    super::threads::async_scoped_fork_join(threads, {
        let rt = rt.clone();
        move |id| {
            let mkconf = mkconf.clone();
            let mesh = mesh.clone();
            let worker = worker.clone();
            let cfg = mkconf(id);
            let mesh = PartialMeshProxy::new(mesh, cfg.clone(), id);
            rt.with_executor(cfg.executor_config, move || async move { worker(id, mesh).await })
        }
    })
    .await
    .into_iter()
    .map(|r| r.ok_or(MeshError::ExecutionError))
    .collect()
}

pub fn create_blocking_partial_mesh<W, Fut, Res, MkCfg, Msg>(
    rt: impl ThreadRuntime + Send,
    threads: Option<usize>,
    mkcfg: MkCfg,
    worker: W,
) -> Vec<Result<Res, MeshError>>
where
    MkCfg: Fn(usize) -> MeshConfig + Send + Clone,
    W: Fn(usize, PartialMeshProxy<Msg>) -> Fut + Clone + Send,
    Fut: Future<Output = Res>,
    Res: Send,
    Msg: Send,
{
    super::panic_if_in_runtime();
    let threads = threads.unwrap_or_else(|| num_cpus::get_physical());
    let mesh = Arc::new(InitializingPartialMesh::<Msg>::new(threads));
    std::thread::scope(|scope| {
        let handles = (0..threads)
            .map(|id| {
                let rt = rt.clone();
                let mkcfg = mkcfg.clone();
                let mesh = mesh.clone();
                let worker = worker.clone();
                scope.spawn(move || {
                    let cfg = mkcfg(id);
                    let mesh = PartialMeshProxy::new(mesh, cfg.clone(), id);
                    rt.with_executor(cfg.executor_config, move || async move { worker(id, mesh).await })
                })
            })
            .collect::<Vec<_>>();
        // ensure the InitializingMesh instance is dropped.
        std::mem::drop(mesh);
        handles.into_iter().map(|t| t.join().map_err(|e| MeshError::JoinError(e))).collect()
    })
}

////////////////////////////////////////////////////////////////////////////////
//  hash functions

#[inline(always)]
const fn murmur64(mut k: u64) -> u64 {
    // MurmurHash64A
    const M: u64 = 0xc6a4a7935bd1e995;
    const R: u32 = 47;
    let mut h: u64 = 0x8445d61a4e774912 ^ (8u64.wrapping_mul(M));

    k = k.wrapping_mul(M);
    k ^= k >> R;
    k = k.wrapping_mul(M);
    h ^= k;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

#[inline(always)]
const fn murmur128(mut k1: u64, mut k2: u64) -> u64 {
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

////////////////////////////////////////////////////////////////////////////////
//  tests

#[cfg(test)]
mod tests {
    use crate::runtime::test_rt;

    use super::*;

    #[test]
    fn full_mesh_send_recv_drop() {
        let thr = 4;
        let res = create_full_mesh(
            test_rt(),
            Some(thr),
            |_id| Default::default(),
            |id, mesh| async move {
                println!("mesh with {} senders", mesh.size());
                let (senders, mut rcv) = mesh.split();
                for i in 0..thr {
                    if i == id {
                        // don't send to self
                        continue;
                    }
                    senders.send_to(i, 1).await.expect("Send error");
                }
                // close all senders so that the loop below terminates properly:
                senders.close_all();
                let mut sum = 0;
                while let Some(msg) = rcv.recv().await {
                    sum += msg;
                }
                sum
            },
        );

        for i in res.into_iter() {
            assert_eq!(i.expect("Execution error"), 3);
        }
    }

    #[test]
    fn partial_mesh() {
        let thr = 4;
        let res = test_rt().with_executor(Default::default(), move || async move {
            create_partial_mesh(
                test_rt(),
                Some(thr),
                |_id| Default::default(),
                |id, mesh| async move {
                    if id % 2 == 0 {
                        let snd = mesh.join_as_sender().await;
                        assert_eq!(snd.size(), 2);
                        snd.send_to(0, id).await.expect("error sending message 1");
                        snd.send_to(1, id).await.expect("error sending message 2");
                        snd.close_all()
                    } else {
                        let mut recv = mesh.join_as_receiver().await;
                        let id1 = recv.recv().await.expect("error receiving message 1");
                        let id2 = recv.recv().await.expect("error receiving message 2");
                        assert!(id1 == 0 || id2 == 0);
                        assert!(id1 == 2 || id2 == 2);
                    }
                    42
                },
            )
            .await
        });
        assert_eq!(res.len(), thr);
        for i in res {
            assert!(matches!(i, Ok(42)));
        }
    }
}

pub mod arcwaker;
pub mod bounded_task_list;
pub mod future;
pub mod mesh;
// pub mod ptrwaker;
pub mod rcwaker;
pub mod stackref;
pub mod threads;
pub mod waker_chain;

mod sync_waker;

crate::util::ifcfg! {
    [test]

    pub mod test_runtime;
    pub(crate) use test_runtime::*;
}

use crate::{
    config::{ClientThreadConfig, ServerConfig, ServerThreadConfig, ThreadPlacement},
    io::{ThreadUring, UringConfig, provide_thread_io, with_thread_io},
    util::TLSResetRAII,
};
use std::{ops::Deref, pin::Pin, rc::Rc, task::Waker, time::Duration};

////////////////////////////////////////////////////////////////////////////////
//  useful runtime-independent re-exports from other crates

pub use tokio::select;
pub mod local {
    pub use local_sync::{OnceCell, mpsc, oneshot, semaphore::Semaphore};
}

pub mod sync {
    pub(crate) use super::sync_waker::WakerMailbox;
    pub use tokio::sync::{Barrier, Notify, OnceCell, Semaphore, mpsc, oneshot};
}

////////////////////////////////////////////////////////////////////////////////
// there are specialized runtimes for client and server,
// but both implement the below interface. code that runs
// on both and needs to spawn can use this, which is
// set when either the server or client runtime is initialized

/// An Executor can spawn tasks and manages I/O
pub trait Executor {
    fn io(&self) -> &crate::io::ThreadUring;
    fn spawn_generic(&self, future: std::pin::Pin<Box<dyn Future<Output = ()>>>);
    fn schedule_remote_waker(&self, waker: Waker);
}

impl<T: Executor> Executor for Rc<T> {
    fn io(&self) -> &crate::io::ThreadUring {
        T::io(self)
    }

    fn spawn_generic(&self, future: std::pin::Pin<Box<dyn Future<Output = ()>>>) {
        T::spawn_generic(self, future)
    }

    fn schedule_remote_waker(&self, waker: Waker) {
        T::schedule_remote_waker(self, waker);
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ExecutorConfig {
    pub tasks_per_thread: usize,
    pub pin_to_core: Option<usize>,
    pub io_config: UringConfig,
}

impl Default for ExecutorConfig {
    fn default() -> Self {
        Self {
            tasks_per_thread: 1024,
            pin_to_core: None,
            io_config: UringConfig::default(),
        }
    }
}

impl ExecutorConfig {
    crate::util::integrated_builder!();

    pub(crate) fn apply_thread_placement(&self) {
        if let Some(cpu) = self.pin_to_core {
            core_affinity::set_for_current(core_affinity::CoreId { id: cpu });
        }
    }
}

impl From<ServerThreadConfig> for ExecutorConfig {
    fn from(cfg: ServerThreadConfig) -> Self {
        Self {
            tasks_per_thread: 16 * 1024,
            pin_to_core: match cfg.server.thread_placement {
                ThreadPlacement::Pinned => Some(cfg.tid),
                ThreadPlacement::Unpinned => None,
            },
            io_config: UringConfig::from((cfg.io, cfg.tid)),
        }
    }
}

impl From<ClientThreadConfig> for ExecutorConfig {
    fn from(cfg: ClientThreadConfig) -> Self {
        Self {
            tasks_per_thread: 32 * 1024,
            pin_to_core: None,
            io_config: UringConfig::from((cfg.io, cfg.tid)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

/// A Runtime can spawn executors
pub trait ThreadRuntime: Clone {
    type Impl: Executor;

    fn with_executor<T, Fut, F>(&self, cfg: ExecutorConfig, f: F) -> T
    where
        Fut: Future<Output = T>,
        F: FnOnce() -> Fut;

    fn current_executor(&self) -> Self::Impl;
}

////////////////////////////////////////////////////////////////////////////////

impl dyn Executor {
    pub fn spawn<F: Future + 'static>(&self, future: F) {
        self.spawn_generic(Box::pin(async move {
            future.await;
        }))
    }

    pub fn with_sync_waker<F: Future>(self: &Rc<Self>, f: F) -> impl Future<Output = F::Output> {
        sync_waker::with_sync_waker(self.clone(), f)
    }

    pub async fn sleep(&self, time: Duration) {
        let _ = self.io().sleep(time).await;
    }
}
pub type GenericExecutor = Rc<dyn Executor>;

////////////////////////////////////////////////////////////////////////////////
//  global state

crate::util::scoped_tls!(static RUNTIME: GenericExecutor);

pub fn with_concrete_executor<T, F: FnOnce() -> T>(inst: Rc<impl Executor + 'static>, f: F) -> T {
    assert!(!RUNTIME.is_set(), "Can not start a runtime inside a runtime");
    let io = inst.io();
    let rt = inst.clone() as GenericExecutor;
    RUNTIME.set(&rt, || with_thread_io(io, f))
}

pub fn generic_executor() -> GenericExecutor {
    RUNTIME.with(|r| r.clone())
}

pub fn rt() -> GenericExecutor {
    generic_executor()
}

pub fn panic_if_in_runtime() {
    assert!(!RUNTIME.is_set(), "We're running in a runtime");
}

////////////////////////////////////////////////////////////////////////////////
//  RAII interface for C++ api & external integration

pin_project_lite::pin_project! {
    struct PinnedExecutor<E> {
        #[pin] inst: Rc<E>,
        #[pin] generic: Rc<dyn Executor>,
        #[pin] io: ThreadUring
    }
}

impl<E: Executor + 'static> PinnedExecutor<E> {
    pub fn new(inst: Rc<E>) -> Pin<Box<Self>> {
        let generic = inst.clone() as Rc<dyn Executor>;
        let io = inst.io().clone();
        Box::pin(Self { inst, generic, io })
    }
}

pub struct ExecutorRAII<E> {
    provider: Pin<Box<PinnedExecutor<E>>>,
    _concrete: TLSResetRAII,
    _generic: TLSResetRAII,
    _io: TLSResetRAII,
}

impl<E: Executor + 'static> ExecutorRAII<E> {
    pub(crate) fn new(concrete_tls: &'static crate::util::ScopedKey<Rc<E>>, inst: Rc<E>) -> ExecutorRAII<E> {
        let provider = PinnedExecutor::new(inst);
        let x = provider.as_ref().project_ref();
        unsafe {
            let _concrete = concrete_tls.bind_pin(x.inst);
            let _generic = RUNTIME.bind_pin(x.generic);
            let _io = provide_thread_io(x.io);
            Self {
                provider,
                _concrete,
                _generic,
                _io,
            }
        }
    }
}

impl<E: Executor + 'static> Deref for ExecutorRAII<E> {
    type Target = Rc<E>;

    fn deref(&self) -> &Self::Target {
        &self.provider.inst
    }
}

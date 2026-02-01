use std::{
    cell::{Cell, UnsafeCell},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::{
    io::{ThreadUring, uring::IOEnterIntent},
    runtime::{
        Executor, ExecutorConfig, ThreadRuntime,
        bounded_task_list::{BoundedTaskList, TaskAction},
        rcwaker::RcWaker,
        sync::WakerMailbox,
    },
};

#[derive(Clone, PartialEq, Eq, Debug)]
#[allow(dead_code)]
pub enum ServerShutdownResult {
    Ok,
    NotAllJournalsFlushed(String),
    NetworkInterfaceError(String),
    RuntimeError(String),
    BusError(String), // ?
}

pub(super) enum InternalRuntimeAction {
    Continue,
    Shutdown,
    DidShutDown(ServerShutdownResult),
}

pub(super) type GenericContinuation = Pin<Box<dyn Future<Output = ()>>>;

////////////////////////////////////////////////////////////////////////////////

pub struct ServerExecutor {
    cfg: ExecutorConfig,
    io: ThreadUring,
    task_buffer: UnsafeCell<Vec<u16>>,
    generic_tasks: UnsafeCell<BoundedTaskList<GenericContinuation>>,
    remote_wakers: WakerMailbox,
    waker_buffer: UnsafeCell<Vec<Waker>>,
    pub(super) wind_down: Cell<bool>,
    #[cfg(feature = "trace-requests")]
    pub(super) polled_qlen: Cell<usize>
}

impl ServerExecutor {
    pub(super) fn new(cfg: ExecutorConfig) -> ServerExecutor {
        reqtrace::calibrate_cheap_clock();
        let io = ThreadUring::new(cfg.io_config.clone()).expect("error initializing server runtime I/O");
        let bound = cfg.tasks_per_thread;
        Self {
            cfg,
            io,
            task_buffer: UnsafeCell::new(Vec::with_capacity(bound)),
            generic_tasks: UnsafeCell::new(BoundedTaskList::new(bound)),
            remote_wakers: WakerMailbox::new(bound),
            waker_buffer: UnsafeCell::new(Vec::with_capacity(bound)),
            wind_down: Cell::new(false),
            #[cfg(feature = "trace-requests")]
            polled_qlen: Cell::new(0)
        }
    }

    pub fn config(&self) -> &ExecutorConfig {
        &self.cfg
    }

    pub fn spawn_task<F: Future + 'static>(&self, fut: F) {
        self.with_open_tasks(|t| {
            t.add_task_or_panic(
                "generic-inner".to_string(),
                Box::pin(async move {
                    fut.await;
                }),
            )
        });
    }

    #[cfg(feature = "trace-requests")]
    pub fn get_current_qlen(&self) -> usize {
        self.polled_qlen.get()
    }

    pub async fn sleep(&self, time: Duration) {
        let _ = self.io.sleep(time).await;
    }

    pub(super) fn with_open_tasks<T, F: FnOnce(&mut BoundedTaskList<GenericContinuation>) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.generic_tasks.get() })
    }

    pub(super) fn poll_generic_tasks(self: &Rc<Self>) -> usize {
        self.with_open_tasks(|t| {
            t.take_active(unsafe { &mut *self.task_buffer.get() });
            #[cfg(feature = "trace-requests")]
            self.polled_qlen.set(unsafe { &*self.task_buffer.get() }.len());
            t.consume_task_id_list_direct(unsafe { &mut *self.task_buffer.get() }, |idx, task| {
                debug_assert!(task.is_active()); // XXX
                let waker = GenericTaskWaker::new_rc_waker(self, idx);
                let mut cx = std::task::Context::from_waker(&waker);
                match task.tracing_poll(&mut cx) {
                    Poll::Ready(()) => TaskAction::Dropped,
                    Poll::Pending => TaskAction::Waiting,
                }
            })
        })
    }

    #[inline]
    pub(super) fn poll_runtime_tasks<Fut, Ptr, MkWaker>(&self, t: &mut BoundedTaskList<Pin<Ptr>>, mkwaker: MkWaker) -> usize
    where
        Fut: Future<Output = InternalRuntimeAction>,
        Ptr: std::ops::DerefMut<Target = Fut>,
        MkWaker: Fn(u16) -> Waker,
    {
        t.take_active(unsafe { &mut *self.task_buffer.get() });
        #[cfg(feature = "trace-requests")]
        self.polled_qlen.set(unsafe { &*self.task_buffer.get() }.len());
        t.consume_task_id_list_direct(unsafe { &mut *self.task_buffer.get() }, |idx, task| {
            debug_assert!(task.is_active()); // XXX
            let waker = mkwaker(idx);
            let mut cx = std::task::Context::from_waker(&waker);
            match task.tracing_poll(&mut cx) {
                Poll::Ready(action) => {
                    match action {
                        InternalRuntimeAction::Continue => {}
                        InternalRuntimeAction::Shutdown => {
                            self.wind_down.set(true);
                        }
                        InternalRuntimeAction::DidShutDown(_server_shutdown_result) => self.wind_down.set(true),
                    }
                    TaskAction::Dropped
                }
                Poll::Pending => TaskAction::Waiting,
            }
        })
    }

    pub(super) fn check_mailbox(&self) -> usize {
        let buffer = unsafe { &mut *self.waker_buffer.get() };
        self.remote_wakers.take_wakers_sync(buffer);
        let len = buffer.len();
        while let Some(waker) = buffer.pop() {
            waker.wake();
        }
        len
    }

    pub fn enter_io(self: &Rc<Self>, progress: usize) {
        if progress == 0 {
            self.io.enter(IOEnterIntent::Starved);
        } else {
            self.io.enter(IOEnterIntent::Poll);
        }
    }

    pub fn enter_io_direct(self: &Rc<Self>, intent: IOEnterIntent) {
        self.io.enter(intent);
    }

    /// this is really meant as a stand-in for launching an standalone executor
    /// the server runtime is meant to be integrated into server implementations
    fn block_on<T, F: Future<Output = T>>(self: &Rc<Self>, fut: F) -> T {
        let mut main_fut = std::pin::pin!(fut);
        let mut main_ctx = Context::from_waker(Waker::noop());
        loop {
            // XXX only poll main task when woken?
            if let Poll::Ready(res) = main_fut.as_mut().poll(&mut main_ctx) {
                let open_generic = self.with_open_tasks(|t| t.tasks_in_system());
                if open_generic > 0 {
                    eprintln!("dropping {} unfinished generic tasks", open_generic);
                }
                return res;
            }
            // check remote tasks
            let mut progress = self.check_mailbox();
            // poll other tasks
            progress += self.poll_generic_tasks();
            // enter I/O if necessary
            self.enter_io(progress);
            // eprintln!("in block_on loop; this usually means something is wrong")
        }
    }

    fn wake_generic_task(self: &Rc<Self>, tag: u16) {
        #[cfg(debug_assertions)]
        Self::assert_single_threaded(self);
        self.with_open_tasks(|t| t.activate_task(tag));
    }

    #[cfg(debug_assertions)]
    fn assert_single_threaded(call_origin: &Rc<Self>) {
        assert_eq!(
            call_origin.as_ref() as *const Self,
            server_exec().as_ref() as *const Self,
            "Waker must be called from the same thread!"
        );
    }

    pub(super) fn executor_stats(&self) -> String {
        format!(
            "(ServerExecutor :generic {} :buffer {})",
            self.with_open_tasks(|r| r.task_stats_str()),
            unsafe { &*self.task_buffer.get() }.len()
        )
    }
}

////////////////////////////////////////////////////////////////////////////////

struct GenericTaskWaker {}
impl RcWaker for GenericTaskWaker {
    type Handler = ServerExecutor;

    fn on_wake(handler: Rc<Self::Handler>, tag: u16) {
        handler.wake_generic_task(tag);
    }

    fn on_wake_by_ref(handler: &Rc<Self::Handler>, tag: u16) {
        handler.wake_generic_task(tag);
    }
}

////////////////////////////////////////////////////////////////////////////////

impl Executor for ServerExecutor {
    fn io(&self) -> &crate::io::ThreadUring {
        &self.io
    }

    fn spawn_generic(&self, future: std::pin::Pin<Box<dyn Future<Output = ()>>>) {
        self.with_open_tasks(|t| t.add_task_or_panic("generic-outer".to_string(), future));
    }

    fn schedule_remote_waker(&self, waker: Waker) {
        self.remote_wakers.enqueue(waker);
        self.io.unpark();
    }
}

////////////////////////////////////////////////////////////////////////////////
//  global state for accessing the runtime from anywhere

crate::util::scoped_tls!(static EXECUTOR: Rc<ServerExecutor>);

#[derive(Clone)]
pub struct ServerRuntime;
impl ServerRuntime {}
impl ThreadRuntime for ServerRuntime {
    type Impl = Rc<ServerExecutor>;

    fn with_executor<T, Fut, F>(&self, cfg: ExecutorConfig, f: F) -> T
    where
        Fut: Future<Output = T>,
        F: FnOnce() -> Fut,
    {
        cfg.apply_thread_placement();
        let rt = Rc::new(ServerExecutor::new(cfg));
        EXECUTOR.set(&rt, || crate::runtime::with_concrete_executor(rt.clone(), || rt.block_on(f())))
    }

    fn current_executor(&self) -> Self::Impl {
        EXECUTOR.with(|r| r.clone())
    }
}

#[inline]
pub fn server_rt() -> ServerRuntime {
    static RT: ServerRuntime = ServerRuntime {};
    RT.clone()
}

#[inline]
pub fn server_exec() -> Rc<ServerExecutor> {
    server_rt().current_executor()
}

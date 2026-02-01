use std::{
    cell::UnsafeCell,
    cell::Cell,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use crate::{
    io::{ThreadUring, uring::IOEnterIntent},
    runtime::{
        Executor, ExecutorConfig, ExecutorRAII, ThreadRuntime,
        bounded_task_list::{BoundedTaskList, TaskAction, TaskState},
        rcwaker::RcWaker,
        sync::WakerMailbox,
    },
};

pub(super) type GenericContinuation = Pin<Box<dyn Future<Output = ()>>>;

pub struct ClientExecutor {
    cfg: ExecutorConfig,
    io: ThreadUring,
    generic_tasks: UnsafeCell<BoundedTaskList<GenericContinuation>>,
    task_buffer: UnsafeCell<Vec<u16>>,
    remote_wakers: WakerMailbox,
    waker_buffer: UnsafeCell<Vec<Waker>>,
    current_qlen: Cell<usize>
}

impl Drop for ClientExecutor {
    fn drop(&mut self) {
        let open_generic = self.with_open_tasks(|t| t.tasks_in_system());
        if open_generic > 0 {
            eprintln!("dropping {} unfinished generic tasks", open_generic);
        }
    }
}

impl ClientExecutor {
    pub fn new(cfg: ExecutorConfig) -> Self {
        reqtrace::calibrate_cheap_clock();
        let io = ThreadUring::new(cfg.io_config.clone()).expect("error initializing server runtime I/O");
        let bound = cfg.tasks_per_thread;
        Self {
            cfg,
            io,
            generic_tasks: UnsafeCell::new(BoundedTaskList::new(bound)),
            task_buffer: UnsafeCell::new(Vec::with_capacity(bound)),
            remote_wakers: WakerMailbox::new(bound),
            waker_buffer: UnsafeCell::new(Vec::with_capacity(bound)),
            current_qlen: Cell::new(0)
        }
    }

    pub fn spawn_awaitable_task<F: Future + 'static>(&self, fut: F) -> impl Future<Output = F::Output> + use<F> {
        let (snd, rcv) = crate::runtime::local::oneshot::channel();
        self.spawn_task(async move {
            let _ = snd.send(fut.await);
        });
        async move { rcv.await.expect("error receiving awaitable task result") }
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

    pub async fn sleep(&self, time: std::time::Duration) {
        self.io.sleep(time).await.expect("error sleeping")
    }

    pub(super) fn with_open_tasks<T, F: FnOnce(&mut BoundedTaskList<GenericContinuation>) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.generic_tasks.get() })
    }

    pub fn check_mailbox(&self) -> usize {
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
        self.io.enter(intent)
    }

    pub fn tick(self: &Rc<Self>, mut n: usize) {
        while n > 0 {
            while n > 0 {
                n -= 1;
                let mut progress = self.poll_generic_tasks();
                progress += self.poll_generic_tasks();
                if progress == 0 {
                    break;
                }
                self.enter_io_direct(IOEnterIntent::Submit);
            }
            let remote = self.check_mailbox();
            if remote == 0 || n == 0 {
                self.enter_io_direct(IOEnterIntent::Starved);
                return;
            }
        }
    }

    pub fn get_current_qlen(&self) -> usize {
        self.current_qlen.get()
    }

    /// this is really meant as a stand-in for launching an standalone executor
    /// the server runtime is meant to be integrated into server implementations
    pub fn block_on<T, F: Future<Output = T>>(self: &Rc<Self>, fut: F) -> T {
        let mut main_fut = std::pin::pin!(fut);
        let mut main_ctx = Context::from_waker(Waker::noop());
        loop {
            // XXX only poll main task when woken?
            if let Poll::Ready(res) = main_fut.as_mut().poll(&mut main_ctx) {
                return res;
            }
            // let _ = self.io.access(|uring| uring.poll_completions());
            self.tick(10);
        }
    }

    pub fn poll_generic_tasks(self: &Rc<Self>) -> usize {
        self.with_open_tasks(|t| {
            t.take_active(unsafe { &mut *self.task_buffer.get() });
            self.current_qlen.set(unsafe { &*self.task_buffer.get() }.len());
            t.consume_task_id_list(unsafe { &mut *self.task_buffer.get() }, |idx, task, tstate| {
                debug_assert!(matches!(tstate, TaskState::Active)); // XXX
                let waker = GenericTaskWaker::new_rc_waker(self, idx);
                let mut cx = std::task::Context::from_waker(&waker);
                match Pin::new(task).poll(&mut cx) {
                    Poll::Ready(()) => TaskAction::Dropped,
                    Poll::Pending => TaskAction::Waiting,
                }
            })
        })
    }

    fn wake_generic_task(self: &Rc<Self>, tag: u16) {
        #[cfg(debug_assertions)]
        Self::assert_single_threaded(self);
        self.with_open_tasks(|t| t.activate_task(tag));
    }

    #[inline]
    #[cfg(debug_assertions)]
    fn assert_single_threaded(call_origin: &Rc<Self>) {
        assert_eq!(
            call_origin.as_ref() as *const Self,
            client_exec().as_ref() as *const Self,
            "Waker must be called from the same thread!"
        );
    }
}

impl Executor for ClientExecutor {
    fn io(&self) -> &crate::io::ThreadUring {
        &self.io
    }

    fn spawn_generic(&self, future: Pin<Box<dyn Future<Output = ()>>>) {
        self.with_open_tasks(|t| t.add_task_or_panic("generic-outer".to_string(), future));
    }

    fn schedule_remote_waker(&self, waker: Waker) {
        self.remote_wakers.enqueue(waker);
        self.io.unpark();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct GenericTaskWaker {}
impl RcWaker for GenericTaskWaker {
    type Handler = ClientExecutor;

    fn on_wake(handler: Rc<Self::Handler>, tag: u16) {
        handler.wake_generic_task(tag);
    }

    fn on_wake_by_ref(handler: &Rc<Self::Handler>, tag: u16) {
        handler.wake_generic_task(tag);
    }
}

////////////////////////////////////////////////////////////////////////////////
//  global state for accessing the runtime from anywhere

crate::util::scoped_tls!(static EXECUTOR: Rc<ClientExecutor>);

#[derive(Clone)]
pub struct ClientRuntime;
impl ClientRuntime {
    pub fn provide_executor<'a>(&self, cfg: ExecutorConfig) -> ExecutorRAII<ClientExecutor> {
        assert!(!EXECUTOR.is_set(), "Can not start a runtime inside a runtime");
        ExecutorRAII::new(&EXECUTOR, Rc::new(ClientExecutor::new(cfg)))
    }
}

impl ThreadRuntime for ClientRuntime {
    type Impl = Rc<ClientExecutor>;

    fn with_executor<T, Fut, F>(&self, cfg: ExecutorConfig, f: F) -> T
    where
        Fut: Future<Output = T>,
        F: FnOnce() -> Fut,
    {
        cfg.apply_thread_placement();
        let rt = Rc::new(ClientExecutor::new(cfg));
        EXECUTOR.set(&rt, || crate::runtime::with_concrete_executor(rt.clone(), || rt.block_on(f())))
    }

    fn current_executor(&self) -> Self::Impl {
        EXECUTOR.with(|r| r.clone())
    }
}

#[inline]
pub fn client_rt() -> ClientRuntime {
    static RT: ClientRuntime = ClientRuntime {};
    RT.clone()
}

#[inline]
pub fn client_exec() -> Rc<ClientExecutor> {
    client_rt().current_executor()
}

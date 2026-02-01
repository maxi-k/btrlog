use futures_util::{Future, task::AtomicWaker};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

use super::{Executor, ExecutorConfig, ThreadRuntime};

////////////////////////////////////////////////////////////////////////////////
//  Multiple Threads

// TODO explicit lifetime tracking ala thread::scope::Scope with <'env, 'scope>?
struct JoinHandlesInner<T> {
    waiting_for: AtomicUsize,
    results: Vec<Mutex<Option<T>>>, // oof
    origin_waker: AtomicWaker,
    origin: std::rc::Rc<dyn Executor>,
}
unsafe impl<T: Send> Send for JoinHandlesInner<T> {}
unsafe impl<T: Send> Sync for JoinHandlesInner<T> {}
pub struct AsyncJoinHandles<T: Send>(Arc<JoinHandlesInner<T>>);

impl<T> JoinHandlesInner<T> {
    fn new(n: usize) -> JoinHandlesInner<T> {
        let mut results = Vec::with_capacity(n);
        for _i in 0..n {
            results.push(Mutex::new(None));
        }
        Self {
            waiting_for: n.into(),
            results,
            origin_waker: AtomicWaker::new(),
            origin: super::rt().clone(),
        }
    }

    fn put(&self, tid: usize, value: T) {
        let mut slot = self.results[tid].try_lock().expect("Can't lock our own join handle?"); // XXX
        debug_assert!(slot.is_none(), "join handle already initialzied");
        *slot = Some(value);
    }

    fn poll_inner(&self, cx: &mut std::task::Context<'_>) -> Poll<Vec<Option<T>>> {
        self.origin_waker.register(cx.waker());
        let waiting = self.waiting_for.load(Ordering::Acquire);
        if waiting == 0 {
            let res = self
                .results
                .iter()
                .map(|cell| {
                    cell.try_lock().expect("Can't lock thread handle mutex").take() // XXX
                })
                .collect();
            Poll::Ready(res)
        } else {
            Poll::Pending
        }
    }

    fn wake(&self) {
        if let Some(waker) = self.origin_waker.take() {
            self.origin.schedule_remote_waker(waker);
        }
    }
}

impl<T: Send> Future for JoinHandlesInner<T> {
    type Output = Vec<Option<T>>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.poll_inner(cx)
    }
}

impl<T: Send> Future for AsyncJoinHandles<T> {
    type Output = Vec<Option<T>>;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        self.0.poll_inner(cx)
    }
}

/// In an executor, spawning threads and then join()ing them will
/// block the calling thread and starve the exexutor, leading to subtle
/// bugs where progress can't be made.
///
/// This spawns n threads with their own executor and returns a future that awaits their completion.
pub fn async_fork_join<T, G>(n: usize, workfn: G) -> AsyncJoinHandles<T>
where
    G: Fn(usize) -> T,
    G: Clone + Send + 'static,
    T: Send + 'static,
{
    let h = Arc::new(JoinHandlesInner::new(n));
    for tid in 0..n {
        std::thread::spawn({
            let h = h.clone();
            let workfn = workfn.clone();
            move || {
                let res = workfn(tid);
                let waiters = h.waiting_for.fetch_sub(1, Ordering::Release);
                if waiters == 1 {
                    h.wake()
                }
                res
            }
        });
    }
    AsyncJoinHandles(h) // implements Future
}

pub async fn async_scoped_fork_join<T, G>(n: usize, workfn: G) -> Vec<Option<T>>
where
    G: FnOnce(usize) -> T,
    G: Clone + Send,
    T: Send,
{
    // XXX lifetimes would allow putting a reference to h in spawned threads, the we could forgo the ARc
    // but a threads' fetch_sub might cause the scope to deallocate immediately if the main thread sees
    // that waiters == 1 before we can wake the waker, which would be an invalid memory access.
    // Until we can figure out an order of operations that ensures this can't happen (for example, second atomic after waiting for),
    // let's use an arc instead (which is, in effect, a second atomic for the reference count).
    let h = Arc::new(JoinHandlesInner::new(n));
    let mkworker = |tid: usize, workfn: G| {
        let h = h.clone();
        move || {
            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| workfn(tid)));
            match res {
                Ok(value) => h.put(tid, value),
                Err(panic_payload) => {
                    eprintln!("thread {} panicked: {:?}", tid, panic_payload);
                } /*panic::resume_unwind(panic_payload),*/
            }
            // println!("forkjoin thread {} done", tid);
            let waiters = h.waiting_for.fetch_sub(1, Ordering::Release);
            if waiters == 1 {
                h.wake()
            }
        }
    };
    // spawn n threads
    for tid in 0..n {
        // we need spawn_unchecked here to allow non-static lifetimes.
        // SAFETY: the required context should stay valid during the lifetime of this futures' state machine,
        // which is ensured by JoinHandlesInner
        unsafe {
            std::thread::Builder::new()
                .spawn_unchecked(mkworker(tid, workfn.clone()))
                .expect("error spawning thread");
        }
    }
    AsyncJoinHandles(h).await
}

////////////////////////////////////////////////////////////////////////////////
//  spawn threads with executors already running

pub fn spawn_n_executors<RT, Cfg, T, G, F>(rt: &RT, cfg: Cfg, n: usize, fut: G) -> AsyncJoinHandles<T>
where
    G: Fn(usize) -> F,
    G: Clone + Send + 'static,
    F: Future<Output = T>,
    T: Send + 'static,
    RT: ThreadRuntime + Send + 'static,
    Cfg: Fn(usize) -> ExecutorConfig + Clone + Send + 'static,
{
    async_fork_join(n, {
        let rt = rt.clone();
        move |tid| {
            let fut = fut.clone();
            let cfg = cfg.clone();
            rt.with_executor(cfg(tid), move || fut(tid))
        }
    })
}

pub async fn spawn_n_scoped_executors<RT, Cfg, T, G, F>(rt: &RT, cfg: Cfg, n: usize, fut: G) -> Vec<Option<T>>
where
    G: Fn(usize) -> F,
    G: Clone + Send,
    F: Future<Output = T>,
    T: Send,
    RT: ThreadRuntime + Send + 'static,
    Cfg: Fn(usize) -> ExecutorConfig + Clone + Send + 'static,
{
    async_scoped_fork_join(n, {
        let rt = rt.clone();
        move |tid| {
            let fut = fut.clone();
            let cfg = cfg.clone();
            rt.with_executor(cfg(tid), move || fut(tid))
        }
    })
    .await
}

/// Spawns a single thread with its own executor and returns a future that awaits its completion.
/// Similar to spawn_n_executors but for a single thread.
pub async fn spawn_single_executor<T, F, Fut, RT>(rt: &RT, cfg: ExecutorConfig, fut: F) -> Result<T, ()>
where
    F: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T>,
    T: Send + 'static,
    RT: ThreadRuntime + Send + 'static,
{
    use super::sync::oneshot;
    let (sender, receiver) = oneshot::channel();
    let rt = rt.clone();
    let _join_handle = std::thread::spawn(move || {
        let res = rt.with_executor(cfg, fut);
        let _ = sender.send(res); // Ignore send error if receiver is dropped
    });
    super::rt().with_sync_waker(receiver).await.map_err(|_e| ())
}

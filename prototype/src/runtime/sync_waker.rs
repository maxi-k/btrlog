use std::{
    pin::Pin,
    rc::Rc,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Waker},
};

use futures_util::task::AtomicWaker;

use super::{Executor, arcwaker::ArcWaker};

pin_project_lite::pin_project! {
    pub(super) struct SyncFutureWrapper<F> {
        #[pin] inner: F,
        // XXX avoid arc allocation?
        executor_ref: Arc<ExecutorRef>
    }
}

#[inline]
pub fn with_sync_waker<F: Future>(rt: Rc<dyn Executor>, f: F) -> impl Future<Output = F::Output> {
    SyncFutureWrapper {
        inner: f,
        executor_ref: Arc::new(ExecutorRef {
            rt,
            local_waker: AtomicWaker::new(),
        }),
    }
}

impl<F: Future> Future for SyncFutureWrapper<F> {
    type Output = F::Output;

    // - poll only called from local thread
    // - but wake may be called from different thread
    // - wake and poll may be called concurrently
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();
        // XXX poll first with old ctx, then do expensive atomic stuff, then
        // repoll? but depending on future, might be more expensive...
        let waker = this.executor_ref.get_synchronized_waker(cx.waker());
        let mut sync_cx = Context::from_waker(&waker);
        this.inner.poll(&mut sync_cx)
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(super) struct ExecutorRef {
    rt: Rc<dyn Executor>,
    local_waker: AtomicWaker,
}

impl ExecutorRef {
    pub(super) fn get_synchronized_waker(self: &Arc<Self>, waker: &Waker) -> Waker {
        self.local_waker.register(waker);
        Self::new_arc_waker(self, 42)
    }

    // pub(super) unsafe fn get_synchronized_unsafe_waker(&self, waker: &Waker) -> Waker {
    //     self.local_waker.register(waker);
    //     unsafe { Self::new_ptr_waker(self, 42) }
    // }

    fn wake_inner(&self, _tag: u16) {
        debug_assert_eq!(_tag, 42);
        match self.local_waker.take() {
            Some(waker) => self.rt.schedule_remote_waker(waker),
            None => {
                log::warn!("synchronized waker not set yet on wake; can't wake task");
            }
        }
    }
}

/*
impl PtrWaker for ExecutorRef {
    type Handler = ExecutorRef;

    fn on_wake(handler: &Self::Handler, tag: u16) {
        handler.wake_inner(tag);
    }

    fn on_wake_by_ref(handler: &Self::Handler, tag: u16) {
        handler.wake_inner(tag);
    }
}*/

impl ArcWaker for ExecutorRef {
    type Handler = ExecutorRef;

    fn on_wake(handler: Arc<Self::Handler>, tag: u16) {
        handler.wake_inner(tag);
    }

    fn on_wake_by_ref(handler: &Arc<Self::Handler>, tag: u16) {
        handler.wake_inner(tag);
    }
}

////////////////////////////////////////////////////////////////////////////////

pub(crate) struct WakerMailbox {
    queue: Mutex<Vec<Waker>>,
    notify: AtomicBool,
}

impl WakerMailbox {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            queue: Mutex::new(Vec::with_capacity(cap)),
            notify: AtomicBool::new(false),
        }
    }

    pub(crate) fn enqueue(&self, waker: Waker) {
        match self.queue.lock() {
            Ok(mut x) => x.push(waker),
            Err(mut poison) => {
                log::error!("waker mailbox mutex poisoned? {:?}", poison);
                poison.get_mut().push(waker);
                self.queue.clear_poison();
            }
        }
        self.notify.store(true, Ordering::Release);
    }

    #[inline]
    pub(crate) fn take_wakers_sync(&self, buffer: &mut Vec<Waker>) {
        debug_assert!(buffer.is_empty());
        // fast path; we don't need to take the mutex, no atomic stores
        if !self.notify.load(Ordering::Relaxed) {
            return;
        }
        // the only disallowed sequence is this:
        // - thread 1: enqueue, set notify
        // - thread 2: clear queue
        // - thread 1: enqueue, set notify
        // - thread 2: clear notify
        // <-- now, notify is unset but queue has elements
        self.notify.store(false, Ordering::SeqCst);
        match self.queue.lock() {
            Ok(mut wakers) => {
                std::mem::swap(wakers.as_mut(), buffer);
            }
            Err(mut poison) => {
                log::error!("waker mailbox mutex poisoned? {:?}", poison);
                std::mem::swap(poison.get_mut().as_mut(), buffer);
                self.queue.clear_poison();
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;
    use crate::runtime::*;

    #[apply(async_test)]
    fn provoke_wake_failure() {
        let (snd, mut rcv) = sync::mpsc::channel::<(usize, sync::oneshot::Sender<usize>)>(1);
        let thr = std::thread::spawn(move || {
            let mut x = 0;
            while let Some((i, reply)) = rcv.blocking_recv() {
                reply.send(i + 42).expect("error sending reply");
                x += 1;
            }
            println!("[T1] sent {}", x);
        });

        let received = Arc::new(AtomicUsize::new(0));
        let n = 64usize;
        for i in 0..n {
            let received = received.clone();
            let snd = snd.clone();
            rt().spawn(async move {
                let (src, sink) = sync::oneshot::channel::<usize>();
                rt().with_sync_waker(snd.send((i, src))).await.expect("error sending");
                let res = rt().with_sync_waker(sink).await;
                println!("task {} received reply {:?}", i, res);
                received.fetch_add(1, Ordering::Relaxed);
            })
        }

        while received.load(Ordering::Relaxed) < n {
            crate::runtime::future::maybe_yield().await;
        }
        println!("received {}", received.load(Ordering::Relaxed));
        std::mem::drop(snd);
        thr.join().expect("error joining thread");
    }
}

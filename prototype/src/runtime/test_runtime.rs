use std::{
    cell::UnsafeCell, pin::Pin, rc::Rc, sync::Arc, task::{Context, Poll, Waker}, time::Duration
};

use crate::{io::{ThreadUring, uring::IOEnterIntent}, runtime::bounded_task_list::TaskState};

use super::{
    bounded_task_list::{BoundedTaskList, TaskAction}, rcwaker::RcWaker, sync_waker::WakerMailbox, Executor, ExecutorConfig, ThreadRuntime
};

pub(crate) struct TestExecutor {
    tasks: UnsafeCell<BoundedTaskList<Pin<Box<dyn Future<Output = ()>>>>>,
    mailbox: Arc<WakerMailbox>,
    io: ThreadUring,
}

// XXX also implement a DST variant
impl TestExecutor {
    pub fn spawn<F: Future + 'static>(&self, future: F) {
        self.spawn_generic(Box::pin(async move {
            future.await;
        }))
    }

    pub async fn sleep(&self, time: Duration) {
        self.io.sleep(time).await.expect("I/O error sleeping");
    }

    fn block_on<T, F: Future<Output = T>>(self: &Rc<Self>, fut: F) -> T {
        let mut task_buffer = Vec::with_capacity(unsafe { & *self.tasks.get() }.capacity());
        let mut waker_buffer = Vec::with_capacity(unsafe { & *self.tasks.get() }.capacity());
        let mut main_fut = std::pin::pin!(fut);
        let mut main_ctx = Context::from_waker(Waker::noop());
        loop {
            // enter I/O if necessary
            self.io.access(|ring| {
                ring.enter(IOEnterIntent::Poll);
                ring.poll_completions().expect("error polling completions");
            });
            // clear mailbox
            self.check_mailbox(&mut waker_buffer);
            // poll other tasks
            self.poll_active_tasks(&mut task_buffer);
            // XXX only poll main task when woken?
            if let Poll::Ready(res) = main_fut.as_mut().poll(&mut main_ctx) {
                let unfinished = unsafe { self.borrow_tasks() }.tasks_in_system();
                if unfinished > 0 {
                    eprintln!("dropping {} unfinished tasks", unfinished);
                }
                return res;
            }
        }
    }

    unsafe fn borrow_tasks(&self) -> &mut BoundedTaskList<Pin<Box<dyn Future<Output = ()>>>> {
        unsafe { &mut *self.tasks.get() }
    }

    fn poll_active_tasks(self: &Rc<Self>, buffer: &mut Vec<u16>) -> usize {
        unsafe { self.borrow_tasks() }.take_active(buffer);
        unsafe { self.borrow_tasks() }.consume_task_id_list(buffer, |idx, task, tstate| {
            debug_assert!(matches!(tstate, TaskState::Active)); // XXX
            let waker = Self::new_rc_waker(self, idx);
            let mut cx = std::task::Context::from_waker(&waker);
            match Pin::new(task).poll(&mut cx) {
                Poll::Ready(_) => TaskAction::Dropped,
                Poll::Pending => TaskAction::Waiting,
            }
        })
    }

    fn check_mailbox(&self, buffer: &mut Vec<Waker>) {
        self.mailbox.take_wakers_sync(buffer);
        while let Some(waker) = buffer.pop() {
            waker.wake();
        }
    }

    fn assert_single_threaded(call_origin: &Rc<Self>) {
        assert_eq!(
           call_origin.as_ref() as *const Self,
           test_exec().as_ref() as *const Self,
            "Waker must be called from the same thread!"
        );
    }
}

impl Executor for TestExecutor {
    fn io(&self) -> &crate::io::ThreadUring {
        &self.io
    }

    fn spawn_generic(&self, future: std::pin::Pin<Box<dyn Future<Output = ()>>>) {
        unsafe { self.borrow_tasks() }.add_task_or_panic("generic-outer".to_string(), future);
    }

    fn schedule_remote_waker(&self, waker: Waker) {
        self.mailbox.enqueue(waker);
        self.io.unpark();
    }

}

impl RcWaker for TestExecutor {
    type Handler = Self;
    fn on_wake(handler: Rc<Self>, tag: u16) {
        Self::assert_single_threaded(&handler);
        unsafe { handler.borrow_tasks() }.activate_task(tag)
    }

    fn on_wake_by_ref(handler: &Rc<Self>, tag: u16) {
        Self::assert_single_threaded(handler);
        unsafe { handler.borrow_tasks() }.activate_task(tag)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  global state for accessing the runtime from anywhere

crate::util::scoped_tls!(static EXECUTOR: Rc<TestExecutor>);

#[derive(Clone)]
pub(crate) struct TestRuntime;
impl TestRuntime{}
impl super::ThreadRuntime for TestRuntime {
    type Impl = Rc<TestExecutor>;

    fn with_executor<T, Fut, F>(&self, cfg: ExecutorConfig, f: F) -> T
    where Fut: Future<Output = T>,
          F: FnOnce() -> Fut {
        cfg.apply_thread_placement();
        let rt = Rc::new(TestExecutor {
            tasks: UnsafeCell::new(BoundedTaskList::new(cfg.tasks_per_thread)),
            mailbox: Arc::new(WakerMailbox::new(cfg.tasks_per_thread)),
            io: ThreadUring::new(cfg.io_config).expect("error initializing uring"),
        });
        assert!(!EXECUTOR.is_set(), "Cannot nest executors");
        EXECUTOR.set(&rt, || super::with_concrete_executor(rt.clone(), || rt.block_on(f())))
    }

    fn current_executor(&self) -> Self::Impl {
        assert!(EXECUTOR.is_set(), "Trying to access executor in thread that doesn't have one!");
        EXECUTOR.with(|r| r.clone())
    }
}

#[inline]
pub(crate) fn test_rt() -> TestRuntime {
    static RT: TestRuntime = TestRuntime{};
    RT.clone()
}

#[inline]
pub(crate) fn test_exec() -> Rc<TestExecutor> {
    test_rt().current_executor()
}

////////////////////////////////////////////////////////////////////////////////
//  macro for async tests

/// Wrapping macro to run async tests with new_test_runtime
/// Use with macro_rules_attr::apply like:
/// ```
/// #[test]
/// #[apply(test_runtime_test!)]
/// async fn my_test() {
///     // test code
/// }
/// ```
macro_rules! async_test {
    // use passed runtime; 
    (
        $(#[$meta:meta])*
        $(async)? fn $name:ident() $body:block
        $param:ident
    ) => {
        #[test]
        $(#[$meta])*
        fn $name() {
            $param().with_executor(Default::default(), || async $body)
        }
    };

    // no explicit runtime set -> use TestRuntime  
    (
        $(#[$meta:meta])*
        $(async)? fn $name:ident() $body:block
    ) => {
        #[test]
        $(#[$meta])*
        fn $name() {
            $crate::runtime::test_runtime::test_rt().with_executor(Default::default(), || async $body)
        }
    };

}

pub(crate) use async_test;
pub(crate) use macro_rules_attr::apply;

////////////////////////////////////////////////////////////////////////////////
//  test the test runtime

#[cfg(test)]
mod tests {
    use super::*;
    use macro_rules_attr::apply;
    use std::cell::{Cell, RefCell};

    #[test]
    fn test_basic_block_on() {
        let result = test_rt().with_executor(Default::default(), || async { 42 });
        assert_eq!(result, 42);
    }

    #[test]
    fn test_spawn_single_task() {
        let counter = Rc::new(Cell::new(0));
        let counter_clone = counter.clone();

        test_rt().with_executor(Default::default(), || async move {
            let rt = test_exec();
            rt.spawn(async move {
                counter_clone.set(counter_clone.get() + 1);
            });

            // Give task time to run
            rt.sleep(Duration::from_millis(10)).await;
        });

        assert_eq!(counter.get(), 1);
    }

    #[test]
    fn test_spawn_multiple_tasks() {
        let counter = Rc::new(Cell::new(0));
        let num_tasks = 5;
        let counter_main = counter.clone();

        test_rt().with_executor(Default::default(), move || async move {
            let rt = test_exec();
            for _ in 0..num_tasks {
                let counter_clone = counter_main.clone();
                rt.spawn(async move {
                    counter_clone.set(counter_clone.get() + 1);
                });
            }

            // Give tasks time to run
            rt.sleep(Duration::from_millis(10)).await;
        });

        assert_eq!(counter.get(), num_tasks);
    }

    #[test]
    fn test_sleep() {
        use std::time::Instant;

        test_rt().with_executor(Default::default(), || async {
            let rt = test_exec();
            let start = Instant::now();
            rt.sleep(Duration::from_millis(50)).await;
            let elapsed = start.elapsed();

            // Check that we actually slept for roughly the right amount of time
            assert!(elapsed >= Duration::from_millis(45));
            assert!(elapsed < Duration::from_millis(150));
        });
    }

    #[test]
    fn test_nested_spawns() {
        let counter = Rc::new(Cell::new(0));
        let counter_main = counter.clone();

        test_rt().with_executor(Default::default(), move || async move {
            let rt = test_exec();
            let counter_clone = counter_main.clone();

            rt.spawn(async move {
                counter_clone.set(counter_clone.get() + 1);
                let rt = test_exec();
                let counter_clone2 = counter_clone.clone();
                rt.spawn(async move {
                    counter_clone2.set(counter_clone2.get() + 10);
                });
            });

            // Give tasks time to run
            rt.sleep(Duration::from_millis(20)).await;
        });

        assert_eq!(counter.get(), 11);
    }

    #[test]
    fn test_tasks_complete_before_main() {
        let completed = Rc::new(Cell::new(false));
        let completed_main = completed.clone();

        test_rt().with_executor(Default::default(), move || async move {
            let rt = test_exec();
            let completed_clone = completed_main.clone();

            rt.spawn(async move {
                let rt = test_exec();
                rt.sleep(Duration::from_millis(10)).await;
                completed_clone.set(true);
            });

            // Main task waits longer to ensure spawned task completes
            rt.sleep(Duration::from_millis(50)).await;
        });

        assert!(completed.get());
    }

    #[test]
    fn test_spawn_with_io() {
        let results = Rc::new(RefCell::new(Vec::new()));
        let results_main = results.clone();

        
        test_rt().with_executor(Default::default(), move || async move {
            let rt = test_exec();

            for i in 0..3 {
                let results_clone = results_main.clone();
                rt.spawn(async move {
                    let rt = test_exec();
                    rt.sleep(Duration::from_millis(10 * (i + 1) as u64)).await;
                    results_clone.borrow_mut().push(i);
                });
            }

            // Wait for all tasks to complete
            rt.sleep(Duration::from_millis(100)).await;
        });

        let mut final_results = results.borrow().clone();
        final_results.sort();
        assert_eq!(final_results, vec![0, 1, 2]);
    }

    #[apply(async_test)]
    async fn test_with_macro() {
        let counter = Rc::new(Cell::new(0));
        let counter_clone = counter.clone();

        test_exec().spawn(async move {
            counter_clone.set(42);
        });

        test_exec().sleep(Duration::from_millis(10)).await;
        assert_eq!(counter.get(), 42);
    }

    #[apply(async_test test_rt)]
    async fn test_with_macro_explicit_runtime() {
        let counter = Rc::new(Cell::new(0));
        let counter_clone = counter.clone();

        test_exec().spawn(async move {
            counter_clone.set(42);
        });

        test_exec().sleep(Duration::from_millis(10)).await;
        assert_eq!(counter.get(), 42);
    }
}

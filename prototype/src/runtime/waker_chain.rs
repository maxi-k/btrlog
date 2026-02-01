use crate::io::ThreadUring;

use super::future::poll_fn;
///! store wakers on 'the stack', book-keep in a linked list
///! across function stacks
use std::{
    cell::{Cell, UnsafeCell},
    pin::Pin,
    task::{Poll, Waker},
    time::Duration,
};

pub(crate) struct WakerChainNode {
    waker: Cell<Option<Waker>>,
    next: Cell<*mut WakerChainNode>,
    prev: Cell<*mut WakerChainNode>,
}

impl WakerChainNode {
    fn new() -> Self {
        Self {
            waker: Cell::new(None),
            next: Cell::new(std::ptr::null_mut()),
            prev: Cell::new(std::ptr::null_mut()),
        }
    }

    pub(crate) fn wake_self(&self) {
        self.waker.take().map(|w| w.wake());
    }

    pub(crate) fn get_prev(&self) -> *const WakerChainNode {
        self.prev.get()
    }

    fn unlink(&mut self) -> (*mut WakerChainNode, *mut WakerChainNode) {
        let prev = self.prev.get();
        let next = self.next.get();

        if !prev.is_null() {
            unsafe {
                (*prev).next.set(next);
            }
        }
        if !next.is_null() {
            unsafe {
                (*next).prev.set(prev);
            }
        }

        (self.prev.replace(std::ptr::null_mut()), self.next.replace(std::ptr::null_mut()))
    }
}

pub struct WakerList {
    sentinel: UnsafeCell<WakerChainNode>,
    length: Cell<usize>,
}

impl std::fmt::Debug for WakerList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakerList").field("length", &self.length).finish()
    }
}

impl Default for WakerList {
    fn default() -> Self {
        Self::new()
    }
}

impl WakerList {
    pub fn new() -> Self {
        Self {
            sentinel: UnsafeCell::new(WakerChainNode::new()),
            length: Cell::new(0),
        }
    }

    unsafe fn link(&self, node: *mut WakerChainNode) {
        let sentinel = self.sentinel.get();
        self.length.update(|l| l + 1);
        unsafe {
            let old_first = (*sentinel).next.get();

            (*node).prev.set(sentinel);
            (*node).next.set(old_first);
            (*sentinel).next.set(node);

            if !old_first.is_null() {
                (*old_first).prev.set(node);
            }
        }
    }

    pub fn pop_wake_one(&self) -> bool {
        unsafe {
            let sentinel = self.sentinel.get();
            let current = (*sentinel).next.get();

            if !current.is_null() {
                self.length.update(|l| l - 1);
                (*sentinel).next.set((*current).next.get());
                (*current).next.set(std::ptr::null_mut());
                (*current).prev.set(std::ptr::null_mut());
                (*current).wake_self();
                true
            } else {
                false
            }
        }
    }

    pub fn wake_all_pessimistic(&self) -> usize {
        unsafe {
            let mut current = (*self.sentinel.get()).next.get();
            while !current.is_null() {
                (*current).wake_self();
                current = (*current).next.get();
            }
        }
        self.length.get()
    }

    pub fn wake_all(&self) -> usize {
        unsafe {
            let sentinel = self.sentinel.get();
            let mut current = (*sentinel).next.get();

            (*sentinel).next.set(std::ptr::null_mut());

            while !current.is_null() {
                let next = (*current).next.get();
                (*current).wake_self();
                (*current).prev.set(std::ptr::null_mut());
                (*current).next.set(std::ptr::null_mut());
                current = next;
            }
            // XXX enough to do this once at the end if wakers don't
            // immediately execute tasks synchronously (which shouldn't happen?)
            self.length.replace(0)
        }
    }

    pub unsafe fn sentinel(&self) -> *const WakerChainNode {
        self.sentinel.get()
    }

    pub fn len(&self) -> usize {
        self.length.get()
    }

    fn is_first(&self, node: &WakerChainNode) -> bool {
        node.prev.get() == self.sentinel.get()
    }

    fn is_last(&self, node: &WakerChainNode) -> bool {
        !node.prev.get().is_null() && node.next.get().is_null()
    }

    /// wait on the wake chain; return once someone has woken the chain
    pub async fn wait_for_ping(&self) {
        let mut guard = std::pin::pin!(WakerGuard::new(self));
        poll_fn(|ctx| {
            if guard.linked() {
                Poll::Ready(())
            } else {
                guard.as_mut().register(ctx.waker());
                Poll::Pending
            }
        })
        .await
    }
}

pin_project_lite::pin_project! {
pub struct WakerGuard<'a> {
    list: &'a WakerList,
    #[pin] node: WakerChainNode,
}

impl<'a> PinnedDrop for WakerGuard<'a> {
    fn drop(mut this: Pin<&mut Self>) {
        this.unlink();
    }
 }
}

impl<'a> WakerGuard<'a> {
    pub fn new(list: &'a WakerList) -> Self {
        Self {
            list,
            node: WakerChainNode::new(),
        }
    }

    unsafe fn unlink_unchecked(mut self: Pin<&mut Self>) {
        // SAFETY: node is pinned, we have exclusive access
        self.node.unlink();
        self.list.length.update(|l| l - 1);
    }

    pub(crate) fn unlink(self: Pin<&mut Self>) {
        if self.linked() {
            unsafe { self.unlink_unchecked() };
        }
    }

    pub(crate) fn link(mut self: Pin<&mut Self>) {
        if !self.linked() {
            let node_ptr = &mut self.node as *mut WakerChainNode;
            // SAFETY: node is pinned, so its address is stable
            unsafe { self.list.link(node_ptr) };
        }
    }

    pub(crate) unsafe fn get_waker_node(&self) -> *const WakerChainNode {
        debug_assert!(self.linked());
        &self.node as *const WakerChainNode
    }

    pub fn register(self: Pin<&mut Self>, waker: &Waker) {
        self.node.waker.set(Some(waker.clone()));
        self.link();
    }

    pub fn pop_wake_ensure_fifo(self: Pin<&mut Self>) -> bool {
        if !self.am_last() {
            return false;
        }
        // SAFETY: am_last check ensures that (1) we're linked (have prev) and (2) we're last
        unsafe {
            let prev = &*self.node.prev.get();
            self.unlink_unchecked();
            prev.wake_self();
        }
        return true;
    }

    pub fn pop_wake_ensure_lifo(self: Pin<&mut Self>) -> bool {
        if !self.am_first() {
            return false;
        }
        // SAFETY: am_first ensures we're in the list, e.g., prev is set (and = sentinel)
        let opt_next = unsafe { self.node.next.get().as_ref() };
        unsafe {
            self.unlink_unchecked();
        }
        if let Some(next) = opt_next {
            next.wake_self();
        }
        return true;
    }

    pub fn am_first(&self) -> bool {
        self.list.is_first(&self.node)
    }

    pub fn am_last(&self) -> bool {
        self.list.is_last(&self.node)
    }

    fn linked(&self) -> bool {
        // next can be null, but prev is always the sentinel or another node
        !self.node.prev.get().is_null()
    }

    pub async fn wait_for_ping(mut self: Pin<&mut Self>) {
        poll_fn(|ctx| {
            if self.linked() {
                // 2nd poll
                Poll::Ready(())
            } else {
                self.as_mut().register(ctx.waker());
                Poll::Pending
            }
        })
        .await
    }

    pub async fn wait_for<T, F: FnMut() -> Option<T>>(mut self: Pin<&mut Self>, mut pred: F) -> T {
        poll_fn(|ctx| match pred() {
            Some(v) => Poll::Ready(v),
            None => {
                self.as_mut().register(ctx.waker());
                Poll::Pending
            }
        })
        .await
    }

    pub async fn wait_for_condition<F: FnMut() -> bool>(mut self: Pin<&mut Self>, mut pred: F) {
        poll_fn(|ctx| {
            if pred() {
                Poll::Ready(())
            } else {
                self.as_mut().register(ctx.waker());
                Poll::Pending
            }
        })
        .await
    }
}

impl std::fmt::Debug for WakerGuard<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WakerGuard").field("list", &self.list).finish()
    }
}

impl WakerList {
    pub fn create_guard(&self) -> WakerGuard<'_> {
        WakerGuard::new(self)
    }
}

/// either 'yield' if the chain is empty (ala `futures_lite::yield_now`),
/// or put ourselves on the chain if someone else is already yielding
/// return false if we yielded, true if we waited and were woken
pub async fn yield_or_wait(list: &WakerList) -> bool {
    let mut guard = std::pin::pin!(WakerGuard::new(list));
    let am_poller = list.len() == 0;
    let mut second_poll = false;
    poll_fn(|ctx| {
        if second_poll {
            debug_assert!(guard.linked()); // this is true if using wake_all_pessimistic
            // this is the second poll - we're ready
            return Poll::Ready(());
        }
        if am_poller {
            // wake ourselves if we're the poller
            ctx.waker().wake_by_ref();
        }
        // (also) wait on the chain
        guard.as_mut().register(ctx.waker());
        second_poll = true;
        return Poll::Pending;
    })
    .await;
    return am_poller;
}

pub async fn sleep_or_wait(list: &WakerList, io: &ThreadUring, duration: Duration) -> bool {
    let mut guard = std::pin::pin!(WakerGuard::new(list));
    let am_sleeper = list.len() == 0;
    if am_sleeper {
        guard.as_mut().link();
        let _ = io.sleep(duration).await;
        return true;
    } else {
        let mut second_poll = false;
        poll_fn(|ctx| {
            if second_poll {
                return Poll::Ready(());
            }
            guard.as_mut().register(ctx.waker());
            second_poll = true;
            return Poll::Pending;
        })
        .await;
        return false;
    }
}

/// wait on the given wake chain; once woken, check the condition to either
/// return or wait again on the chain
pub async fn wait_for<T, F: FnMut() -> Option<T>>(list: &WakerList, mut pred: F) -> (T, usize) {
    let res = {
        let mut guard = std::pin::pin!(WakerGuard::new(list));
        poll_fn(|ctx| match pred() {
            Some(v) => Poll::Ready(v),
            None => {
                guard.as_mut().register(ctx.waker());
                Poll::Pending
            }
        })
        .await
    };
    (res, list.len())
}

////////////////////////////////////////////////////////////////////////////////

/*
#[cfg(test)]
#[cfg(feature = "uses some v1 features that are annoying to migrate; disabled for now")]
mod test {
    use crate::runtime::*;

    use super::*;
    use std::{rc::Rc, task::Poll, time::Duration};

    struct Something {
        wakers: WakerList,
        value: Cell<usize>,
    }

    impl Something {
        fn new(val: usize) -> Self {
            Self {
                wakers: WakerList::new(),
                value: Cell::new(val),
            }
        }

        async fn do_async_thing(&self, myid: usize) {
            rt().sleep(Duration::from_millis(1)).await;
            println!("{} increasing value", myid);
            self.value.set(self.value.get() + 1);
            self.wakers.wake_all();
        }

        async fn wait_for_async_thing(&self, myid: usize, waiting_for: usize) {
            let guard = WakerGuard::new(&self.wakers);
            // Pin it to the stack - pin_project ensures safety
            let mut guard = std::pin::pin!(guard);

            println!("{} will poll waiting for {}", myid, waiting_for);
            poll_fn(|ctx| {
                println!("{} got polled", myid);
                if self.value.get() == waiting_for {
                    Poll::Ready(())
                } else {
                    guard.as_mut().register(ctx.waker());
                    Poll::Pending
                }
            })
            .await;

            println!("{} finished waiting for {}", myid, waiting_for);
        }
    }

    #[apply(async_test)]
    async fn test_safe_with_pin_project() {
        let thing = Rc::new(Something::new(0));
        let rt = test_exec();

        let t1 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.wait_for_async_thing(1, 1).await;
            }
        });

        let t2 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.wait_for_async_thing(2, 2).await;
            }
        });

        rt.maybe_yield().await;
        rt.maybe_yield().await;

        let t3 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.do_async_thing(100).await;
            }
        });

        // Safe to cancel!
        t2.try_cancel();

        rt.maybe_yield().await;

        let _ = t1.as_future().await;
        let _ = t3.as_future().await;
    }

    #[apply(async_test)]
    fn test_waker_chain_simple() {
        let rt = test_exec();
        let thing = Rc::new(Something::new(0));
        let t1 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.wait_for_async_thing(0, 1).await;
            }
        });

        let t2 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.wait_for_async_thing(1, 1).await;
            }
        });

        rt.maybe_yield().await;
        rt.maybe_yield().await;

        let t3 = crate::rt().spawn({
            let thing = thing.clone();
            async move {
                thing.do_async_thing(100).await;
                thing.do_async_thing(200).await;
            }
        });

        t2.try_cancel(); // Now safe!

        rt.maybe_yield().await;

        println!("awaiting rest");
        let _ = t1.as_future().await;
        let _ = t3.as_future().await;
    }

    #[test]
    #[ignore]
    fn test_forget_creates_dangling_pointer() {
        fn register_and_forget(thing: &Something) {
            // Create a dummy waker
            static VTABLE: std::task::RawWakerVTable =
                std::task::RawWakerVTable::new(|p| std::task::RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
            let waker = unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) };

            // Register a guard on the stack
            let guard = WakerGuard::new(&thing.wakers);
            let mut pinned = std::pin::pin!(guard);
            pinned.as_mut().register(&waker);
            println!("Node address: {:p}", &pinned.node);
            // we can't do that since its been moved into the pin
            // the destructor of the guard actually runs when forgetting `pinned`.
            // thus, dangling pointers are prevented since the user cannot
            // forget the guard (preventing drop) without using unsafe themeselves.
            // ----------------------------------------
            // std::mem::forget(guard);
            // ----------------------------------------
            println!("Exiting scope - stack will be reclaimed");
        }

        let thing = Something::new(0);
        register_and_forget(&thing);

        let _stack_user = [0; 100];

        // The waker list now has a dangling pointer to reclaimed stack memory
        println!("Calling wake_all on dangling pointer... {}", _stack_user[99]);
        thing.wakers.wake_all(); // use-after-free!

        println!("If this prints, we got lucky. Run under Miri to catch the UB.");
    }

    #[test]
    fn test_length_tracking() {
        static VTABLE: std::task::RawWakerVTable =
            std::task::RawWakerVTable::new(|p| std::task::RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {});
        let waker = unsafe { std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE)) };

        let waker_list = WakerList::new();
        assert_eq!(waker_list.len(), 0);

        // Test multiple guards, wake_all, and duplicate registration
        {
            let guard1 = WakerGuard::new(&waker_list);
            let guard2 = WakerGuard::new(&waker_list);
            let mut pinned1 = std::pin::pin!(guard1);
            let mut pinned2 = std::pin::pin!(guard2);

            // Register both guards
            pinned1.as_mut().register(&waker);
            assert_eq!(waker_list.len(), 1);
            pinned2.as_mut().register(&waker);
            assert_eq!(waker_list.len(), 2);

            // Duplicate registration should not increment
            pinned1.as_mut().register(&waker);
            assert_eq!(waker_list.len(), 2);

            // wake_all should reset to 0
            waker_list.wake_all();
            assert_eq!(waker_list.len(), 0);
        }
        assert_eq!(waker_list.len(), 0);
    }

    #[apply(async_test)]
    fn test_length_tracking_async() {
        let rt = test_exec();
        let thing = Rc::new(Something::new(0));
        assert_eq!(thing.wakers.len(), 0);

        // Start two waiting tasks
        let t1 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.wait_for_async_thing(1, 2).await;
            }
        });
        let t2 = rt.spawn({
            let thing = thing.clone();
            async move {
                thing.wait_for_async_thing(2, 2).await;
            }
        });

        rt.maybe_yield().await;
        assert_eq!(thing.wakers.len(), 2); // Both tasks waiting

        // Complete the condition
        thing.do_async_thing(100).await;
        thing.do_async_thing(100).await;

        let _ = t1.as_future().await;
        let _ = t2.as_future().await;
        assert_eq!(thing.wakers.len(), 0); // All done
    }

    ////////////////////////////////////////////////////////////////////////////////
    // this erronous code won't compile now after having introduced pin
    // can we 'assert' that code doesn't compile somehow?

    // #[test]
    // fn test_move_prevented_by_pin_project() {
    //     let thing = Something::new(0);
    //     let mut guard = WaiterGuard::new(&thing.wakers);
    //     let mut pinned = std::pin::pin!(guard);
    //     let moved = pinned; // ERROR: cannot move out of `pinned`
    // }

    // #[test]
    // fn test_move_breaks_safety() {
    //
    //         #[inline(never)]
    //         fn stackframe(thing: &Something) -> Pin<&mut WaiterGuard> {
    //             // Create a dummy waker
    //             static VTABLE: std::task::RawWakerVTable = std::task::RawWakerVTable::new(
    //                 |p| std::task::RawWaker::new(p, &VTABLE), |_| {}, |_| {}, |_| {}
    //             );
    //             let waker = unsafe {
    //                 std::task::Waker::from_raw(std::task::RawWaker::new(std::ptr::null(), &VTABLE))
    //             };
    //
    //             // Step 1: Create guard and register it
    //             let guard1 = WaiterGuard::new(&thing.wakers);
    //             guard1.register(&waker);
    //             let addr1 = &guard1.node as *const _;
    //             println!("Registered node at address: {:p}", addr1);
    //             guard1
    //         }
    //
    //         let thing = Something::new(0);
    //         let guard1 = stackframe(&thing);
    //
    //         // Step 2: Move the guard (without Drop running)
    //         let mut guard2 = std::mem::ManuallyDrop::new(guard1);
    //         // let mut guard3 = std::mem::ManuallyDrop::into_inner(guard2);
    //         let addr2 = &guard2.node as *const _;
    //         println!("After move, node at address: {:p}", addr2);
    //
    //         // Step 3: The linked list still points to addr1, but the node is at addr2!
    //         // wake_all will dereference the stale pointer
    //         println!("Calling wake_all (will access stale pointer)...");
    //         thing.wakers.wake_all(); // Use-after-move
    //
    //         unsafe { std::mem::ManuallyDrop::drop(&mut guard2) };
    // }
}*/

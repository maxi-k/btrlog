use futures_util::task::AtomicWaker;
use std::{
    cell::{Cell, UnsafeCell},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Poll, Waker},
};

pub type PhantomUnsync = PhantomData<std::cell::Cell<()>>;
pub type PhantomUnsend = PhantomData<std::sync::MutexGuard<'static, ()>>;

////////////////////////////////////////////////////////////////////////////////
//  A (highly unsafe) 'registry table' that maps some state
// to some (slot, ticket), as well as some future waker, if desired.
//
// Safe Usage through `TableRef`, which invalidates danling pointers.
// Can only be used from a single thread, which is a big reason for the safety.
#[derive(Debug)]
pub(crate) struct StateTable<Output> {
    // tracked data + unique value to prevent ABA
    refs: UnsafeCell<Vec<(*mut Output, u32)>>,
    // wakers for implementing futures
    wakers: Vec<AtomicWaker>,
    // free list (XXX may be better to just scan the refs vector, 'push' ops are rare)
    free: UnsafeCell<Vec<u16>>,
    // unique counter to prevent ABA problem
    uniq: Cell<u32>,
}

#[derive(PartialEq, Eq)]
pub(crate) enum MapResult<T> {
    Woken(T),
    NotWoken,
    SlotGone,
}
impl<T: std::fmt::Debug> std::fmt::Debug for MapResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Woken(arg0) => f.debug_tuple("Woken").field(arg0).finish(),
            Self::NotWoken => write!(f, "NotWoken"),
            Self::SlotGone => write!(f, "SlotGone"),
        }
    }
}

impl<Output: std::fmt::Debug> StateTable<Output> {
    pub fn new(cap: u16) -> Self {
        StateTable {
            refs: UnsafeCell::new(vec![(std::ptr::null_mut(), 0); cap as usize]),
            wakers: (0..cap).map(|_| AtomicWaker::new()).collect(),
            free: UnsafeCell::new((0u16..cap).collect()),
            uniq: Cell::new(1),
        }
    }

    #[cfg(test)]
    pub fn free_count(&self) -> usize {
        unsafe { &mut *self.free.get() }.len()
    }

    #[cfg(test)]
    pub fn capacity(&self) -> usize {
        unsafe { &mut *self.refs.get() }.len()
    }

    pub fn remove(&self, idx: u16, ticket: u32) {
        let slot = unsafe { self.slot(idx) };
        if slot.1 == ticket {
            *slot = (std::ptr::null_mut(), 0);
            unsafe { &mut *self.free.get() }.push(idx);
            self.drop_waker(idx);
        }
    }

    unsafe fn slot(&self, idx: u16) -> &mut (*mut Output, u32) {
        unsafe { &mut *(&mut *self.refs.get()).as_mut_ptr().add(idx as usize) }
    }

    fn drop_waker(&self, idx: u16) {
        self.wakers[idx as usize].take();
    }

    pub fn register_waker(&self, idx: u16, waker: &Waker) {
        self.wakers[idx as usize].register(waker);
    }

    pub fn try_wake(&self, idx: u16) {
        self.wakers[idx as usize].wake()
    }

    pub fn put<'a, 'b>(self: &'b Self, t: &'a mut Output) -> Option<TableRef<'a, 'b, Output>> {
        let slot = match unsafe { &mut *self.free.get() }.pop() {
            Some(idx) => idx,
            None => return None,
        };
        // https://godbolt.org/z/o9fbY3ajc
        let ticket = self.uniq.replace(self.uniq.get() + 1);
        unsafe { *self.slot(slot) = (t as *mut Output, ticket) };
        let tref = TableRef {
            value: t,
            tbl: self,
            ticket,
            slot,
        };
        Some(tref)
    }

    pub fn map<T, Op: FnOnce(&mut Output) -> Option<T>>(&self, idx: u16, ticket: u32, op: Op) -> MapResult<T> {
        let (ptr, id) = unsafe { self.slot(idx) };
        if *id != ticket {
            return MapResult::SlotGone;
        }
        match unsafe { ptr.as_mut() } {
            Some(r) => match op(r) {
                Some(res) => {
                    self.try_wake(idx);
                    MapResult::Woken(res)
                }
                None => MapResult::NotWoken,
            },
            None => MapResult::SlotGone,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  A Reference to the Registry Table that self-invalidates on Drop

pub(crate) struct TableRef<'a, 'env, State: std::fmt::Debug> {
    value: &'a mut State,
    tbl: &'env StateTable<State>,
    ticket: u32,
    slot: u16,
}

impl<'a, 'env, State: std::fmt::Debug> Deref for TableRef<'a, 'env, State> {
    type Target = State;
    fn deref(&self) -> &Self::Target {
        self.value
    }
}

impl<'a, 'env, State: std::fmt::Debug> DerefMut for TableRef<'a, 'env, State> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.value
    }
}

impl<'a, 'env, T: std::fmt::Debug> Drop for TableRef<'a, 'env, T> {
    fn drop(&mut self) {
        self.tbl.remove(self.slot, self.ticket)
    }
}

impl<'a, 'env, T: std::fmt::Debug> TableRef<'a, 'env, T> {
    pub fn wait_for<'r, R, F: FnMut(&mut T) -> Option<R>>(&'r mut self, check: F) -> AwaitStateRef<'a, 'r, 'env, T, R, F> {
        AwaitStateRef { table_ref: self, check }
    }

    pub fn refids(&self) -> (u16, u32) {
        (self.slot, self.ticket)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Wait for state on table ref

// takes a reference to the table ref
pin_project_lite::pin_project! {
    pub struct AwaitStateRef<'a, 'r, 'env, T: std::fmt::Debug, R, F: FnMut(&mut T) -> Option<R>> {
        #[pin] table_ref: &'r mut TableRef<'a, 'env, T>,
        check: F
    }
}

// owns the table ref
pin_project_lite::pin_project! {
    pub struct AwaitState<'a, 'env, T: std::fmt::Debug, R, F: FnMut(&mut T) -> Option<R>> {
        table_ref: TableRef<'a, 'env, T>,
        check: F
    }
}

impl<'a, 'r, 'env, T: std::fmt::Debug, R, F: FnMut(&mut T) -> Option<R>> Future for AwaitStateRef<'a, 'r, 'env, T, R, F> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        if let Some(res) = (this.check)(this.table_ref.value) {
            return Poll::Ready(res);
        }
        this.table_ref.tbl.register_waker(this.table_ref.slot, cx.waker());
        Poll::Pending
    }
}

impl<'a, 'env, T: std::fmt::Debug, R, F: FnMut(&mut T) -> Option<R>> Future for AwaitState<'a, 'env, T, R, F> {
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();
        if let Some(res) = (this.check)(this.table_ref.value) {
            return Poll::Ready(res);
        }
        this.table_ref.tbl.register_waker(this.table_ref.slot, cx.waker());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::runtime::*;
    use std::{rc::Rc, time::Duration};

    #[derive(Debug)]
    struct MyData {
        x: i32,
    }

    type TestTable = StateTable<MyData>;
    fn mock_operation(d: &mut MyData) -> Option<()> {
        d.x += 1;
        Some(())
    }

    fn other_fn(t: &TestTable) {
        let mut data = MyData { x: 0 };
        // println!("ptr1 {:?}", unsafe { data.as_mut().get_unchecked_mut() as *mut MyData });
        let tref = t.put(&mut data).expect("error adding to table");
        let res = t.map(tref.slot, tref.ticket, mock_operation);
        assert_eq!(res, MapResult::Woken(()));
        assert_eq!(tref.value.x, 1);
        assert_eq!(t.capacity(), 10);
        assert_eq!(t.free_count(), t.capacity() - 1);
    }

    #[test]
    fn test_table_sync() {
        let tbl = StateTable::new(10);
        other_fn(&tbl);
        assert_eq!(tbl.capacity(), 10);
        assert_eq!(tbl.free_count(), 10);
    }

    async fn succeeding_async_fn(t: &Rc<TestTable>) {
        let mut data = MyData { x: 0 };
        // println!("ptr1 {:?}", unsafe { data.as_mut().get_unchecked_mut() as *mut MyData });
        let tref = t.put(&mut data).expect("error adding to table");
        rt().spawn({
            let t = t.clone();
            async move {
                for _ in 0..2 {
                    rt().sleep(Duration::from_millis(10)).await;
                    let res = t.map(tref.slot, tref.ticket, mock_operation);
                    assert_eq!(res, MapResult::Woken(()));
                }
            }
        });
        let mut exp = 2;
        let state = AwaitState {
            table_ref: tref,
            check: |d| {
                if d.x < exp {
                    None
                } else {
                    exp += 1; // can mutate
                    Some(d.x)
                }
            },
        }
        .await;
        assert_eq!(exp, 3);
        assert_eq!(state, 2);
        assert_eq!(t.capacity(), 10);
        assert_eq!(t.free_count(), t.capacity()); // dropped through future
    }

    async fn failing_async_fn(t: &Rc<TestTable>) -> impl Future<Output = ()> {
        let mut data = MyData { x: 0 };
        // println!("ptr1 {:?}", unsafe { data.as_mut().get_unchecked_mut() as *mut MyData });
        let tref = t.put(&mut data).expect("error adding to table");
        let (reschan, h) = local::oneshot::channel();
        rt().spawn({
            let t = t.clone();
            async move {
                for _ in 0..2 {
                    rt().sleep(Duration::from_millis(10)).await;
                    let res = t.map(tref.slot, tref.ticket, mock_operation);
                    assert_eq!(res, MapResult::SlotGone);
                }
                reschan.send(()).expect("error sending result");
            }
        });
        std::mem::drop(tref);
        async move { h.await.expect("error receiving") }
    }

    #[apply(async_test)]
    async fn test_table_nested_async() {
        let tbl = Rc::new(TestTable::new(10));
        // old stack frame
        let res = failing_async_fn(&tbl).await;
        // allocate new stack frame
        succeeding_async_fn(&tbl).await;
        // await old result
        res.await;
        assert_eq!(tbl.capacity(), 10);
        assert_eq!(tbl.free_count(), 10);
    }

    #[test]
    fn test_freelist_usage_and_refill() {
        let tbl = StateTable::new(3);

        // Initially, freelist should have all slots available
        assert_eq!(tbl.free_count(), 3);
        assert_eq!(tbl.capacity(), 3);

        // Add items and check freelist gets consumed
        let mut data1 = MyData { x: 1 };
        let mut data2 = MyData { x: 2 };
        let mut data3 = MyData { x: 3 };

        let ref1 = tbl.put(&mut data1).expect("should have space");
        assert_eq!(tbl.free_count(), 2); // One slot used

        let ref2 = tbl.put(&mut data2).expect("should have space");
        assert_eq!(tbl.free_count(), 1); // Two slots used

        let ref3 = tbl.put(&mut data3).expect("should have space");
        assert_eq!(tbl.free_count(), 0); // All slots used

        // Try to add when full - should fail
        let mut data4 = MyData { x: 4 };
        assert!(tbl.put(&mut data4).is_none());
        assert_eq!(tbl.free_count(), 0); // Still no free slots

        // Remove one item and check freelist gets refilled
        drop(ref2);
        assert_eq!(tbl.free_count(), 1); // One slot freed

        // Should be able to add again
        let ref4 = tbl.put(&mut data4).expect("should have space after freeing");
        assert_eq!(tbl.free_count(), 0); // Used the freed slot

        // Remove all remaining items
        drop(ref1);
        drop(ref3);
        drop(ref4);

        // All slots should be back in freelist
        assert_eq!(tbl.free_count(), 3);
        assert_eq!(tbl.capacity(), 3);
    }

    #[apply(async_test)]
    async fn test_pointer_stability_async() {
        let tbl = Rc::new(TestTable::new(10));

        let (snd, recv) = local::oneshot::channel();
        let (snd1, recv1) = local::oneshot::channel();
        let (snd2, recv2) = local::oneshot::channel();
        let (task_result, task_handle) = local::oneshot::channel();
        let slot = 9;
        let ticket = 1;
        rt().spawn({
            let tbl = tbl.clone();
            async move {
                let mut data = MyData { x: 42 };
                println!("p1 {:?}", (&data as *const MyData));
                let mut tblref = tbl.put(&mut data).expect("error putting");
                println!("p2 {:?}", (tblref.value as *const MyData));
                assert_eq!(tblref.slot, slot);
                assert_eq!(tblref.ticket, ticket);
                snd1.send(slot).expect("send error");
                let val = recv.await.expect("receive error");
                println!("p3 {:?}", (tblref.value as *const MyData));
                assert_eq!(val, 3);
                assert_eq!(tblref.value.x, 43);
                let _ = snd2.send(());
                println!("p3 {:?}", (tblref.value as *const MyData));
                // data.x += val;
                tblref.wait_for(|a| if a.x > 50 { Some(()) } else { None }).await;
                task_result.send(tblref.value.x).expect("error sending task reply");
            }
        });

        let slot = recv1.await.expect("error receiving");
        let res = tbl.map(slot, ticket, |data| {
            data.x += 1;
            Some(())
        });
        assert_eq!(res, MapResult::Woken(()));

        snd.send(3).expect("error sending");
        recv2.await.expect("error receiving");

        let res = tbl.map(slot, ticket, |data| {
            data.x += 10;
            Some(())
        });
        assert_eq!(res, MapResult::Woken(()));

        let final_value = task_handle.await;
        assert!(matches!(final_value, Ok(53)));

        assert_eq!(tbl.capacity(), 10);
        assert_eq!(tbl.free_count(), 10);
    }
}

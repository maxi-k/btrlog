// XXX decrease code duplication?
use std::{
    marker::PhantomData,
    sync::Arc,
    task::{RawWaker, RawWakerVTable, Waker},
};

pub trait ArcWaker {
    type Handler;
    fn on_wake(handler: Arc<Self::Handler>, tag: u16);
    fn on_wake_by_ref(handler: &Arc<Self::Handler>, tag: u16);
    fn on_drop_waker(_handler: Arc<Self::Handler>, _tag: u16) {
        // this will auto-decrease the strong count
        // XXX when to remove task?
    }

    fn new_arc_waker(handler: &Arc<Self::Handler>, tag: u16) -> Waker
    where
        Self: Sized,
    {
        TaggingWaker::<Self>::new_waker(handler, tag)
    }

    fn set_tag(waker: &mut Waker, tag: u16)
    where
        Self: Sized,
    {
        *waker = unsafe { Waker::new(TaggingWaker::<Self>::change_tag(waker.data(), tag), waker.vtable()) }
    }
}

struct TaggingWaker<W: ArcWaker> {
    _data: PhantomData<W>,
}

impl<W: ArcWaker> TaggingWaker<W> {
    const WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::vtable_clone, Self::vtable_wake, Self::vtable_wake_by_ref, Self::vtable_drop);

    #[inline]
    unsafe fn new_raw_waker(inner: &Arc<W::Handler>, tag: u16) -> RawWaker {
        RawWaker::new(unsafe { Self::leak_tagged_ptr(inner.clone(), tag) }, &Self::WAKER_VTABLE)
    }

    #[inline]
    fn new_waker(inner: &Arc<W::Handler>, tag: u16) -> Waker {
        unsafe { Waker::from_raw(Self::new_raw_waker(inner, tag)) }
    }

    #[inline]
    unsafe fn leak_tagged_ptr(inner: Arc<W::Handler>, tag: u16) -> *const () {
        let ptr = Arc::into_raw(inner) as *const () as usize;
        debug_assert_eq!(ptr >> 48, 0usize);
        (ptr << 16 | (tag as usize)) as *const ()
    }

    #[inline]
    unsafe fn from_tagged_ptr(ptr: *const ()) -> (Arc<W::Handler>, u16) {
        let rc = unsafe { Arc::<W::Handler>::from_raw((ptr as usize >> 16) as *const W::Handler) };
        let tag = (ptr as usize) & (0xFFFF);
        (rc, tag as u16)
    }

    #[inline]
    fn change_tag(ptr: *const (), newtag: u16) -> *const () {
        ((ptr as usize) << 16 | newtag as usize) as *const ()
    }

    unsafe fn vtable_clone(tagged: *const ()) -> RawWaker {
        unsafe {
            // need to create new Arc to increase the reference count
            Arc::<W::Handler>::increment_strong_count((tagged as usize >> 16) as *const W::Handler);
            RawWaker::new(tagged, &Self::WAKER_VTABLE)
        }
    }

    unsafe fn vtable_wake(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_wake(inst, tag);
    }

    unsafe fn vtable_wake_by_ref(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_wake_by_ref(&inst, tag);
        let _ = Arc::<W::Handler>::into_raw(inst); // forget Arc again
    }

    unsafe fn vtable_drop(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_drop_waker(inst, tag);
    }
}

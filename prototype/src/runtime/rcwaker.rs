use std::{
    marker::PhantomData,
    rc::Rc,
    task::{RawWaker, RawWakerVTable, Waker},
};

pub trait RcWaker {
    type Handler;
    fn on_wake(handler: Rc<Self::Handler>, tag: u16);
    fn on_wake_by_ref(handler: &Rc<Self::Handler>, tag: u16);
    fn on_drop_waker(_handler: Rc<Self::Handler>, _tag: u16) {
        // this will auto-decrease the strong count
        // XXX when to remove task?
    }

    fn new_rc_waker(handler: &Rc<Self::Handler>, tag: u16) -> Waker
    where
        Self: Sized,
    {
        TaggingWaker::<Self>::new_waker(handler, tag)
    }
}

struct TaggingWaker<W: RcWaker> {
    _data: PhantomData<W>,
}

impl<W: RcWaker> TaggingWaker<W> {
    const WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::vtable_clone, Self::vtable_wake, Self::vtable_wake_by_ref, Self::vtable_drop);

    #[inline]
    unsafe fn new_raw_waker(inner: &Rc<W::Handler>, tag: u16) -> RawWaker {
        RawWaker::new(unsafe { Self::leak_tagged_ptr(inner.clone(), tag) }, &Self::WAKER_VTABLE)
    }

    #[inline]
    fn new_waker(inner: &Rc<W::Handler>, tag: u16) -> Waker {
        unsafe { Waker::from_raw(Self::new_raw_waker(inner, tag)) }
    }

    #[inline]
    unsafe fn leak_tagged_ptr(inner: Rc<W::Handler>, tag: u16) -> *const () {
        let ptr = Rc::into_raw(inner) as *const () as usize;
        debug_assert_eq!(ptr >> 48, 0usize);
        (ptr << 16 | (tag as usize)) as *const ()
    }

    #[inline]
    unsafe fn from_tagged_ptr(ptr: *const ()) -> (Rc<W::Handler>, u16) {
        let rc = unsafe { Rc::<W::Handler>::from_raw((ptr as usize >> 16) as *const W::Handler) };
        let tag = (ptr as usize) & (0xFFFF);
        (rc, tag as u16)
    }

    unsafe fn vtable_clone(tagged: *const ()) -> RawWaker {
        unsafe {
            // need to create new Rc to increase the reference count
            Rc::<W::Handler>::increment_strong_count((tagged as usize >> 16) as *const W::Handler);
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
        let _ = Rc::<W::Handler>::into_raw(inst); // forget Rc again
    }

    unsafe fn vtable_drop(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_drop_waker(inst, tag);
    }
}

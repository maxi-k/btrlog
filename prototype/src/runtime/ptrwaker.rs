// XXX decrease code duplication?
use std::{
    marker::PhantomData,
    task::{RawWaker, RawWakerVTable, Waker},
};

pub trait PtrWaker {
    type Handler: 'static;
    fn on_wake(handler: &Self::Handler, tag: u16);
    fn on_wake_by_ref(handler: &Self::Handler, tag: u16);
    fn on_drop_waker(_handler: &Self::Handler, _tag: u16) {
        // this will auto-decrease the strong count
        // XXX when to remove task?
    }

    unsafe fn new_ptr_waker(handler: &Self::Handler, tag: u16) -> Waker
    where
        Self: Sized,
    {
        unsafe { TaggingWaker::<Self>::new_waker(handler, tag) }
    }

    fn set_tag(waker: &mut Waker, tag: u16)
    where
        Self: Sized,
    {
        *waker = unsafe { Waker::new(TaggingWaker::<Self>::change_tag(waker.data(), tag), waker.vtable()) }
    }
}

struct TaggingWaker<W: PtrWaker> {
    _data: PhantomData<W>,
}

impl<W: PtrWaker> TaggingWaker<W> {
    const WAKER_VTABLE: RawWakerVTable =
        RawWakerVTable::new(Self::vtable_clone, Self::vtable_wake, Self::vtable_wake_by_ref, Self::vtable_drop);

    #[inline]
    unsafe fn new_raw_waker(inner: &W::Handler, tag: u16) -> RawWaker {
        RawWaker::new(unsafe { Self::leak_tagged_ptr(inner, tag) }, &Self::WAKER_VTABLE)
    }

    #[inline]
    unsafe fn new_waker(inner: &W::Handler, tag: u16) -> Waker {
        unsafe { Waker::from_raw(Self::new_raw_waker(inner, tag)) }
    }

    #[inline]
    unsafe fn leak_tagged_ptr(inner: &W::Handler, tag: u16) -> *const () {
        let ptr = (inner as *const W::Handler) as *const () as usize;
        debug_assert_eq!(ptr >> 48, 0usize);
        (ptr << 16 | (tag as usize)) as *const ()
    }

    #[inline] // this is so unsafe it's not even funny
    unsafe fn from_tagged_ptr(ptr: *const ()) -> (&'static W::Handler, u16) {
        let refr = unsafe { ((ptr as usize >> 16) as *const W::Handler).as_ref().unwrap_unchecked() };
        let tag = (ptr as usize) & (0xFFFF);
        (refr, tag as u16)
    }

    #[inline]
    fn change_tag(ptr: *const (), newtag: u16) -> *const () {
        ((ptr as usize) << 16 | newtag as usize) as *const ()
    }

    unsafe fn vtable_clone(tagged: *const ()) -> RawWaker {
        RawWaker::new(tagged, &Self::WAKER_VTABLE)
    }

    unsafe fn vtable_wake(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_wake(inst, tag);
    }

    unsafe fn vtable_wake_by_ref(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_wake_by_ref(&inst, tag);
    }

    unsafe fn vtable_drop(tagged: *const ()) {
        let (inst, tag) = unsafe { Self::from_tagged_ptr(tagged) };
        W::on_drop_waker(inst, tag);
    }
}

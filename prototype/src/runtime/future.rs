use std::{
    pin::Pin,
    task::{Context, Poll},
};

////////////////////////////////////////////////////////////////////////////////
//  Re-exports

pub use futures_util::future::{join, join_all, poll_fn, select};
use more_asserts::debug_assert_ge;

use crate::io::{ThreadBuffers, buffer::IoBuffer};

////////////////////////////////////////////////////////////////////////////////
//  Yielding

pub fn maybe_yield() -> MaybeYield {
    MaybeYield { polled_once: false }
}

pub struct MaybeYield {
    polled_once: bool,
}

impl Future for MaybeYield {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if !self.polled_once {
            self.polled_once = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Reify future

pub(crate) type HeapFuture<Fut> = Pin<IoBuffer<Fut>>;

#[inline]
pub(crate) fn allocate_heap_future<Fut: 'static>(alloc: &ThreadBuffers, fut: Fut) -> HeapFuture<Fut> {
    debug_assert_ge!(alloc.buffer_size(), std::mem::size_of::<Fut>());
    IoBuffer::new_init(alloc.clone(), |mem| {
        mem.write(fut);
    })
    .into_pin()
}

crate::util::ifcfg! {
    [debug_assertions]

    use reqtrace::MicrosecondMeasurement;

    pub(crate) struct TrackDrop<'a> {
        id: &'a str,
        timer: MicrosecondMeasurement,
    }
    impl<'a> TrackDrop<'a> {
        pub(crate) fn new(id: &'a str) -> Self {
            let timer = MicrosecondMeasurement::new_started();
            log::info!("starting request {}", id);
            Self { id, timer }
        }
    }
    impl<'a> Drop for TrackDrop<'a> {
        fn drop(&mut self) {
            log::info!("dropping request {} after {}us", self.id, self.timer.stop_measurement());
        }
    }
}

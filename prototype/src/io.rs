pub mod buffer;
pub mod send_buffer;
pub mod uring;
pub mod watermark;

use std::{cell::OnceCell, rc::Rc};

pub use buffer::{BufferPoolConfig, ThreadBuffers};
pub use uring::{ThreadUring, UringConfig};

////////////////////////////////////////////////////////////////////////////////
//  thread local buffer interface

thread_local! {
    static IO_BUFFERS: [OnceCell<ThreadBuffers>; 64] = [const { OnceCell::new() }; 64];
}

#[inline]
pub fn local_buffer_pool(size: usize, cfg: BufferPoolConfig) -> ThreadBuffers {
    let idx = if size.is_power_of_two() {
        size.ilog2()
    } else {
        size.ilog2() + 1
    };
    IO_BUFFERS.with(|bufs| bufs[idx as usize].get_or_init(move || Rc::new(buffer::LocalBufferPool::new(1 << idx, cfg))).clone())
}

#[inline]
pub fn local_buffer_pool_size_ceil(size: usize, cfg: BufferPoolConfig) -> ThreadBuffers {
    let idx = if size.is_power_of_two() {
        size.ilog2()
    } else {
        size.ilog2() - 1
    };
    IO_BUFFERS.with(|bufs| bufs[idx as usize].get_or_init(move || Rc::new(buffer::LocalBufferPool::new(1 << idx, cfg))).clone())
}

pub static MAX_PACKET_SIZE: usize = 1 << 16;
#[inline]
pub fn local_packet_buffer_pool() -> ThreadBuffers {
    local_buffer_pool(
        MAX_PACKET_SIZE,
        BufferPoolConfig::build_with(|cfg| {
            cfg.prealloc_size = MAX_PACKET_SIZE * 4096;
        }),
    )
}

////////////////////////////////////////////////////////////////////////////////
//  thread local I/O interface

crate::util::scoped_tls!(static THREAD_URING: ThreadUring);

pub unsafe fn provide_thread_io(io: std::pin::Pin<&ThreadUring>) -> crate::util::TLSResetRAII {
    unsafe { THREAD_URING.bind_pin(io) }
}

pub fn with_thread_io<T, F: FnOnce() -> T>(io: &ThreadUring, f: F) -> T {
    THREAD_URING.set(io, f)
}

#[inline]
pub fn thread_io() -> ThreadUring {
    THREAD_URING.with(|io| io.clone())
}

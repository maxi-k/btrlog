use std::cell::OnceCell;

use crate::runtime::sync;

use super::{buffer::IoBuffer, local_buffer_pool};

struct BufferIdentifier(*mut u8, usize);
// XXX bound?
type DeallocSend = sync::mpsc::UnboundedSender<BufferIdentifier>;
thread_local! {
    static DEALLOCATORS: OnceCell<DeallocSend> = OnceCell::new();
}

fn ensure_local_deallocator() -> DeallocSend {
    DEALLOCATORS.with(|d| {
        d.get_or_init(|| {
            let (snd, mut recv) = sync::mpsc::unbounded_channel::<BufferIdentifier>();
            let rt = crate::runtime::rt();
            rt.spawn(async move {
                while let Some(to_dealloc) = crate::runtime::rt().with_sync_waker(recv.recv()).await {
                    let pool = local_buffer_pool(to_dealloc.1, Default::default());
                    pool.push_raw(to_dealloc.0);
                }
            });
            snd
        })
        .clone()
    })
}

pub(crate) struct SendableIOBuffer {
    mem: *mut u8,
    size: usize,
    trunc: usize,
    dealloc: DeallocSend,
}

impl SendableIOBuffer {
    pub(crate) fn new(local: IoBuffer<u8>) -> Self {
        let raw = local.ptr_mut();
        let size = local.capacity();
        let trunc = local.used_bytes();
        // don't auto-dealloc to the local buffer pool
        std::mem::forget(local);
        let dealloc = ensure_local_deallocator();
        Self {
            mem: raw,
            size,
            trunc,
            dealloc,
        }
    }

    pub(crate) fn into_bytes(self) -> bytes::Bytes {
        bytes::Bytes::from_owner(self)
    }
}
unsafe impl Send for SendableIOBuffer {}

// for bytes::Bytes::from_owner<T>(owner: T) -> Self where T: AsRef<[u8]> + Send + 'static
impl AsRef<[u8]> for SendableIOBuffer {
    fn as_ref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.mem, self.trunc) }
    }
}

impl Drop for SendableIOBuffer {
    fn drop(&mut self) {
        self.dealloc
            .send(BufferIdentifier(self.mem, self.size))
            .expect("error while deallocating SendIOBuffer");
    }
}

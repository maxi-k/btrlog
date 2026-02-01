use std::{
    cell::RefCell,
    mem::MaybeUninit,
    rc::Rc,
    sync::atomic::{AtomicUsize, Ordering},
};

////////////////////////////////////////////////////////////////////////////////
//  Config

pub struct BufferPoolConfig {
    pub vm_size: usize,
    pub prealloc_size: usize,
    pub hugepage: bool,
}

impl BufferPoolConfig {
    crate::util::integrated_builder!();
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            vm_size: 1024 * 1024 * 1024 * 8,
            prealloc_size: 0,
            hugepage: true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Buffer Pool

pub type ThreadBuffers = Rc<LocalBufferPool>;

#[derive(Debug)]
pub struct LocalBufferPool {
    alloc_size: usize,
    page_size: usize,
    vm: *mut u8,
    issued: AtomicUsize,
    freelist: RefCell<Vec<*mut u8>>,
}

impl LocalBufferPool {
    pub(crate) fn new(pagesize: usize, cfg: BufferPoolConfig) -> Self {
        // assert!(pagesize.is_power_of_two());
        use libc::{MADV_HUGEPAGE, MAP_ANONYMOUS, MAP_PRIVATE, PROT_READ, PROT_WRITE};
        let vm = unsafe {
            let vm = libc::mmap(core::ptr::null_mut(), cfg.vm_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
            if vm == libc::MAP_FAILED {
                panic!("mmapping thread buffers failed with {}", std::io::Error::last_os_error());
            }
            if cfg.hugepage {
                if libc::madvise(vm, cfg.vm_size, MADV_HUGEPAGE) < 0 {
                    panic!("madvise failed with {}", std::io::Error::last_os_error())
                }
            }
            if cfg.prealloc_size > 0 {
                libc::memset(vm, 0, cfg.prealloc_size);
            }
            vm as *mut u8
        };
        Self {
            alloc_size: cfg.vm_size,
            page_size: pagesize,
            vm,
            issued: 0.into(),
            freelist: Vec::new().into(),
        }
    }

    fn drop_inner(&mut self) {
        let res = unsafe { libc::munmap(self.vm as *mut libc::c_void, self.alloc_size) };
        if res < 0 {
            let err = std::io::Error::last_os_error();
            panic!("munmap failed {}", err);
        }
    }

    #[cfg(test)]
    fn free_count(&self) -> usize {
        self.freelist.borrow().len()
    }

    pub unsafe fn issued_range(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.vm, self.page_size * self.issued.load(Ordering::Relaxed)) }
    }

    pub fn issued(&self) -> usize {
        self.issued.load(Ordering::Relaxed)
    }

    pub fn vm_start(&self) -> *mut u8 {
        self.vm
    }

    pub unsafe fn pop_raw(&self) -> *mut u8 {
        if self.freelist.borrow().is_empty() {
            let offset = self.issued.fetch_add(self.page_size, Ordering::Relaxed);
            unsafe { self.vm.add(offset) }
        } else {
            self.freelist.borrow_mut().pop().expect("Freelist empty")
        }
    }

    pub fn push_raw(&self, ptr: *mut u8) {
        debug_assert!(ptr.ge(&self.vm)); // ptr originates from here
        debug_assert!(unsafe { self.vm.add(self.alloc_size).gt(&ptr) });
        self.freelist.borrow_mut().push(ptr);
    }

    pub fn pop(self: &Rc<Self>) -> IoBuffer {
        IoBuffer::new(self.clone())
    }

    pub fn iter<'a>(self: &'a Rc<Self>) -> ThreadBuffersIter<'a> {
        ThreadBuffersIter(self)
    }

    pub(crate) fn buffer_size(&self) -> usize {
        self.page_size
    }
}

impl Drop for LocalBufferPool {
    fn drop(&mut self) {
        self.drop_inner();
    }
}

pub struct ThreadBuffersIter<'a>(&'a Rc<LocalBufferPool>);
impl<'a> Iterator for ThreadBuffersIter<'a> {
    type Item = IoBuffer;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.0.pop())
    }
}

////////////////////////////////////////////////////////////////////////////////
//  safe thread-local io buffer interface

#[derive(Debug)]
pub struct IoBuffer<T = u8> {
    origin: Rc<LocalBufferPool>,
    data: *mut T,
    trunc: usize,
}
pub type IoBuf = super::buffer::IoBuffer<u8>;

impl<T> IoBuffer<T> {
    pub fn new_init<F: FnOnce(&mut MaybeUninit<T>)>(bufs: Rc<LocalBufferPool>, init: F) -> Self {
        debug_assert_eq!(0, bufs.page_size % std::mem::align_of::<T>());
        debug_assert!(bufs.page_size >= std::mem::size_of::<T>());
        // SAFETY: pop_raw() never returns nullptr
        let data = unsafe { &mut *(bufs.pop_raw() as *mut MaybeUninit<T>) };
        debug_assert!(data.as_ptr().is_aligned());
        init(data); // SAFETY: data is now initialized
        let trunc = bufs.page_size;
        Self {
            origin: bufs,
            data: data.as_mut_ptr(),
            trunc,
        }
    }

    pub fn try_new_init<E, F>(bufs: Rc<LocalBufferPool>, init: F) -> Result<Self, E>
    where
        F: FnOnce(&mut MaybeUninit<T>) -> Result<(), E>,
    {
        let data = unsafe { &mut *(bufs.pop_raw() as *mut MaybeUninit<T>) };
        let trunc = bufs.page_size;
        match init(data) {
            Ok(_) => Ok(Self {
                origin: bufs,
                data: data.as_mut_ptr(),
                trunc,
            }),
            Err(e) => Err(e),
        }
    }

    unsafe fn return_to_origin(&mut self) {
        unsafe { self.get_origin().push_raw(self.type_erased_ptr()) }
    }

    pub(crate) unsafe fn type_erased_ptr(&self) -> *mut u8 {
        self.data as *mut u8
    }

    pub fn get_origin(&self) -> &LocalBufferPool {
        self.origin.as_ref()
    }

    pub fn used_bytes(&self) -> usize {
        self.trunc
    }

    pub fn mark_used(&mut self, size: usize) {
        debug_assert!(size <= self.capacity());
        self.trunc = size;
    }

    pub fn mark_all_used(&mut self) {
        self.mark_used(self.capacity())
    }

    pub fn capacity(&self) -> usize {
        self.get_origin().page_size
    }

    pub fn as_ref(&self) -> &T {
        // SAFETY: constructor guarantees that data behind pointer is initialized
        unsafe { &*self.data }
    }

    pub fn as_mut_ref(&self) -> &mut T {
        // SAFETY: constructor guarantees that data behind pointer is initialized
        unsafe { &mut *self.data }
    }

    /// Returns a pointer just behind the object, and a pointer
    /// to the end of the memory managed by this buffer.
    #[inline]
    pub fn heap_ptr(&self) -> (*mut u8, *mut u8) {
        let memsize = self.origin.page_size;
        let objsize = std::mem::size_of::<T>();
        debug_assert!(memsize > objsize);
        // SAFETY the returned pointers point to memory managed by this buffer
        unsafe { (self.type_erased_ptr().add(objsize), self.type_erased_ptr().add(memsize)) }
    }

    /// Returns the slice of memory *behind* the object that is
    /// still managed by this buffer.
    #[inline]
    pub fn heap(&self) -> &mut [u8] {
        let memsize = self.origin.page_size;
        let objsize = std::mem::size_of::<T>();
        debug_assert!(memsize > objsize);
        // SAFETY: the returned data is managed by this buffer
        unsafe { std::slice::from_raw_parts_mut(self.type_erased_ptr().add(objsize), memsize - objsize) }
    }

    #[inline]
    pub const fn header_object_size(&self) -> usize {
        std::mem::size_of::<T>()
    }

    #[inline]
    pub fn type_erase_dropping(self) -> IoBuffer {
        unsafe {
            if std::mem::needs_drop::<T>() {
                std::ptr::drop_in_place(self.data);
            }
            std::mem::transmute::<Self, IoBuffer>(self)
        }
    }

    #[inline]
    pub unsafe fn transmute<O>(self) -> IoBuffer<O> {
        unsafe { std::mem::transmute::<Self, IoBuffer<O>>(self) }
    }

    pub fn into_pin(self) -> std::pin::Pin<Self>
    where
        T: 'static,
    {
        // SAFETY: the data pointer ain't movin'
        unsafe { std::pin::Pin::new_unchecked(self) }
    }
}

impl<T> Drop for IoBuffer<T> {
    fn drop(&mut self) {
        if self.data.is_null() {
            return;
        }
        if std::mem::needs_drop::<T>() {
            unsafe { std::ptr::drop_in_place(self.data) }
        }
        unsafe { self.return_to_origin() };
        self.data = std::ptr::null_mut();
        self.trunc = 0;
    }
}

impl<T> std::ops::Deref for IoBuffer<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl<T> std::ops::DerefMut for IoBuffer<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_ref()
    }
}

impl IoBuffer<u8> {
    pub fn new(bufs: Rc<LocalBufferPool>) -> Self {
        let data = unsafe { bufs.pop_raw() };
        let trunc = bufs.page_size;
        Self {
            origin: bufs,
            data,
            trunc,
        }
    }

    pub fn take(&mut self) -> Self {
        Self {
            origin: self.origin.clone(),
            data: std::mem::replace(&mut self.data, std::ptr::null_mut()),
            trunc: self.trunc,
        }
    }

    pub fn ptr(&self) -> *const u8 {
        self.data
    }

    pub fn ptr_mut(&self) -> *mut u8 {
        self.data
    }

    pub fn data(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.used_bytes()) }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.data()
    }

    pub fn data_mut(&self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.used_bytes()) }
    }

    pub fn as_mut_slice(&self) -> &mut [u8] {
        self.data_mut()
    }
}

impl AsRef<[u8]> for IoBuffer<u8> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Clone for IoBuffer<u8> {
    fn clone(&self) -> Self {
        let mut newbuf = self.origin.pop();
        unsafe { std::ptr::copy_nonoverlapping(self.data, newbuf.data, self.trunc) };
        newbuf.mark_used(self.trunc);
        newbuf
    }
}

////////////////////////////////////////////////////////////////////////////////
//  Tests

#[cfg(test)]
mod tests {
    use crate::io::local_buffer_pool;

    #[test]
    fn test_get_local_buffer() {
        let buf = local_buffer_pool(64, Default::default());
        assert_eq!(buf.page_size, 64);
        let buf2 = local_buffer_pool(60, Default::default());
        assert_eq!(buf2.page_size, 64);
    }

    #[test]
    fn test_pop() {
        let buf = local_buffer_pool(64, Default::default());
        let ptr = unsafe { buf.pop_raw() };
        assert_eq!(ptr, buf.vm);
    }

    #[test]
    fn test_buffer_reuse() {
        let buf = local_buffer_pool(64, Default::default());
        let ptr1 = unsafe { buf.pop_raw() };
        assert_eq!(ptr1, buf.vm);
        buf.push_raw(ptr1);
        assert_eq!(1, buf.free_count());
        let ptr2 = unsafe { buf.pop_raw() };
        assert_eq!(ptr1, ptr2);
        assert_eq!(0, buf.free_count());
    }

    #[test]
    fn test_lazy_alloc() {
        // https://tenor.com/view/yuge-huge-donald-trump-gif-7357158
        let _buf = local_buffer_pool(1 << 62, Default::default());
    }

    #[test]
    fn test_io_buffers() {
        let pool = local_buffer_pool(4096, Default::default());
        {
            let buf = pool.pop();
            assert_eq!(buf.data().len(), 4096);
        } // drop
        assert_eq!(pool.free_count(), 1);
    }
}

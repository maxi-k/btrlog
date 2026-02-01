use io_uring::{opcode, types};
use smallvec::smallvec;

use crate::runtime::rcwaker::RcWaker;

use super::ThreadBuffers;
use super::buffer::IoBuf;
use super::uring::{IOEnterIntent, OpCompletionAction, OpId, OpUid, ThreadUring};
use std::{
    cell::{Cell, UnsafeCell},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    rc::Rc,
};

pub trait PacketConsumer {
    fn consume_raw(&self, _result: i32, _from: SocketAddr, _buf: IoBuf, _id: OpId) {}
}

type StateVec<T> = smallvec::SmallVec<[T; 32]>;

struct WatermarkState {
    lookup: Vec<u16>,
    free: StateVec<u16>,
    opuid: StateVec<OpUid>,
    data: StateVec<libc::msghdr>,
    msgname: StateVec<libc::sockaddr_in>,
    iov: StateVec<libc::iovec>,
}

impl WatermarkState {
    fn new(iodepth: u32, watermark: u16) -> Self {
        let mut free = StateVec::with_capacity(watermark.into());
        for i in 0..watermark {
            free.push(watermark - 1 - i);
        }
        Self {
            lookup: vec![0xFFFF as u16; iodepth as usize],
            free,
            opuid: smallvec![0; watermark as usize],
            data: smallvec![Self::empty_msghdr(); watermark as usize],
            msgname: smallvec![Self::empty_addr(); watermark as usize],
            iov: smallvec![Self::empty_iovec(); watermark as usize],
        }
    }

    #[inline]
    fn at(&mut self, slot: u16) -> Option<(SocketAddr, OpUid, &mut libc::iovec)> {
        let idx = self.lookup[slot as usize] as usize;
        if idx == 0xFFFF {
            return None;
        }
        let sockaddr = &self.msgname[idx];
        let ip = IpAddr::V4(Ipv4Addr::from(u32::from_be(sockaddr.sin_addr.s_addr)));
        let from = ((ip, u16::from_be(sockaddr.sin_port))).into();
        Some((from, self.opuid[idx], &mut self.iov[idx]))
    }

    fn prepare_new_message_slot(&mut self, opid: OpId) -> Option<usize> {
        let slot = opid.slot() as usize;
        let idx = self.free.pop()?;
        debug_assert_eq!(self.lookup[slot], 0xFFFF);
        self.lookup[slot] = idx;
        self.opuid[idx as usize] = opid.uid();
        Some(idx as usize)
    }

    #[inline]
    fn set_msg_buffer(&mut self, op: OpId, buf: &mut IoBuf) -> Option<*mut libc::msghdr> {
        let idx = self.lookup[op.slot() as usize] as usize;
        if idx == 0xFFFF || self.opuid[idx] != op.uid() {
            None
        } else {
            Some(self.init_internal_msg(idx, op.uid(), buf))
        }
    }

    #[inline]
    #[allow(dead_code)]
    fn get_op_uid(&self, slot: u16) -> Option<OpUid> {
        let idx = self.lookup[slot as usize] as usize;
        match idx {
            0xFFFF => None,
            _ => match self.opuid[idx] {
                0 => None,
                x => Some(x),
            },
        }
    }

    fn free_slot(&mut self, slot: u16) {
        let idx = &mut self.lookup[slot as usize];
        self.opuid[*idx as usize] = 0;
        *idx = 0xFFFF;
    }

    // XXX most of these don't change; could move to constructor, only set
    // buffer and reset address
    fn init_internal_msg(&mut self, idx: usize, uid: OpUid, buf: &mut IoBuf) -> &mut libc::msghdr {
        self.opuid[idx] = uid; // do bounds check once, then access without bounds check below
        let data = unsafe { self.data.get_unchecked_mut(idx) };
        let iov = unsafe { self.iov.get_unchecked_mut(idx) };
        let msgname = unsafe { self.msgname.get_unchecked_mut(idx) };
        // prepare buffer info
        iov.iov_base = buf.ptr_mut() as *mut libc::c_void;
        iov.iov_len = buf.used_bytes();
        // prepare data
        *msgname = Self::empty_addr();
        data.msg_name = msgname as *mut libc::sockaddr_in as *mut libc::c_void;
        data.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;
        data.msg_iov = iov;
        data.msg_iovlen = 1;
        data
    }

    const fn empty_msghdr() -> libc::msghdr {
        libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as u32,
            msg_iov: std::ptr::null_mut(),
            msg_iovlen: 0,
            msg_control: std::ptr::null_mut(),
            msg_controllen: 0,
            msg_flags: 0,
        }
    }

    const fn empty_addr() -> libc::sockaddr_in {
        libc::sockaddr_in {
            sin_family: libc::AF_INET as u16,
            sin_port: 0,
            sin_addr: libc::in_addr { s_addr: 0u32 },
            sin_zero: [0u8; 8],
        }
    }

    const fn empty_iovec() -> libc::iovec {
        libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        }
    }
}

/// tries to keep a configurable number of recv calls open at the same time
pub struct WatermarkRecv<Consumer> {
    socket: std::os::fd::RawFd,
    io: ThreadUring,
    state: UnsafeCell<WatermarkState>,
    watermark: u16,
    wind_down: Cell<bool>,
    bufpool: ThreadBuffers,
    consumer: Consumer,
}

impl<Consumer: PacketConsumer> WatermarkRecv<Consumer> {
    pub fn new(sock: impl std::os::fd::AsRawFd, io: ThreadUring, watermark: u16, consumer: Consumer) -> Rc<Self> {
        let iodepth = io.iodepth();
        let res = Rc::new(Self {
            socket: sock.as_raw_fd(),
            io,
            state: UnsafeCell::new(WatermarkState::new(iodepth as u32, watermark)),
            watermark,
            wind_down: Cell::new(false),
            bufpool: crate::io::local_packet_buffer_pool(),
            //bufpool: crate::io::local_io_buffer_pool(1 << 10),
            consumer,
        });
        for _ in 0..watermark {
            res.enqueue_new_recv_op();
        }
        res.io
            .access(|ctx| ctx.enter(IOEnterIntent::Submit))
            .expect("error during initial watermark submit");
        res
    }

    pub fn active_op_count(&self) -> usize {
        let inactive = self.access(|state| state.free.len());
        self.watermark as usize - inactive
    }

    pub fn start_winding_down(&self) {
        // TODO cancel open recv calls
        self.wind_down.set(true);
    }

    #[inline]
    fn access<T, F: FnOnce(&mut WatermarkState) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.state.get() })
    }

    fn enqueue_new_recv_op(self: &Rc<Self>) {
        let _op_id = self.io.access(|context| {
            let buf = self.bufpool.pop();
            // Allocate a slot and get op_id
            let (op_id, buf) = context.register_buffer_op(buf).expect("No free slots available");
            self.access(|state| state.prepare_new_message_slot(op_id));
            match self.submit_recv_op(op_id, buf) {
                Ok(_) => {}
                Err(_e) => {
                    self.access(|s| s.free_slot(op_id.slot()));
                    panic!("error during initial watermark submit: {:?}", _e);
                }
            }
            // XXX make better?
            let _waker = context.register_waker(op_id, Self::new_rc_waker(self, op_id.slot()));
            debug_assert!(_waker);
            op_id
        });
    }

    pub fn poll_completed(self: &Rc<Self>) -> usize {
        self.io
            .access(|ctx| {
                ctx.poll_completions_callback(|opid, result, opref| {
                    let (from, uid, _iobuf) = match self.access(|state| state.at(opid.slot()).map(|x| (x.0, x.1, x.2.iov_base))) {
                        Some(x) => x, // an I/O known by us
                        None => return OpCompletionAction::TryWake,
                    };
                    let buf = std::mem::replace(unsafe { opref.buffer_ref().unwrap_unchecked() }, self.bufpool.pop());
                    debug_assert_eq!(uid, opid.uid());
                    debug_assert_eq!(_iobuf as *const u8, buf.ptr());
                    self.consumer.consume_raw(result, from, buf, opid);
                    // reuse operation slot directly by resubmitting an op with the same slot and ID
                    if self.wind_down.get() {
                        self.access(|state| state.free_slot(opid.slot()));
                        log::error!("wind_down flag set, removing watermark task");
                        return OpCompletionAction::Drop;
                    }
                    let res = self.submit_recv_op(opid, unsafe { opref.buffer_ref().unwrap_unchecked() });
                    match res {
                        Ok(_) => OpCompletionAction::ReuseSlot,
                        Err(_e) => {
                            self.access(|state| state.free_slot(opid.slot()));
                            log::error!("error during recv op submit, removing watermark task: {}", _e);
                            // XXX refill up to watermark after dropping this continuous I/O
                            OpCompletionAction::Drop
                        }
                    }
                })
            })
            .expect("error")
    }

    /// XXX result
    fn submit_recv_op(&self, op_id: OpId, buf: &mut IoBuf) -> Result<(), std::io::Error> {
        let msghdr = self.access(|state| state.set_msg_buffer(op_id, buf)).ok_or(std::io::Error::other("unknown slot"))?;
        let op = opcode::RecvMsg::new(types::Fd(self.socket), msghdr)
            .flags(libc::MSG_CMSG_CLOEXEC as u32)
            .build()
            .clear_flags()
            .user_data(op_id.into_user_data());
        // eprintln!("opcode {:?}, msghdr {:?}, msgiov {:?}", op, unsafe {  *msghdr }, unsafe { *(*msghdr).msg_iov });
        self.io
            .access(|uring| uring.enqueue_retry_once(op))
            .map(|_r| ())
            .map_err(|_sqe| std::io::Error::other("Could not enqueue SQE after two tries"))
    }

    fn wake_inner(self: &Rc<Self>, tag: u16, register_new_waker: bool) {
        let (from, opuid, _iobuf) = match self.access(|state| state.at(tag).map(|x| (x.0, x.1, x.2.iov_base))) {
            Some(x) if x.1 != 0 => x,
            Some(_x) => {
                log::error!("op uid for slot {} is zero", tag);
                return;
            }
            None => {
                log::error!("slot {} not found in watermark state", tag);
                return;
            }
        };
        let opid = OpId::new(tag, opuid);
        self.io.access(|uring| {
            uring.access_pending_op(opid, |opref| {
                let buf = std::mem::replace(unsafe { opref.buffer_ref().unwrap_unchecked() }, self.bufpool.pop());
                debug_assert_eq!(opref.op_uid(), opuid);
                debug_assert_eq!(_iobuf as *const u8, buf.ptr());
                debug_assert!(opref.result.is_some());
                self.consumer.consume_raw(unsafe { opref.result.unwrap_unchecked() }, from, buf, opid);
                // reuse operation slot directly by resubmitting an op with the same slot and ID
                if self.wind_down.get() {
                    self.access(|state| state.free_slot(opid.slot()));
                    log::error!("wind_down flag set, removing watermark task");
                    return ((), OpCompletionAction::Drop);
                }
                let res = self.submit_recv_op(opid, unsafe { opref.buffer_ref().unwrap_unchecked() });
                if register_new_waker {
                    opref.set_waker(Self::new_rc_waker(&self, tag));
                }
                match res {
                    Ok(_) => ((), OpCompletionAction::ReuseSlot),
                    Err(_e) => {
                        self.access(|state| state.free_slot(opid.slot()));
                        log::error!("error during recv op submit, removing watermark task: {}", _e);
                        // XXX refill up to watermark after dropping this continuous I/O
                        ((), OpCompletionAction::Drop)
                    }
                }
            })
        });
    }
}

////////////////////////////////////////////////////////////////////////////////
// Waking support for the case where we're used as a future
// instead of polling uring ourselves

impl<Consumer: PacketConsumer> RcWaker for WatermarkRecv<Consumer> {
    type Handler = Self;
    fn on_wake(handler: Rc<Self>, tag: u16) {
        log::debug!("watermark slot {} woken", tag);
        handler.wake_inner(tag, true);
    }

    fn on_wake_by_ref(handler: &Rc<Self>, tag: u16) {
        log::debug!("watermark slot {} woken by ref", tag);
        handler.wake_inner(tag, false);
    }

    fn on_drop_waker(_handler: Rc<Self>, tag: u16) {
        log::debug!("watermark slot {} waker dropped", tag);
    }
}

////////////////////////////////////////////////////////////////////////////////
// stream implementation

#[cfg(test)]
mod tests {
    use crate::runtime::{Executor, ThreadRuntime, test_exec, test_rt};
    use socket2::{Domain, SockAddr, Type};
    use std::{cell::Cell, net::ToSocketAddrs as _, os::fd::AsRawFd, time::Duration};

    use super::*;

    struct MockConsumer {
        from: SocketAddr,
        expected: Vec<(String, Cell<usize>)>,
    }

    impl MockConsumer {
        fn new(from: SocketAddr, msgs: Vec<String>) -> Self {
            let mut vec = Vec::with_capacity(msgs.len());
            for msg in msgs.into_iter() {
                vec.push((msg, Cell::new(0)))
            }
            Self { from, expected: vec }
        }

        fn all_done(&self) -> bool {
            for (_s, c) in self.expected.iter() {
                if c.get() < 1 {
                    return false;
                }
            }
            true
        }

        fn assert_all_received_exactly_once(&self) {
            for (s, c) in self.expected.iter() {
                assert_eq!(c.get(), 1, "Message {} received {} times", s, c.get());
            }
        }
    }

    impl PacketConsumer for MockConsumer {
        fn consume_raw(&self, result: i32, from: SocketAddr, buf: IoBuf, _opid: OpId) {
            if result <= 0 {
                panic!("recv error: {}", result);
            }
            if from != self.from {
                panic!(
                    "recv from {:?} ({:x}), but expected from {:?} ({:x})",
                    from,
                    from.port(),
                    self.from,
                    self.from.port()
                );
            }
            let bytes = &buf.as_slice()[0..(result as usize)];
            let received = std::str::from_utf8(&bytes).expect("error converting received bytes to string");
            let found = match self.expected.iter().find(|(s, _c)| s == received) {
                Some(r) => r,
                None => panic!("message {} not found in {:?}", received, self.expected),
            };
            found.1.update(|x| x + 1)
        }
    }

    #[test]
    fn test_standalone_continuous_receive() {
        let messages = vec![
            "Hello World".to_string(),
            "test_standalone_continuous_receive test".to_string(),
            "4269".to_string(),
        ];
        let recv_addr = ("127.0.4.1", 8000u16)
            .to_socket_addrs()
            .expect("error converting to socket addr")
            .next()
            .expect("no socket addr conversions found");
        let sender_addr = (("127.0.5.1", 8001u16))
            .to_socket_addrs()
            .expect("error converting to socket addr")
            .next()
            .expect("no socket addr conversions found");
        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|s| {
            s.spawn({
                let sender_addr = sender_addr.clone();
                let messages = messages.clone();
                let barrier = &barrier;
                move || {
                    let ring = ThreadUring::new(Default::default()).expect("error creating uring");
                    let sock = std::net::UdpSocket::bind(recv_addr).expect("error binding udp socket");
                    let rawfd = sock.as_raw_fd();
                    // ----------------------------------------
                    // let mut buf = [0u8; 1024];
                    // let mut nrecv = 0;
                    // barrier.wait();
                    // while let Ok((nbyte, from)) = sock.recv_from(&mut buf) {
                    //     let s = std::str::from_utf8(&buf[0..nbyte]).expect("error constructing string");
                    //     println!("received {} from {:?}", s, from);
                    //     nrecv += 1;
                    //     if nrecv == messages.len() {
                    //         break;
                    //     }
                    // }
                    // ----------------------------------------
                    barrier.wait();
                    let watermark = WatermarkRecv::new(rawfd, ring.clone(), 4, MockConsumer::new(sender_addr, messages));
                    while !watermark.consumer.all_done() {
                        watermark.poll_completed();
                    }
                    watermark.consumer.assert_all_received_exactly_once();
                }
            });

            barrier.wait();
            test_rt().with_executor(Default::default(), || async move {
                let sock = test_exec().io().udp_bind(sender_addr).expect("error binding sender socket");
                let _port = sock.local_addr().expect("error getting local address").port();
                let bufpool = crate::io::local_packet_buffer_pool();
                let fd = sock.as_raw_fd();
                for msg in messages {
                    let bytes = msg.as_bytes();
                    let mut buf = bufpool.pop();
                    buf.as_mut_slice()[0..bytes.len()].copy_from_slice(bytes);
                    buf.mark_used(bytes.len());
                    let (res, _buf) = test_exec().io().send_to(fd, recv_addr, buf).await;
                    let sent = res.expect("error sending to socket");
                    assert_eq!(sent, bytes.len());
                }
            })
            // now send udp messages to that socket and assert that they arrive
        });
    }

    /// Test using watermark recv via async/waker interface
    /// Instead of polling manually, let librt drive the uring via wakers
    #[test]
    fn test_watermark_with_waker_interface() {
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };

        let messages = vec!["wake1".to_string(), "wake2".to_string(), "wake3".to_string()];
        let recv_addr = ("127.0.4.30", 8300u16)
            .to_socket_addrs()
            .expect("error converting to socket addr")
            .next()
            .expect("no socket addr conversions found");
        let sender_addr = ("127.0.5.30", 8301u16)
            .to_socket_addrs()
            .expect("error converting to socket addr")
            .next()
            .expect("no socket addr conversions found");

        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(std::sync::Barrier::new(2));
        std::thread::scope(|s| {
            s.spawn({
                let messages = messages.clone();
                let barrier = barrier.clone();
                let done = done.clone();
                move || {
                    // Use librt to drive the uring via wakers
                    test_rt().with_executor(Default::default(), || async move {
                        // Create a new uring that librt will drive
                        let ring = ThreadUring::new(Default::default()).expect("error creating uring");
                        let sock = ring.udp_bind(recv_addr).expect("error binding recv socket");
                        let recv_fd = sock.as_raw_fd();

                        let watermark =
                            WatermarkRecv::new(recv_fd, ring.clone(), 4, MockConsumer::new(sender_addr, messages.clone()));
                        barrier.wait();

                        let mut poller = std::pin::pin!(ring.new_spurious_poller());
                        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

                        // Don't call poll_completed - let the wakers do the work
                        // Just wait for completion by periodically checking
                        let mut check_count = 0;
                        while !watermark.consumer.all_done() {
                            // Yield to allow other tasks to run by awaiting a completed future
                            let _ = poller.as_mut().poll(&mut cx);
                            check_count += 1;
                            if check_count > 1000 {
                                panic!("timeout waiting for watermark completion");
                            }
                            test_exec().io().sleep(Duration::from_millis(1)).await.expect("error sleeping");
                        }

                        watermark.consumer.assert_all_received_exactly_once();
                        done.store(true, Ordering::SeqCst);
                    })
                }
            });

            // Sender using separate runtime
            test_rt().with_executor(Default::default(), || async move {
                let sock = test_exec().io().udp_bind(sender_addr).expect("error binding sender socket");
                let bufpool = crate::io::local_packet_buffer_pool();

                barrier.wait();

                for msg in messages {
                    let bytes = msg.as_bytes();
                    let mut buf = bufpool.pop();
                    buf.as_mut_slice()[0..bytes.len()].copy_from_slice(bytes);
                    buf.mark_used(bytes.len());
                    let (res, _buf) = test_exec().io().send_to(sock.as_raw_fd(), recv_addr, buf).await;
                    res.expect("error sending message");
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }

                // Wait for receiver to finish
                while !done.load(Ordering::SeqCst) {
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            })
        });
    }
}

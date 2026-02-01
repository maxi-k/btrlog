use std::{
    cell::UnsafeCell,
    net::{SocketAddr, UdpSocket},
    os::fd::AsRawFd,
    rc::Rc,
    time::{Duration, Instant},
};

use reqtrace::trace;

use crate::{trace::{ClientSideTracer, ClientRequestTraceBuffer}, types::wire::EncodeError};
use crate::{
    io::{
        ThreadBuffers, ThreadUring,
        buffer::IoBuffer,
        local_packet_buffer_pool,
        watermark::{PacketConsumer, WatermarkRecv},
    },
    runtime::{self, stackref::StateTable},
    types::{
        message::JournalRequest,
        packet::{PacketHeader, RequestPacket, ResponsePacket},
        wire::WireMessage,
    },
};

use super::{ForCluster, stats::ClientStats};

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct RetrySpec {
    pub cluster_size: usize,
    pub required_majority: usize,
    pub retries: usize,
    pub backoff: f64,
    pub maj_timeout: Duration,
    pub max_timeout: Duration,
    pub track_stats: bool,
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct PendingQuorum<Msg> {
    received: ForCluster<Option<Msg>>,
    from: ForCluster<SocketAddr>,
    wake_after_n: isize,
}

impl<Msg> PendingQuorum<Msg> {
    pub(super) fn new(max: usize, majority: usize, expected: impl IntoIterator<Item = SocketAddr>) -> Self {
        let mut received = ForCluster::with_capacity(max);
        (0..max).for_each(|_| received.push(None));
        Self {
            received,
            from: ForCluster::from_iter(expected),
            wake_after_n: majority as isize,
        }
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (&Option<Msg>, &SocketAddr)> {
        self.received.iter().zip(self.from.iter())
    }

    #[allow(dead_code)]
    pub(super) fn into_iter(self) -> impl Iterator<Item = (Option<Msg>, SocketAddr)> {
        self.received.into_iter().zip(self.from.into_iter())
    }

    pub(super) fn iter_mut(&mut self) -> impl Iterator<Item = (&mut Option<Msg>, &mut SocketAddr)> {
        self.received.iter_mut().zip(self.from.iter_mut())
    }
}

////////////////////////////////////////////////////////////////////////////////
struct GlobalQuorumState {
    pending: StateTable<PendingQuorum<ResponsePacket>>,
}

impl GlobalQuorumState {
    fn new(cap: u16) -> Rc<Self> {
        Rc::new(Self {
            pending: StateTable::new(cap),
        })
    }

    fn on_message_recv(self: &Rc<Self>, from: SocketAddr, slot: u16, uid: u32, msg: ResponsePacket) {
        let ip = from.ip();
        self.pending.map(slot, uid, |state| {
            log::trace!("received response packet from {:?} for stream {}/{} at {:?}", from, slot, uid, Instant::now());
            // XXX implement different replacement semantics
            for (addr, msg_slot) in state.from.iter_mut().zip(state.received.iter_mut()) {
                if addr.ip() != ip {
                    continue;
                }
                match msg_slot {
                    Some(_) => {
                        log::trace!("received another message from {:?} on stream {}/{}, ignoring second one", ip, slot, uid);
                        return None;
                    }
                    None => {
                        //
                        *addr = from; // write the port where message came from
                        *msg_slot = Some(msg); // write the message
                        state.wake_after_n -= 1; // wake potential listener (majority reached?)
                        return if state.wake_after_n <= 0 { Some(()) } else { None };
                    }
                }
            }
            None
        });
    }
}

impl PacketConsumer for Rc<GlobalQuorumState> {
    fn consume_raw(&self, result: i32, from: SocketAddr, buf: crate::io::buffer::IoBuf, _id: crate::io::uring::OpId) {
        if result <= 0 {
            log::error!("error while receiving journal response: {}", result);
            return;
        }
        let (pkg, _bytes) = match ResponsePacket::decode_from(buf.as_slice()) {
            Ok((pkg, bytes)) => (pkg, bytes),
            Err(_e) => {
                log::error!("error decoding packet: {:?}", _e);
                return;
            }
        };
        let (slot, uid) = ((pkg.header.msg_id >> 32) as u16, pkg.header.msg_id as u32);
        // XXX this doesn't need to be the case, but the current
        // implementation has this property; assert to ensure correct logic
        debug_assert_eq!(pkg.header.reply_to, from.port());

        self.on_message_recv(from, slot, uid, pkg);
    }
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, derive_more::Display, thiserror::Error)]
pub enum RetryRequestError {
    NoOpenStreams,
    LocalIO,
    MessageEncoding,
    #[display("(:timeout {_0:?} :retries {_1} :took {_2:?})")]
    TooManyRetries(Duration, usize, Duration),
    Aborted(String),
}

#[derive(Debug)]
pub enum RetryStatus {
    Ok,
    Retry,
    Abort(String),
}

////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct JournalDriverConfig {
    pub io_depth: u16,
    pub open_reads: u16,
    pub track_percentiles: bool,
    pub request_sample_rate: u32
}
impl JournalDriverConfig {
    crate::util::integrated_builder!();
}

pub struct JournalQuorumDriver {
    sock: UdpSocket,
    addr: SocketAddr,
    io: ThreadUring,
    bufpool: ThreadBuffers,
    pending: Rc<GlobalQuorumState>,
    recv: Rc<WatermarkRecv<Rc<GlobalQuorumState>>>,
    stats: std::cell::UnsafeCell<ClientStats>,
    cfg: JournalDriverConfig,
    request_trace_buffer: Option<ClientRequestTraceBuffer>
}

impl JournalQuorumDriver {
    pub(crate) fn new(sock: UdpSocket, io: ThreadUring, cfg: JournalDriverConfig) -> Self {
        let rawsock = sock.as_raw_fd();
        let pending = GlobalQuorumState::new(cfg.io_depth as u16);
        let addr = sock.local_addr().expect("error getting local address of udp port");
        let trace_buffer = match cfg.request_sample_rate {
            0 => None,
            _n => Some(ClientRequestTraceBuffer::new())
        };
        Self {
            sock,
            addr,
            io: io.clone(),
            bufpool: local_packet_buffer_pool(),
            pending: pending.clone(),
            recv: WatermarkRecv::new(rawsock, io.clone(), cfg.open_reads as u16, pending),
            stats: UnsafeCell::new(ClientStats::new(cfg.track_percentiles)),
            cfg,
            request_trace_buffer: trace_buffer
        }
    }

    pub fn trace<T, F: FnOnce(&mut ClientStats) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.stats.get() })
    }

    pub fn take_stats(&self) -> ClientStats {
        std::mem::replace(unsafe { &mut *self.stats.get() }, ClientStats::new(self.cfg.track_percentiles))
    }

    pub fn poll_recv(&self) -> usize {
        self.recv.poll_completed()
    }

    pub fn wind_down_recv(&self) {
        self.recv.start_winding_down();
    }

    // XXX put these futures into their own queue, separate from
    // generic tasks, by intercepting the waker?
    pub async fn retrying_quorum_send_recv<Remotes, CheckResponse>(
        self: &Rc<Self>,
        req: JournalRequest,
        spec: &RetrySpec,
        remotes: Remotes,
        mut check_response: CheckResponse,
    ) -> Result<(ForCluster<Option<ResponsePacket>>, ForCluster<SocketAddr>), RetryRequestError>
    where
        CheckResponse: FnMut(&ResponsePacket, &SocketAddr) -> RetryStatus,
        Remotes: IntoIterator<Item = SocketAddr>,
    {
        // initialize & register quorum state for this request
        let mut quorum = PendingQuorum::new(spec.cluster_size, spec.required_majority, remotes);
        let mut stream = self.pending.pending.put(&mut quorum).ok_or(RetryRequestError::NoOpenStreams)?;
        // encode buffers for request
        let local_port = self.addr.port();
        let (slot, uid) = stream.refids();
        log::trace!(
            "[{:?}] created stream for req {} with no ({}, {}) for remotes {:?}",
            std::thread::current().id(),
            req,
            slot,
            uid,
            stream.from
        );
        let packet = RequestPacket {
            header: PacketHeader {
                msg_id: ((slot as u64) << 32) | (uid as u64),
                reply_to: local_port,
            },
            request: req,
        };
        let mut bufs = Self::encode_many(&self.bufpool, spec.cluster_size, packet)
            .map_err(|_| RetryRequestError::MessageEncoding)?
            .collect::<ForCluster<_>>();
        // track time 

        let t_start = Instant::now();
        let mut t_recv_start = t_start;
        #[cfg(feature = "trace-requests")]
        let mut _time_tracer = ClientSideTracer::new(t_start);
        #[cfg(feature = "trace-requests")]
        let mut should_trace_request = false;
        // retry sends and receives
        let mut cur_timeout = spec.maj_timeout;
        let request_bytes = bufs[0].used_bytes();
        for try_no in 0..spec.retries {
            trace!(_time_tracer.send_call, {
                // execute sends to nodes we haven't seen responses from
                let sends = runtime::future::join_all(stream.iter().filter_map(|(existing_result, remote)| {
                    if existing_result.is_some() {
                        return None;
                    }
                    let buf = bufs.pop().expect("not enough buffers left?!");
                    let _time = Instant::now();
                    log::trace!(
                        "[{:?}][try {}] executing send on stream {}/{} to {} at {:?}",
                        std::thread::current().id(),
                        try_no,
                        slot,
                        uid,
                        *remote,
                        _time
                    );
                    return Some(async move { self.io.send_to(self.sock.as_raw_fd(), *remote, buf).await });
                }))
                .await;
                // reclaim buffers once sends are done
                for (res, buf) in sends {
                    bufs.push(buf);
                    assert!(res.is_ok(), "Send failed"); // XXX
                }
            });
            if try_no == 0 {
                t_recv_start = Instant::now();
            } else {
                log::debug!("retry #{} for request {}/{}", try_no, slot, uid);
            }
            // retry messages
            let quorum_future = stream.wait_for(|state| {
                if state.wake_after_n > 0 {
                    // short-circuit initial/spurious wakes
                    return None;
                }
                let mut valid_count = 0;
                let mut retry_count = 0;
                for (from_idx, (opt_msg, addr)) in state.iter_mut().enumerate() {
                    if let Some(msg) = opt_msg {
                        // XXX track which request we already called the validation callback for to avoid redos?
                        match check_response(&msg, addr) {
                            RetryStatus::Ok => {
                                #[cfg(feature = "trace-requests")] {
                                    use crate::types::message::{AddEntryResponse, JournalResponse};
                                    if let ResponsePacket{ request: JournalResponse::AddEntry(AddEntryResponse { tracer, .. }), .. } = msg {
                                        _time_tracer.server_traces[from_idx] = tracer.clone();
                                        _time_tracer.server_timestamps[from_idx] = Instant::now();
                                        should_trace_request = true;
                                    }
                                }
                                valid_count += 1;
                            },
                            RetryStatus::Retry => {
                                // need to schedule additional sends; clear previous result
                                retry_count += 1;
                                *opt_msg = None;
                            }
                            x => {
                                return Some(x);
                            }
                        }
                    }
                }
                if valid_count >= spec.required_majority {
                    // wake the future, we're done!
                    Some(RetryStatus::Ok)
                } else if retry_count > 0 {
                    // wake the future, schedule retries
                    Some(RetryStatus::Retry)
                } else {
                    // don't wake the future
                    None
                }
            });
            let rt = crate::client::client_exec();
            // XXX uring-supported timeouts using sqe chains
            let timeout_future = rt.sleep(cur_timeout);
            runtime::select! {
                _ = timeout_future => {
                    log::error!("[{:?}] timeout on stream {}/{} after {:?}, retrying at {:?}", std::thread::current().id(), slot, uid, cur_timeout, Instant::now());
                    cur_timeout = cur_timeout.mul_f64(spec.backoff).min(spec.max_timeout);
                    continue;
                },
                res = quorum_future => match res {
                    RetryStatus::Abort(msg) => return Err(RetryRequestError::Aborted(msg)),
                    RetryStatus::Retry => continue, // XXX should we increase the timeout? we did receive responses...
                    RetryStatus::Ok => {
                        let recv_lat = t_recv_start.elapsed();
                        if spec.track_stats {
                            self.trace(|s| s.record_request(
                                recv_lat,
                                t_recv_start.duration_since(t_start),
                                request_bytes,
                                try_no
                            ));
                        }
                        std::mem::drop(stream);
                        log::trace!("destroying stream {}/{}", slot, uid);
                        #[cfg(feature = "trace-requests")] {
                            if should_trace_request {
                                if let Some(ref buf) = self.request_trace_buffer {
                                    // if uid % self.cfg.request_sample_rate == 0 {
                                        _time_tracer.message_id = ((slot as u64) << 32) | (uid as u64);
                                        _time_tracer.c_qlen = rt.get_current_qlen();
                                        _time_tracer.c_oio = self.io.current_outstanding();
                                        _time_tracer.c_cqlen = self.io.last_known_cq_batch_size();
                                        _time_tracer.recv_lat = recv_lat;
                                        _time_tracer.total_lat = t_start.elapsed();
                                        _time_tracer.retries = try_no;
                                        buf.push(_time_tracer);
                                    // }
                                }
                            }
                        }
                        return Ok((quorum.received, quorum.from))
                    }
                }
            }
        }
        self.trace(|s| {
            s.record_request(t_recv_start.elapsed(), t_recv_start.duration_since(t_start), request_bytes, spec.retries)
        });
        Err(RetryRequestError::TooManyRetries(spec.maj_timeout, spec.retries, t_start.elapsed()))
    }

    pub(crate) fn encode_many<R: WireMessage<()>>(
        bufpool: &ThreadBuffers,
        n: usize,
        req: R,
    ) -> Result<impl Iterator<Item = IoBuffer>, EncodeError> {
        let mut first_buf = bufpool.pop();
        let bytes = req.encode_into(first_buf.data_mut())?;
        first_buf.mark_used(bytes);
        let ptr = first_buf.ptr();
        let iter = std::iter::once(first_buf).chain(bufpool.iter().take(n - 1).map(move |mut buf| {
            buf.mark_used(bytes);
            buf.data_mut().copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, bytes) });
            buf
        }));
        Ok(iter)
    }
}

pub fn bind_retrying_client<Addr>(addr: Addr, cfg: JournalDriverConfig) -> std::io::Result<Rc<JournalQuorumDriver>>
where
    Addr: std::net::ToSocketAddrs + 'static,
{
    let io = runtime::rt().io().clone();
    let addr = addr.to_socket_addrs()?.next().ok_or(std::io::Error::other("No address found"))?;
    let sock = io.udp_bind(addr)?;
    Ok(Rc::new(JournalQuorumDriver::new(sock, io, cfg)))
}

#[cfg(test)]
mod tests {
    use crate::{
        dst::RandomDST as _,
        types::{id::JournalId, message::JournalMetadataRequest},
    };

    use super::*;

    #[test]
    fn test_encode_many() {
        let pool = local_packet_buffer_pool();
        let id = JournalId::dst_random();
        let msg = JournalMetadataRequest { id: id.clone().into() };
        let res: Vec<_> = JournalQuorumDriver::encode_many(&pool, 3, msg.clone()).expect("error encoding message").collect();
        debug_assert_eq!(res.len(), 3);
        let expected_id = id.into();
        for (idx, buf) in res.iter().enumerate() {
            let (decoded, _size) = JournalMetadataRequest::decode_from(buf.as_slice()).expect("failed decoding buf");
            assert_eq!(decoded.id, expected_id, "buffer no {} mismatch: {} vs expected {}", idx, decoded.id, expected_id);
        }
    }
}

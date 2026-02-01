use std::{
    cell::{Cell, UnsafeCell},
    future::Future,
    net::SocketAddr,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use clap::{Parser, Subcommand};
use perf_event_block::{BenchmarkParameters, PerfEventBlock};
use rand_distr::Uniform;
use socket2::{Domain, Socket, Type};
use std::os::fd::AsRawFd;
use v2::{
    client::{client_exec, client_rt},
    dst::{dst_sample, seed_thread_local_dst_rng},
    io::{
        ThreadUring, UringConfig,
        buffer::IoBuffer,
        local_buffer_pool,
        uring::{IOEnterIntent, OpId, UringPollMode},
        watermark::{PacketConsumer, WatermarkRecv},
    },
    runtime::{self, Executor, ThreadRuntime, rcwaker::RcWaker},
    trace::DurationStats,
};

#[derive(Parser, Debug)]
#[command(author, version, about = "io_uring-based Latency Benchmark Tool")]
struct Config {
    #[command(subcommand)]
    mode: Mode,
}

#[derive(Subcommand, Debug)]
enum Mode {
    /// Run as server
    Server(ServerConfig),
    /// Run as client
    Client(ClientConfig),
}

#[derive(Parser, Debug, Clone)]
struct ServerConfig {
    /// Port to bind to
    #[clap(long, default_value_t = 8080)]
    port: u16,

    /// Maximum packet size to handle
    #[clap(long, default_value_t = 8192)]
    max_packet_size: usize,

    /// Number of operations to benchmark
    #[clap(long, default_value_t = 100000)]
    op_count: usize,

    /// io_uring queue depth (number of concurrent recv operations)
    #[clap(long, default_value_t = 32)]
    io_depth: u16,

    /// io_uring SQ entries
    #[clap(long, default_value_t = 1024)]
    sq_entries: u32,

    /// io_uring CQ entries
    #[clap(long, default_value_t = 4096)]
    cq_entries: u32,

    #[clap(long, default_value_t = 10)]
    min_submit_frequency_us: usize,

    #[clap(long, default_value_t = 20)]
    syscall_timeout_us: usize,
}

#[derive(Parser, Debug, Clone)]
struct ClientConfig {
    /// Server IP address
    #[clap(long, default_value = "127.0.0.1")]
    server_ip: String,

    /// Server port
    #[clap(long, default_value_t = 8080)]
    server_port: u16,

    /// Number of operations to send
    #[clap(long, default_value_t = 100000)]
    op_count: usize,

    /// Minimum packet size
    #[clap(long, default_value_t = 64)]
    min_packet_size: usize,

    /// Maximum packet size
    #[clap(long, default_value_t = 8192)]
    max_packet_size: usize,

    /// Rate limiting: operations per second (0 = unlimited)
    #[clap(long, default_value_t = 0)]
    ops_per_sec: usize,

    /// Timeout for receiving responses (milliseconds)
    #[clap(long, default_value_t = 5000)]
    timeout_ms: u64,
}

type SendFuture = Pin<Box<dyn Future<Output = ()>>>;

struct BoundedTaskList {
    tasks: Vec<Option<SendFuture>>,
    active: Vec<u16>,
    free: Vec<u16>,
}

impl BoundedTaskList {
    fn new(iodepth: usize) -> Self {
        let mut free = vec![0u16; iodepth];
        let mut tasks = Vec::with_capacity(iodepth);
        for i in 0..iodepth {
            free[iodepth - i - 1] = i as u16;
            tasks.push(None);
        }
        Self {
            tasks,
            active: Vec::with_capacity(iodepth),
            free,
        }
    }

    fn make_buffer(&self) -> Vec<u16> {
        Vec::with_capacity(self.tasks.len())
    }
}

struct BenchmarkServer {
    io: ThreadUring,
    socket: std::os::fd::RawFd,
    op_count: Cell<usize>,
    target_ops: usize,
    e2e_latency_stats: UnsafeCell<DurationStats>,
    tasks: UnsafeCell<BoundedTaskList>,
}

struct ServerExt(Rc<BenchmarkServer>);
impl BenchmarkServer {
    fn new(io: ThreadUring, socket: std::os::fd::RawFd, target_ops: usize, task_depth: usize) -> Self {
        Self {
            io,
            socket,
            op_count: Cell::new(0),
            target_ops,
            e2e_latency_stats: UnsafeCell::new(DurationStats::default()),
            tasks: UnsafeCell::new(BoundedTaskList::new(task_depth)),
        }
    }

    fn is_done(&self) -> bool {
        self.op_count.get() >= self.target_ops
    }

    fn stats<T, F: FnOnce(&mut DurationStats) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.e2e_latency_stats.get() })
    }

    fn tasks<T, F: FnOnce(&mut BoundedTaskList) -> T>(&self, f: F) -> T {
        f(unsafe { &mut *self.tasks.get() })
    }

    fn poll_active_tasks(self: &Rc<Self>, buffer: Vec<u16>) -> Vec<u16> {
        debug_assert!(buffer.is_empty());
        let mut active = self.tasks(|t| std::mem::replace(&mut t.active, buffer));
        while let Some(idx) = active.pop() {
            self.tasks(|t| {
                let task_ref = unsafe { t.tasks.get_unchecked_mut(idx as usize) };
                let task = task_ref.as_mut().expect("active task doesn't exist");
                let waker = Self::new_rc_waker(self, idx);
                let mut cx = Context::from_waker(&waker);
                match Pin::new(task).poll(&mut cx) {
                    Poll::Ready(()) => {
                        // task is done, drop it
                        t.free.push(idx);
                        *task_ref = None;
                    }
                    Poll::Pending => {
                        // task got the waker, can re-insert itself into 'active'
                    }
                }
            })
        }
        active
    }

    fn spawn_send_op(self: &Rc<Self>, to: SocketAddr, op_id: u32) {
        let fut: SendFuture = Box::pin({
            let io = self.io.clone();
            let socket = self.socket;
            async move {
                let mut outgoing = local_buffer_pool(4, Default::default()).pop();
                outgoing.as_mut_slice()[..4].copy_from_slice(&op_id.to_le_bytes());
                outgoing.mark_used(4);
                let (_send_result, _) = io.send_to(socket, to, outgoing).await;
            }
        });

        self.tasks(|t| {
            let id = t.free.pop().expect("No more free task slots!");
            let slot = unsafe { t.tasks.get_unchecked_mut(id as usize) };
            debug_assert!(matches!(slot, None));
            *slot = Some(fut);
            t.active.push(id);
        });
    }

    fn wake_inner(self: &Rc<Self>, tag: u16) {
        self.tasks(|t| {
            debug_assert!(t.tasks[tag as usize].is_some());
            t.active.push(tag);
        })
    }

    fn print_stats(&self) {
        self.stats(|s| {
            println!("\n=== SERVER LATENCY STATISTICS ===");

            println!("Server E2E Latency Statistics:");
            println!("  Count: {}", s.count());
            println!("  Min: {:?}", s.min());
            println!("  Max: {:?}", s.max());
            println!("  Avg: {:?}", s.avg());
            if s.tracks_percentiles() {
                println!("  P50: {:?}", s.percentile(50.0));
                println!("  P95: {:?}", s.percentile(95.0));
                println!("  P99: {:?}", s.percentile(99.0));
            }
        })
    }
}

impl RcWaker for BenchmarkServer {
    type Handler = Self;

    fn on_wake(handler: Rc<Self>, tag: u16) {
        handler.wake_inner(tag);
    }

    fn on_wake_by_ref(handler: &Rc<Self>, tag: u16) {
        handler.wake_inner(tag);
    }

    fn on_drop_waker(_handler: Rc<Self>, _tag: u16) {}
}

impl PacketConsumer for ServerExt {
    fn consume_raw(&self, result: i32, from: SocketAddr, _buf: IoBuffer, _id: OpId) {
        let e2e_start = Instant::now();

        if result <= 0 {
            eprintln!("Failed to receive packet: error code {}", result);
            return;
        }

        let current_op = self.0.op_count.get();

        // Send response back to client (spawn as future)
        self.0.spawn_send_op(from, current_op as u32);

        let e2e_duration = e2e_start.elapsed();
        self.0.stats(|s| s.record(e2e_duration));

        self.0.op_count.set(current_op + 1);

        if (current_op + 1) % 10000 == 0 {
            println!("Processed {} operations", current_op + 1);
        }
    }
}

fn run_server(config: ServerConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Server listening on port {}", config.port);

    let _perf = PerfEventBlock::default_events(
        config.op_count as u64,
        BenchmarkParameters::new([
            ("server_port", config.port.to_string()),
            ("max_packet_size", config.max_packet_size.to_string()),
            ("io_depth", config.io_depth.to_string()),
        ]),
        perf_event_block::PrintMode::Transposed,
    );

    // Create io_uring context
    let ring = ThreadUring::new(UringConfig {
        poll_mode: UringPollMode::ManualPoll,
        taskrun_mode: Default::default(),
        sq_entries: config.sq_entries,
        cq_entries: config.cq_entries,
        print_slow_submits: false,
        min_submit_frequency_us: config.min_submit_frequency_us,
        allow_parking: true,
        syscall_timeout_us: config.syscall_timeout_us,
    })
    .map_err(|e| format!("Failed to create io_uring: {}", e))?;

    // Create UDP socket
    let socket = Socket::new(Domain::IPV4, Type::DGRAM, None).map_err(|e| format!("Failed to create socket: {}", e))?;
    // socket
    //     .set_nonblocking(true)
    //     .map_err(|e| format!("Failed to set non-blocking: {}", e))?;
    socket.set_reuse_address(true).map_err(|e| format!("Failed to set reuse address: {}", e))?;
    socket.set_reuse_port(true).map_err(|e| format!("Failed to set reuse port: {}", e))?;

    let addr: SocketAddr = format!("0.0.0.0:{}", config.port).parse().map_err(|e| format!("Invalid address: {}", e))?;
    socket.bind(&addr.into()).map_err(|e| format!("Failed to bind: {}", e))?;

    let socket_fd = socket.as_raw_fd();

    // Create server
    let server = Rc::new(BenchmarkServer::new(
        ring.clone(),
        socket_fd,
        config.op_count,
        config.io_depth as usize * 2, // task depth for send operations
    ));

    // Create watermark receiver
    let watermark = WatermarkRecv::new(socket_fd, ring.clone(), config.io_depth, ServerExt(server.clone()));

    // Main loop - poll both recv completions and send tasks
    let mut task_buffer = server.tasks(|t| t.make_buffer());
    while !server.is_done() {
        watermark.poll_completed();
        task_buffer = server.poll_active_tasks(task_buffer);
        ring.access(|ctx| ctx.enter(IOEnterIntent::Poll)).ok();
    }

    // Drain remaining send tasks
    while server.tasks(|t| !t.active.is_empty() || t.free.len() < t.tasks.len()) {
        task_buffer = server.poll_active_tasks(task_buffer);
        ring.enter(IOEnterIntent::Poll);
    }

    // Keep socket alive until all operations complete
    std::mem::drop(socket);

    server.print_stats();
    Ok(())
}

fn run_client(config: ClientConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("Starting benchmark with config: {:?}", config);

    let _perf = PerfEventBlock::default_events(
        config.op_count as u64,
        BenchmarkParameters::new([
            ("server_addr", format!("{}:{}", config.server_ip, config.server_port)),
            ("packet_size_range", format!("{}-{}", config.min_packet_size, config.max_packet_size)),
            ("ops_per_sec", config.ops_per_sec.to_string()),
        ]),
        perf_event_block::PrintMode::Transposed,
    );

    let _ = client_rt()
        .with_executor(Default::default(), || async move {
            let server_addr: SocketAddr = format!("{}:{}", config.server_ip, config.server_port)
                .parse()
                .map_err(|e| format!("Invalid server address: {}", e))?;

            let local_addr: SocketAddr = "0.0.0.0:0".parse().map_err(|e| format!("Invalid local address: {}", e))?;
            let io = client_exec().io().clone();

            let socket = io.udp_bind(local_addr).map_err(|e| format!("Failed to bind UDP socket: {}", e))?;
            let fd = socket.as_raw_fd();

            seed_thread_local_dst_rng(0);
            let size_dist = Uniform::new(config.min_packet_size, config.max_packet_size + 1).unwrap();
            let mut client_latency_stats = DurationStats::default();

            let rate_limit_interval = if config.ops_per_sec > 0 {
                Some(Duration::from_nanos(1_000_000_000 / config.ops_per_sec as u64))
            } else {
                None
            };

            let mut last_send_time = Instant::now();
            for op_idx in 0..config.op_count {
                // Rate limiting
                if let Some(interval) = rate_limit_interval {
                    let elapsed = last_send_time.elapsed();
                    if elapsed < interval {
                        io.sleep(interval - elapsed).await.expect("error sleeping");
                    }
                    last_send_time = Instant::now();
                }

                let packet_size = dst_sample(&size_dist);
                let mut payload = vec![0u8; packet_size];
                // Fill with pattern (optional, can be random if needed)
                for i in 0..packet_size {
                    payload[i] = (i % 256) as u8;
                }

                let client_start = Instant::now();

                // Send packet
                let mut iobuf = local_buffer_pool(packet_size, Default::default()).pop();
                iobuf.as_mut_slice()[..packet_size].copy_from_slice(&payload);
                iobuf.mark_used(packet_size);
                let (send_result, _) = io.send_to(fd, server_addr, iobuf).await;

                if let Err(e) = send_result {
                    eprintln!("Failed to send packet {}: {:?}", op_idx, e);
                    continue;
                }

                // Receive response with timeout
                let timeout_duration = Duration::from_millis(config.timeout_ms);

                let response_result: Result<u32, String> = runtime::select! {
                    result = async {
                        let iobuf = local_buffer_pool(4, Default::default()).pop();
                        let (result, retbuf) = io.recv_from(fd, iobuf, None).await;
                        match result {
                            Ok((bytes_received, _from_addr)) => {
                                if bytes_received < 4 {
                                    Err("Received packet too small".into())
                                } else {
                                    let response_data = &retbuf.as_slice()[..bytes_received];
                                    let op_id = u32::from_le_bytes([
                                        response_data[0],
                                        response_data[1],
                                        response_data[2],
                                        response_data[3],
                                    ]);
                                    Ok(op_id)
                                }
                            }
                            Err(e) => Err(format!("Failed to receive response: {:?}", e).into()),
                        }
                    } => result,
                    _ = io.sleep(timeout_duration) => Err("timeout".into())
                };

                match response_result {
                    Ok(id) => {
                        let client_duration = client_start.elapsed();
                        client_latency_stats.record(client_duration);
                        assert_eq!(id as usize, op_idx, "wrong response: got id {}, expected {}", id, op_idx);
                    }
                    Err(e) => {
                        eprintln!("Failed to receive response for packet {}: {}", op_idx, e);
                        continue;
                    }
                }

                if op_idx % 10000 == 0 && op_idx > 0 {
                    println!("Processed {} operations", op_idx);
                }
            }

            println!("\n=== CLIENT LATENCY STATISTICS ===");
            println!("Client-Observed Latency Statistics:");
            println!("  Count: {}", client_latency_stats.count());
            println!("  Min: {:?}", client_latency_stats.min());
            println!("  Max: {:?}", client_latency_stats.max());
            println!("  Avg: {:?}", client_latency_stats.avg());
            if client_latency_stats.tracks_percentiles() {
                println!("  P50: {:?}", client_latency_stats.percentile(50.0));
                println!("  P95: {:?}", client_latency_stats.percentile(95.0));
                println!("  P99: {:?}", client_latency_stats.percentile(99.0));
            }

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
        .map_err(|e| format!("Runtime error: {}", e))?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    #[cfg(feature = "env-logger")]
    env_logger::init_from_env("RUST_LOG");

    let config = Config::parse();

    match config.mode {
        Mode::Server(server_config) => {
            println!("Server config: {:?}", server_config);
            run_server(server_config)
        }
        Mode::Client(client_config) => run_client(client_config),
    }
}

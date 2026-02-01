use std::{
    cell::{Cell, RefCell},
    net::{IpAddr, SocketAddr},
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use clap::Parser;
use futures_util::FutureExt;
use perf_event_block::{BenchmarkParameters, ColumnWriter, PerfEventBlock, PerfEventTimesliced};
use rand_distr::{Exp, Zipf};
use serde::{Deserialize, Serialize};
use v2::{
    bench::{get_benchmark_environment, output_benchmark_result},
    client::{
        self, ClusterClientView, ClusterUtils, ForCluster, JournalClientState, WalRequestQueue, WalSlotGuard, client_exec,
        client_rt,
        quorum::{JournalDriverConfig, JournalQuorumDriver, RetrySpec, RetryStatus, bind_retrying_client},
        stats::ClientStats,
    },
    config::{ClientConfig, ClientThreadConfig, IOConfig, ServerConfig},
    dst::{RandomDST, dst_sample, random_byte_vec, seed_thread_local_dst_rng},
    node::health::ClusterHealth,
    runtime::{self, ExecutorConfig, ThreadRuntime, sync::mpsc::UnboundedSender},
    trace::MetricMap,
    types::{id::JournalId, message::*, packet::ResponsePacket},
};

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Open Journal Service Benchmark")]
struct BenchConfig {
    /// [bench] how many operations to execute overall
    #[clap(long, default_value_t = 1000*1000)]
    op_count: usize,

    /// [bench] how many operations to execute overall
    #[clap(long, default_value_t = 1000)]
    sample_frequency: usize,

    /// [bench] after how many seconds should we create new logs
    /// with their own arrival rate
    #[clap(long, default_value_t = 200)]
    log_creation_interval_ms: u64,

    /// [bench] how often do we try to push data at most?
    /// note this is also bounded (for each log instance) by
    /// the response time, since logs have sequential semantics
    #[clap(long, default_value_t = 1)]
    min_log_push_interval_ms: u64,

    /// [bench] maximum for the push interval random distribution of each log.
    #[clap(long, default_value_t = 10)]
    max_log_push_interval_ms: u64,

    /// [bench] how often should we print out current latency percentile numbers and op count?
    #[clap(long, default_value_t = 200)]
    print_interval_ms: u64,

    /// [bench] Number of parallel benchmark instances (threads) to run.
    #[clap(long, default_value_t = 1)]
    primary_thread_count: usize,

    /// [bench] Number of initial journals to create per thread.
    #[clap(long, default_value_t = 1)]
    initial_log_count: usize,

    /// [bench] Maximum message payload size for random distribution
    #[clap(long, default_value_t = 8192)]
    max_message_size: usize,

    /// [bench] Minimum message payload size for random distribution
    #[clap(long, default_value_t = 1)]
    min_message_size: usize,

    /// [bench] Zipf exponent for size distribution (higher = more skewed towards smaller sizes)
    #[clap(long, default_value_t = 0.9)]
    zipf_factor: f64,

    /// [bench] track more fine grained latencies
    #[clap(long, default_value_t = false)]
    track_journal_latency: bool,

    /// [bench] enable (more expensive) latency percentile tracking
    #[clap(long, default_value_t = cfg!(feature = "trace-percentiles"))]
    track_percentiles: bool,

    /// [bench] send heartbeats instead of write requests; servers will respond with heartbeats too.
    /// useful for measuring network + network stack + async runtime behavior (w/o disk, data alloc etc.)
    #[clap(long, default_value_t = false)]
    heartbeat_mode: bool,

    #[command(flatten)]
    client: ClientConfig,

    #[command(flatten)]
    server: ServerConfig,

    #[command(flatten)]
    io: IOConfig,
}

impl BenchConfig {
    fn driver_cfg(&self, tid: usize, cluster: &ClusterClientView) -> JournalDriverConfig {
        JournalDriverConfig {
            io_depth: (self.io.io_depth / 4) as u16, // XXX should limit the no of tasks per thread spawned
            open_reads: (cluster.node_count() * 2) as u16, // XXX
            track_percentiles: self.track_percentiles,
            request_sample_rate: (if cfg!(feature = "trace-requests") && tid == 1 { self.sample_frequency } else { 0 }) as u32
        }
    }

    fn journal_retry_spec(&self, cluster: &ClusterClientView) -> RetrySpec {
        RetrySpec {
            cluster_size: cluster.node_count(),
            required_majority: cluster.node_count(),
            retries: self.client.create_journal_message_retries,
            backoff: self.client.retry_backoff_factor,
            maj_timeout: Duration::from_millis(self.client.create_journal_timeout_ms),
            max_timeout: Duration::from_millis(100), // XXX
            track_stats: self.track_journal_latency,
        }
    }

    fn entry_retry_spec(&self, cluster: &ClusterClientView) -> RetrySpec {
        let required_majority = if self.client.required_majority == 0 {
            cluster.majority()
        } else {
            self.client.required_majority
        };
        RetrySpec {
            cluster_size: cluster.node_count(),
            required_majority,
            retries: self.client.per_message_retries,
            backoff: self.client.retry_backoff_factor,
            maj_timeout: Duration::from_millis(self.client.per_message_timeout_ms),
            max_timeout: Duration::from_millis(20), // XXX
            track_stats: true,
        }
    }

    fn cluster_retry_spec(&self, cluster: &ClusterClientView) -> RetrySpec {
        RetrySpec {
            cluster_size: cluster.node_count(),
            required_majority: cluster.node_count(),
            retries: self.client.create_journal_message_retries,
            backoff: self.client.retry_backoff_factor,
            maj_timeout: Duration::from_millis(self.client.cluster_state_timeout_ms),
            max_timeout: Duration::from_millis(1000), // XXX
            track_stats: false,
        }
    }
}

struct BenchState {
    config: &'static BenchConfig,
    cluster: ClusterClientView,
    op_counter: AtomicUsize,
    startup_timestamp: Instant,
    journals_created: AtomicUsize,
}

impl BenchState {
    fn new(config: &'static BenchConfig, cluster: ClusterClientView) -> Arc<Self> {
        Arc::new(Self {
            config,
            cluster,
            op_counter: AtomicUsize::new(0),
            // allow some leeway for cluster startup
            startup_timestamp: Instant::now() + Duration::from_secs(2),
            journals_created: AtomicUsize::new(0),
        })
    }

    fn should_create_journal(&self, tid: usize) -> Result<(), Duration> {
        let journals_created = self.journals_created.load(Ordering::Acquire);
        let designated_creator = journals_created % self.config.primary_thread_count;
        if tid != designated_creator {
            // XXX order?
            return Err(Duration::from_millis(self.config.log_creation_interval_ms));
        }
        let now = Instant::now();
        let time_since_start = now.duration_since(self.startup_timestamp);
        // might be negative if startup timestamp > now; logic works the same though
        let journals_should = time_since_start.as_millis() as f64 / self.config.log_creation_interval_ms as f64;
        if journals_should > (journals_created as f64) {
            log::trace!(
                "journal creator {}/{} found SHOULD {} vs IS {} journals; allowing creation",
                designated_creator,
                tid,
                journals_should,
                journals_created
            );
            self.journals_created.fetch_add(1, Ordering::Release);
            Ok(())
        } else {
            let frac = 1f64 - (journals_should - journals_should.trunc());
            Err(Duration::from_millis((frac * self.config.log_creation_interval_ms as f64).max(1.0) as u64))
        }
    }
}

struct ThreadState {
    cfg: &'static BenchConfig,
    global: Arc<BenchState>,
    stat_collector: std::sync::mpsc::Sender<ClientStats>,
    local_ops: Cell<usize>,
    keep_running: Cell<bool>,
    driver: Rc<JournalQuorumDriver>,
}

async fn create_thread_state(
    cfg: &'static BenchConfig,
    global: Arc<BenchState>,
    stat_collector: std::sync::mpsc::Sender<ClientStats>,
    local_ip: IpAddr,
    myid: usize
) -> Rc<ThreadState> {
    let driver = bind_retrying_client((local_ip, 0), cfg.driver_cfg(myid, &global.cluster)).expect("failed to create retrying client");
    Rc::new(ThreadState {
        cfg,
        global,
        stat_collector,
        local_ops: Cell::new(0),
        keep_running: Cell::new(true),
        driver,
    })
}

impl ThreadState {
    fn cluster(&self) -> &ClusterClientView {
        &self.global.cluster
    }

    fn acquire_op_ticket(&self) -> bool {
        static MORSEL_SIZE: usize = 10000;
        let ops_left = self.local_ops.get();
        if ops_left > 0 {
            self.local_ops.set(ops_left - 1);
            return true;
        }
        let global_cursor = self.global.op_counter.fetch_add(MORSEL_SIZE, Ordering::Acquire);
        if global_cursor >= self.cfg.op_count {
            self.keep_running.set(false);
            return false;
        } else {
            let ops_left = MORSEL_SIZE.min(self.cfg.op_count - global_cursor);
            debug_assert!(ops_left > 0);
            self.local_ops.set(ops_left - 1);
            return true;
        }
    }

    fn spawn_stat_collector(self: Rc<Self>) -> impl Future<Output = ()> {
        client_exec().spawn_awaitable_task(async move {
            while self.keep_running.get() {
                client_exec().sleep(Duration::from_millis(self.cfg.print_interval_ms / 2)).await;
                let stats = self.driver.take_stats();
                if let Err(send_err) = self.stat_collector.send(stats) {
                    log::warn!("error while sending stats: {:?}", send_err);
                }
            }
        })
    }

    fn spawn_journal_task(self: Rc<Self>) -> impl Future<Output = ()> {
        client_exec().spawn_awaitable_task(async move {
            // Zipf distribution samples from 1 to n, so we adjust for the min/max range
            let size_range = (self.cfg.max_message_size - self.cfg.min_message_size + 1) as f64;
            let size_dist = Zipf::new(size_range, self.cfg.zipf_factor).unwrap();
            let size_offset = self.cfg.min_message_size - 1;
            let timer_mean_ms = (self.cfg.max_log_push_interval_ms.saturating_sub(self.cfg.min_log_push_interval_ms)) as f64;
            let timer_mean_ms = if timer_mean_ms > 0.0 { timer_mean_ms } else { 1.0 };
            let timer_dist = Exp::new(1.0 / timer_mean_ms).unwrap();
            let min_timer_interval = Duration::from_millis(self.cfg.min_log_push_interval_ms);
            let mut journal = self.create_journal().await;
            let wal_queue = WalRequestQueue::new();
            let mut time_overhang = Duration::from_millis(0);
            self.driver.trace(ClientStats::on_create_journal);
            while self.acquire_op_ticket() {
                let t_start = Instant::now();
                if self.cfg.heartbeat_mode {
                    self.heartbeat_quorum(&mut journal).await;
                } else {
                    let size = (dst_sample(&size_dist) as usize) + size_offset;
                    self.push_to_journal(&mut journal, &wal_queue, size).await;
                }
                let t_end = Instant::now();
                let op_latency = t_end.duration_since(t_start);
                let timer_sample_ms = dst_sample(&timer_dist);
                let push_interval = min_timer_interval + Duration::from_millis(timer_sample_ms as u64) + time_overhang;
                if push_interval <= op_latency {
                    continue;
                }
                let sleep_time_scaled = (push_interval - op_latency) * 1000;
                // clock resolution > 1ms -> sleep in millisecond intervals,
                // add the time overhang to the next round to keep the overall
                // rate roughly the same
                // XXX how expensive is all this conversion from and to Duration?
                let sleep_ms = sleep_time_scaled.as_secs();
                let sleep_micro = sleep_time_scaled.subsec_millis();
                time_overhang = Duration::from_micros(sleep_micro as u64);
                if sleep_ms >= 1 {
                    client_exec().sleep(Duration::from_millis(sleep_ms)).await;
                }
            }
        })
    }

    async fn create_journal(self: &Rc<Self>) -> JournalClientState {
        let cluster = self.cluster();
        let journal_retry_spec = self.cfg.journal_retry_spec(cluster);
        let journal_id: JournalId = JournalId::dst_random();
        let req = CreateJournalRequest { id: journal_id.into() };
        let (replies, remotes) = self
            .driver
            .retrying_quorum_send_recv(JournalRequest::CreateJournal(req), &journal_retry_spec, cluster.clone(), |msg, _from| {
                match &msg.request {
                    &JournalResponse::CreateJournal(ref create_resp) => match create_resp {
                        CreateJournalResponse::Ok | CreateJournalResponse::JournalAlreadyExists => RetryStatus::Ok,
                        CreateJournalResponse::BlobStoreError(_) => RetryStatus::Retry,
                    },
                    x => RetryStatus::Abort(format!("unexpected response to CreateJournal: {:?}", x)),
                }
            })
            .await
            .expect("error creating journals");
        // let req_size = retrying_stream.get_message_bytes();
        // let start_time = Instant::now();
        // let majority_latency = start_time.elapsed();
        // let retries = retrying_stream.get_executed_retries();
        // stats.record_request(majority_latency, req_size, retries);
        //
        let reply_to = replies
            .into_iter()
            .zip(remotes.into_iter())
            .map(|(resp, addr)| match resp {
                Some(ResponsePacket {
                    header,
                    request: JournalResponse::CreateJournal(create_resp),
                }) => {
                    match create_resp {
                        CreateJournalResponse::Ok => log::trace!("created journal on {:?}", addr),
                        CreateJournalResponse::JournalAlreadyExists => log::trace!("journal already exists on {:?}", addr),
                        CreateJournalResponse::BlobStoreError(e) => {
                            log::warn!("blobstore error creating journal on {:?}: {:?}", addr, e)
                        }
                    };
                    header.reply_to
                }
                None => {
                    log::trace!("no response received from {:?}", addr);
                    addr.port()
                }
                e => panic!("unexpected response creating journal: {:?}", e),
            })
            .collect::<ForCluster<_>>();
        JournalClientState::new_empty(journal_id, reply_to, self.cfg.io.lsn_window as u64)
    }

    async fn push_to_journal(
        self: &Rc<Self>,
        state: &mut JournalClientState,
        queue: &WalRequestQueue,
        size: usize,
    ) {
        let cluster = self.cluster();
        let mut seq_guard = std::pin::pin!(WalSlotGuard::new(state, queue));
        let entry_retry_spec = self.cfg.entry_retry_spec(cluster);
        let payload = random_byte_vec(size); // XXX prevent / reuse with exsiting iobuf
        let wal_position = seq_guard.get_wal_slot(payload.len() as u64).expect("error getting wal slot");
        let entry = seq_guard.make_request(&wal_position, &payload);
        // broadcast append request with retry logic
        let (replies, remotes) = self
            .driver
            .retrying_quorum_send_recv(
                JournalRequest::AddEntry(entry.clone()),
                &entry_retry_spec,
                cluster
                    .iter()
                    .zip(seq_guard.state().reply_to_iter())
                    .map(|(addr, port)| SocketAddr::from((addr.ip(), *port))),
                |msg, _from| {
                    match &msg.request {
                        &JournalResponse::AddEntry(ref add_resp) => {
                            match seq_guard.check(&add_resp) {
                                //
                                true => RetryStatus::Ok,
                                false => RetryStatus::Retry,
                            }
                        }
                        x => RetryStatus::Abort(format!("unexpected response to AddEntry: {:?}", x)),
                    }
                },
            )
            .await
            .expect(format!("error receiving replies for {} - {}", entry.id, entry.lsn).as_str());

        let mut any_response: Option<&AddEntryResponse> = None;
        for (idx, (resp, addr)) in replies.iter().zip(remotes.into_iter()).enumerate() {
            match resp {
                Some(ResponsePacket {
                    header: h,
                    request: JournalResponse::AddEntry(add_resp),
                }) => {
                    debug_assert_eq!(add_resp.lsn, wal_position.lsn);
                    seq_guard.set_reply_to(idx, h.reply_to);
                    any_response = Some(&add_resp);
                }
                None => {
                    log::trace!("no response received from {:?} for (AddEntry :lsn {} :offset {})", addr, entry.lsn, entry.offset)
                }
                x => panic!("unexpected response: {:?} for request {:?}", x, entry),
            }
        }
        match any_response {
            Some(resp) => seq_guard.ensure_completion_sequence(resp).await,
            None => unreachable!("received responses, but no response set"),
        };
    }

    async fn heartbeat_quorum(self: &Rc<Self>, state: &mut JournalClientState) {
        let cluster = self.cluster();
        let entry_retry_spec = self.cfg.entry_retry_spec(cluster);
        let id = state.id().into();
        let request = JournalRequest::JournalHeartbeat(id);

        let (replies, remotes) = self
            .driver
            .retrying_quorum_send_recv(
                request,
                &entry_retry_spec,
                cluster.iter().zip(state.reply_to_iter()).map(|(addr, port)| SocketAddr::from((addr.ip(), *port))),
                |msg, _from| match &msg.request {
                    &JournalResponse::JournalHeartbeat(got_id) if got_id == id => RetryStatus::Ok,
                    x => RetryStatus::Abort(format!("unexpected response to Heartbeat: {:?}", x)),
                },
            )
            .await
            .expect(format!("error receiving replies heartbeat request").as_str());

        for (_idx, (resp, addr)) in replies.into_iter().zip(remotes.into_iter()).enumerate() {
            match resp {
                Some(ResponsePacket {
                    header: _h,
                    request: JournalResponse::JournalHeartbeat(_),
                }) => {}
                None => {
                    log::trace!("no response received from {:?} for Heartbeat", addr)
                }
                x => panic!("unexpected response to heartbeat: {:?}", x),
            }
        }
    }
}

fn make_perf_block<ID: Into<String>>(id: ID, cfg: &BenchConfig, server_cfg: &ServerConfig) -> PerfEventBlock {
    let params = BenchmarkParameters::new([
        ("node", id.into()),
        ("msg_max", format!("{}", cfg.max_message_size)),
        ("msg_min", format!("{}", cfg.min_message_size)),
        ("blob", format!("{:?}", server_cfg.blob_store_impl)),
    ]);
    PerfEventBlock::default_events(cfg.op_count as u64, params, perf_event_block::PrintMode::Transposed)
}

////////////////////////////////////////////////////////////////////////////////
//  primary

async fn primary(bench_cfg: &'static BenchConfig) -> Result<(), anyhow::Error> {
    log::trace!("starting primary with benchmark cfg {:?}", bench_cfg);
    v2::dst::seed_thread_local_dst_rng(u64::MAX - 1);

    let server_cfg: &'static ServerConfig = &bench_cfg.server;
    // let (stats_send, mut stats_recv) = v2::runtime::sync::mpsc::unbounded_channel();

    let health = ClusterHealth::join_cluster_as_client().await?;
    let global_state = BenchState::new(
        bench_cfg,
        health
            .other_logger_ips()
            .map(|ip| SocketAddr::from((ip, server_cfg.port)))
            .collect::<ClusterClientView>(),
    );
    log::trace!("primary initialized cluster, waiting for all to be ready");
    health.set_state(NodeState::Ready);
    health.await_all_ready().await;
    log::info!("primary got ready cluster, starting benchmark on {} threads...", bench_cfg.primary_thread_count);
    // stat collection and continuous printing
    let run_stat_collector = Rc::new(AtomicBool::new(true));
    let global_stats = Rc::new(RefCell::new(ClientStats::new(bench_cfg.track_percentiles)));
    let _perf = PerfEventTimesliced::default_events(1, ClientStats::default(), Duration::from_millis(bench_cfg.print_interval_ms), true);
    // client::client_exec().spawn_task({
    //     let run_stat_collector = run_stat_collector.clone();
    //     let global_stats = global_stats.clone();
    //     let global_state = global_state.clone();
    //     async move {
    //         // let mut header = true;
    //         while run_stat_collector.load(Ordering::Acquire)
    //             && global_state.op_counter.load(Ordering::Acquire) < bench_cfg.op_count
    //         {
    //             // let t_start = Instant::now();
    //             client_exec().sleep(Duration::from_millis(bench_cfg.print_interval_ms)).await;
    //             let mut current_stats = ClientStats::new(bench_cfg.track_percentiles);
    //             // merge all accumulated stats
    //             while let Ok(stats) = stats_recv.try_recv() {
    //                 current_stats.merge_from(&stats)
    //             }
    //             // let t_end = Instant::now();
    //             // let n = current_stats.request_count;
    //             // track overall global stats
    //             global_stats.borrow_mut().merge_from(&current_stats);
    //             let _ = _perf.update(current_stats).expect("failed sending current stats to perf");
    //             // print them
    //             // let metrics = current_stats.export_metrics().into_iter();
    //             // let mut col_writer = (&mut String::new(), &mut String::new());
    //             // col_writer.write_str("blob", format!("{:?}", server_cfg.blob_store_impl).as_str());
    //             // col_writer.write_u64("requests", n as u64);
    //             // // diff here will be the println time itself
    //             // col_writer.write_u64("timestamp_ms", t_end.duration_since(global_start_time).as_millis() as u64);
    //             // col_writer.write_u64("duration_ms", t_end.duration_since(t_start).as_millis() as u64);
    //             // for (name, value) in metrics {
    //             //     col_writer.write_str(name, value.as_str());
    //             // }
    //             // if header {
    //             //     println!("{}", col_writer.0);
    //             //     header = false;
    //             // }
    //             // println!("{}", col_writer.1);
    //         }
    //     }
    // });

    {
        let gen_cfg = |tid| {
            ExecutorConfig::from(ClientThreadConfig {
                client: &bench_cfg.client,
                io: &bench_cfg.io,
                tid,
            })
        };
        // -------------------- this section is benchmarked -------------------- //
        // let _perf = make_perf_block("primary", bench_cfg, server_cfg);
        let local_ip = health.ip();
        let main_result = std::panic::AssertUnwindSafe(v2::runtime::threads::spawn_n_scoped_executors(
            &client_rt(),
            gen_cfg,
            bench_cfg.primary_thread_count,
            |tid| {
                log::trace!("spawned executor {} on thread {:?}", tid, std::thread::current().id());
                seed_thread_local_dst_rng(tid as u64 + 1);
                let global_state = global_state.clone();
                let stats_send = _perf.sender();
                async move {
                    let lstate = create_thread_state(bench_cfg, global_state, stats_send, local_ip, tid).await;
                    let stats_task = lstate.clone().spawn_stat_collector();
                    let mut tasks = Vec::new();
                    for _ in 0..bench_cfg.initial_log_count {
                        tasks.push(lstate.clone().spawn_journal_task());
                    }
                    let exec = client_exec();
                    while lstate.keep_running.get() {
                        exec.tick(10);
                        // exec.check_mailbox();
                        // let mut progress = lstate.driver.poll_recv();
                        // progress += exec.poll_generic_tasks();
                        // exec.enter_io(progress);
                        // create new journals in the given interval
                        match lstate.global.should_create_journal(tid) {
                            Ok(_) => {
                                log::trace!("thread {} should create a journal at t = {:?}", tid, Instant::now());
                                tasks.push(lstate.clone().spawn_journal_task());
                            }
                            Err(_timeout) => {
                                // exec.sleep(timeout / 2).await; // XXX
                            }
                        }; // XXX sleep on err
                    }
                    lstate.driver.wind_down_recv();
                    let _ = runtime::future::join_all(tasks).await;
                    stats_task.await;
                }
            },
        ))
        .catch_unwind()
        .await;
        if let Err(_e) = main_result {
            log::error!("main result yielded error {:?}", _e);
        }
        // wait until stat collector stops and releases lock
        run_stat_collector.store(false, Ordering::Release);
        let final_stats = loop {
            match global_stats.try_borrow_mut() {
                Ok(r) => break r,
                Err(_) => {
                    client_exec().sleep(Duration::from_millis(1)).await;
                    continue;
                }
            }
        };
        // _perf.finalize_with(final_stats.clone().export_metrics());
        output_benchmark_result("open", get_benchmark_environment(), bench_cfg, final_stats.clone());
        // ----------------------------------------------------------------------- //
    }

    // send shutdown to nodes
    let client = bind_retrying_client((health.ip(), 0), bench_cfg.driver_cfg(0, &global_state.cluster))?;
    log::info!("sending shutdown requests");
    let (replies, _remotes) = client
        .retrying_quorum_send_recv(
            JournalRequest::Shutdown,
            &bench_cfg.cluster_retry_spec(&global_state.cluster),
            global_state.cluster.clone(),
            |msg, _addr| match msg.request {
                JournalResponse::ShutdownSucessful => RetryStatus::Ok,
                JournalResponse::ShutdownCancelled => RetryStatus::Retry,
                _ => RetryStatus::Abort(format!("Unexpected reply to shutdown request: {:?}", msg)),
            },
        )
        .await?;
    log::info!("shutdown responses: {:?}", replies);

    // set own state to stopping
    health.set_state(NodeState::Stopping);
    // wait until all nodes are in 'stopping' state (or stopped)
    if health
        .await_cluster_stopped(Some(Duration::from_millis(bench_cfg.client.cluster_state_timeout_ms)))
        .await
    {
        log::info!("all loggers stopped or unreachable, stopping primary");
    } else {
        eprintln!(
            "ERROR: cluster did not stop in < {}ms; exiting primary anyway. manual cleanup required!",
            bench_cfg.client.cluster_state_timeout_ms
        );
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////
fn main() -> Result<(), anyhow::Error> {
    #[cfg(feature = "env-logger")]
    env_logger::init_from_env("RUST_LOG");
    // println!("log level {}", std::env::var("RUST_LOG").unwrap_or("unset".to_string()));
    log::info!("args {:?}", std::env::args_os());
    let bench_cfg = Box::leak(Box::new(BenchConfig::parse()));
    log::info!("using bench cfg {:?}", bench_cfg);
    let (myid, am_primary) = v2::node::cluster::node_id_from_env()?;
    if am_primary {
        v2::client::client_rt().with_executor(Default::default(), || async move {
            primary(bench_cfg).await.expect("error executing primary");
        });
    } else {
        v2::server::server_rt().with_executor(Default::default(), || async move {
            v2::bench::logger(myid, bench_cfg.op_count, &bench_cfg.server, &bench_cfg.io)
                .await
                .expect("error in logger")
        });
    }
    log::info!("stopping binary for node {}", myid);
    Ok(())
}

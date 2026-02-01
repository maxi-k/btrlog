use std::cell::Cell;
use std::collections::BTreeMap;
use std::mem::MaybeUninit;
use std::time::{Duration, Instant};

use macro_rules_attr::apply;
use perf_event_block::{BenchmarkParameterUpdates, BenchmarkParameters};
use reqtrace::*;

use crate::client::ForCluster;
use crate::io::BufferPoolConfig;
use crate::io::buffer::LocalBufferPool;
use crate::types::wire::auto::*;

////////////////////////////////////////////////////////////////////////////////
//  server-side tracing

#[apply(tracer)]
#[apply(on_wire)]
#[derive(Encode, Decode, Default)]
pub struct RequestTracer {
    pub recv_call: MicroScale,
    pub mesh_call: MicroScale,
    pub send_call: MicroScale,
    pub ht_lookup: MicroScale,
    pub seq_access: MicroScale,
    pub wal_write: MicroScale,
    pub seq_flushable_access: MicroScale,
    pub inmem_journal_push: MicroScale,
    pub store_accept: MicroScale,
    pub server_total_time: MicroScale,
    #[notrace] pub server_qlen: usize,
    #[notrace] pub server_uring_outstanding: usize
}

#[cfg(not(feature = "trace-requests"))]
static_assertions::const_assert_eq!(std::mem::size_of::<RequestTracer>(), 0);

impl RequestTracer {
    #[allow(dead_code)]
    pub(crate) fn print_if_long(&self, total_micros: f64, id: String) {
        if self.total_duration() >= Duration::from_micros(30) {
            log::warn!("total: {}us\n{}", total_micros, self.transposed_csv(id.as_str()));
        }
    }

    #[cfg(feature ="trace-requests")]
    pub(crate) fn stat_tuple(&self) -> (f64, usize, usize ) {
        (self.server_total_time.get_scaled(), self.server_qlen, self.server_uring_outstanding)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  client-side tracing

#[apply(tracer)]
pub struct ClientSideTracer {
    pub send_call: MicroScale,
    #[notrace] pub message_id: u64,
    #[notrace] pub start_timestamp: Instant,
    #[notrace] pub total_lat: Duration,
    #[notrace] pub recv_lat: Duration,
    #[notrace] pub retries: usize,
    #[notrace] pub server_traces: crate::client::ForCluster<RequestTracer>,
    #[notrace] pub server_timestamps: crate::client::ForCluster<Instant> ,
    #[notrace] pub c_qlen: usize,
    #[notrace] pub c_cqlen: usize,
    #[notrace] pub c_oio: usize,
}

impl Default for ClientSideTracer {

    #[cfg(feature = "trace-requests")]
    fn default() -> Self {
        Self::new(Instant::now())
    }

    #[cfg(not(feature = "trace-requests"))]
    fn default() -> Self {
        Self {}
    }
}

#[cfg(feature = "trace-requests")]
impl ClientSideTracer {

    pub fn new(start_timestamp: Instant) -> Self {
        Self {
            send_call: Default::default(),
            message_id: Default::default(),
            start_timestamp,
            recv_lat: Default::default(),
            total_lat: Default::default(),
            retries: Default::default(),
            server_traces: ForCluster::from_elem(Default::default(), 3),
            server_timestamps: ForCluster::from_elem(start_timestamp, 3),
            c_qlen: 0,
            c_cqlen: 0,
            c_oio: 0
        }
    }
    
    fn get_csv_header() -> &'static str {
       "msgid,start_ts,total_us,send_call_us,recv_lat_us,retries,c_qlen,c_cqlen,c_oio,s1_proc_us,s2_proc_us,s3_proc_us,s1_us,s2_us,s3_us,s1_net_us,s2_net_us,s3_net_us,s1_qlen,s2_qlen,s3_qlen,s1_oio,s2_oio,s3_oio" 
    }

    fn get_csv_row(&self, global_start: Instant) -> String {
        let ( c_qlen, c_oio, c_cqlen, msgid ) = (self.c_qlen, self.c_oio, self.c_cqlen,self.message_id);
        let start_ts = self.start_timestamp.duration_since(global_start).as_micros();
        let send_call_us = self.send_call.get_scaled();
        let recv_lat_us = self.recv_lat.as_micros();
        let total_us = self.total_lat.as_micros();
        let retries = self.retries;
        let (s1_proc_us, s1_qlen, s1_oio) = self.server_traces[0].stat_tuple();
        let (s2_proc_us, s2_qlen, s2_oio) = self.server_traces[1].stat_tuple();
        let (s3_proc_us, s3_qlen, s3_oio) = self.server_traces[2].stat_tuple();
        let s1_us = self.server_timestamps[0].duration_since(self.start_timestamp).as_micros();
        let s2_us = self.server_timestamps[1].duration_since(self.start_timestamp).as_micros();
        let s3_us = self.server_timestamps[2].duration_since(self.start_timestamp).as_micros();
        let (s1_net_us, s2_net_us, s3_net_us) = (s1_us as f64 - s1_proc_us as f64, s2_us as f64 - s2_proc_us as f64, s3_us as f64 - s3_proc_us as f64);
        format!("{msgid},{start_ts},{total_us},{send_call_us:.2},{recv_lat_us},{retries},{c_qlen},{c_cqlen},{c_oio},{s1_proc_us:.2},{s2_proc_us:.2},{s3_proc_us:.2},{s1_us},{s2_us},{s3_us},{s1_net_us:.2},{s2_net_us:.2},{s3_net_us:.2},{s1_qlen},{s2_qlen},{s3_qlen},{s1_oio},{s2_oio},{s3_oio}")
    }
}

pub struct ClientRequestTraceBuffer {
    mem: LocalBufferPool,
    write_cursor: Cell<*mut ClientSideTracer>,
    end_cursor: *mut ClientSideTracer,
    start_ts: Instant
}

impl ClientRequestTraceBuffer {
    pub fn new() -> Self {
        const ELEM_SIZE: usize = std::mem::size_of::<ClientSideTracer>();
        let max_elems = 1000 * 1000;
        let pool = LocalBufferPool::new(ELEM_SIZE, BufferPoolConfig::build_with(|cfg| {
                cfg.prealloc_size = max_elems*ELEM_SIZE
        }));
        let write_cursor: *mut ClientSideTracer = unsafe { std::mem::transmute(pool.vm_start()) };
        let end_cursor = write_cursor.wrapping_add(max_elems);
        Self {
            mem: pool,
            write_cursor: Cell::new(write_cursor),
            end_cursor: end_cursor,
            start_ts: Instant::now()
        }
    }

    pub fn push(&self, tracer: ClientSideTracer) {
        let ptr = self.write_cursor.get();
        self.write_cursor.update(|v| {
            if v == self.end_cursor {
                 unsafe { std::mem::transmute(self.mem.vm_start()) }
            }  else {
                v.wrapping_add(1)
            }
        });
        unsafe { ptr.write(tracer) }
    }

    #[cfg(feature = "trace-requests")]
    pub fn generate_csv(&self) {
        use std::io::{BufWriter, Write};
        let start = unsafe { std::mem::transmute(self.mem.vm_start()) };
        let count = unsafe { self.end_cursor.offset_from_unsigned(start) } ;
        let requests: &[ClientSideTracer] = unsafe { std::slice::from_raw_parts(start, count) };
        let requests = requests.split_last().unwrap().1; // last request might've failed to write latencies to the tracer
        let filename = format!("request-trace-{}.csv", std::time::SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs());
        let file = match std::fs::File::create(&filename) {
            Ok(file) => file,
            Err(err) => {
                log::error!("failed to create request trace file {}: {}", filename, err);
                return;
            }
        };
        let mut writer = BufWriter::new(file);
        if let Err(err) = writeln!(writer, "{}", ClientSideTracer::get_csv_header()) {
            log::error!("failed to write request trace header: {}", err);
            return;
        }
        for request in requests {
            if let Err(err) = writeln!(writer, "{}", request.get_csv_row(self.start_ts)) {
                log::error!("failed to write request trace row: {}", err);
                break;
            }
        }
    }
}

#[cfg(feature = "trace-requests")]
impl Drop for ClientRequestTraceBuffer {
    fn drop(&mut self) {
        self.generate_csv();
    }
}

////////////////////////////////////////////////////////////////////////////////

#[apply(tracer)]
#[derive(Default)]
pub struct OpenBenchTracer {
    pub send_call: MicroScale,
    pub recv_call: MicroScale,
    pub sleep_time: MicroScale,
}

#[derive(Default)]
#[cfg(feature = "trace-requests")]
pub struct TaskPhaseTracker {
    pub name: String,
    pub polls: u64,
    pub poll_cycles: u64,
    pub wait_cycles: u64,
    pub active_cycles: u64,
    pub last_transition_ts: CycleMeasurement,
}

#[cfg(feature = "trace-requests")]
impl TaskPhaseTracker {
    #[inline]
    pub const fn zero() -> Self {
        Self {
            name: String::new(),
            polls: 0,
            poll_cycles: 0,
            wait_cycles: 0,
            active_cycles: 0,
            last_transition_ts: CycleMeasurement::zero(),
        }
    }

    #[inline]
    pub fn new_active(name: String) -> Self {
        let mut res = Self::default();
        res.name = name;
        res.last_transition_ts.start_measurement();
        res
    }

    #[inline]
    pub fn on_deactivate(&mut self) {
        let dur = self.last_transition_ts.next_measurement();
        self.active_cycles += dur;
    }

    #[inline]
    pub fn on_poll_done(&mut self) {
        let dur = self.last_transition_ts.next_measurement();
        self.poll_cycles += dur;
        self.polls += 1;
    }

    #[inline]
    pub fn on_wait_done(&mut self) {
        let dur = self.last_transition_ts.next_measurement();
        self.wait_cycles += dur;
    }

    #[inline]
    pub fn as_micros(&self) -> (f64, f64, f64, u64) {
        let poll_micros = MicroScale::scale_cycles(self.poll_cycles);
        let active_micros = MicroScale::scale_cycles(self.active_cycles);
        let wait_micros = MicroScale::scale_cycles(self.wait_cycles);
        (poll_micros, active_micros, wait_micros, self.polls)
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

#[derive(Default)]
#[cfg(not(feature = "trace-requests"))]
pub struct TaskPhaseTracker {}

#[cfg(not(feature = "trace-requests"))]
impl TaskPhaseTracker {
    #[inline]
    pub const fn zero() -> Self {
        Self {}
    }
    #[inline]
    pub fn new_active(_name: String) -> Self {
        Self::default()
    }
    #[inline]
    pub fn on_deactivate(&mut self) {}
    #[inline]
    pub fn on_poll_done(&mut self) {}
    #[inline]
    pub fn on_wait_done(&mut self) {}
    #[inline]
    pub fn as_micros(&self) -> (f64, f64, f64, u64) {
        (0.0, 0.0, 0.0, 0)
    }
    #[inline]
    pub fn name(&self) -> &str {
        ""
    }
}

////////////////////////////////////////////////////////////////////////////////
//  summarized metrics

#[derive(Debug, Clone)]
pub struct Metric<T> {
    pub min: T,
    pub max: T,
    pub total: T,
}

impl<T> Metric<T>
where
    T: Copy + PartialOrd + std::ops::Add<Output = T>,
{
    pub fn new(initial_min: T, initial_max: T, initial_total: T) -> Self {
        Self {
            min: initial_min,
            max: initial_max,
            total: initial_total,
        }
    }

    pub fn record(&mut self, value: T) {
        if value < self.min {
            self.min = value;
        }
        if value > self.max {
            self.max = value;
        }
        self.total = self.total + value;
    }

    pub fn merge(&mut self, other: &Self) {
        if other.min < self.min {
            self.min = other.min;
        }
        if other.max > self.max {
            self.max = other.max;
        }
        self.total = self.total + other.total;
    }
}

impl Metric<usize> {
    pub fn avg(&self, count: usize) -> usize {
        if count == 0 { 0 } else { self.total / count }
    }
}

impl Metric<Duration> {
    pub fn avg(&self, count: usize) -> Duration {
        if count == 0 {
            Duration::ZERO
        } else {
            self.total / count as u32
        }
    }
}

impl Default for Metric<Duration> {
    fn default() -> Self {
        Self::new(Duration::MAX, Duration::ZERO, Duration::ZERO)
    }
}

impl Default for Metric<usize> {
    fn default() -> Self {
        Self::new(usize::MAX, 0, 0)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  generic way of exporting a set of metrics

pub trait MetricMap: Default + Send {
    fn merge_from(&mut self, other: &Self);
    fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)>;
    fn merge_all(iter: impl IntoIterator<Item = Self>) -> Self {
        let mut combined = Self::default();
        for item in iter {
            combined.merge_from(&item);
        }
        combined
    }

    fn get_scale_metric(&self) -> Option<u64> {
        None
    }
}

impl<T1: MetricMap, T2: MetricMap> MetricMap for (T1, T2) {
    fn merge_from(&mut self, other: &Self) {
        self.0.merge_from(&other.0);
        self.1.merge_from(&other.1);
    }

    fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)> {
        let i0 = self.0.export_metrics();
        let i1 = self.1.export_metrics();
        i0.into_iter().chain(i1)
    }

    fn get_scale_metric(&self) -> Option<u64> {
        match self.0.get_scale_metric() {
            Some(s) => Some(s + self.1.get_scale_metric().unwrap_or(0)),
            None => self.1.get_scale_metric(),
        }
    }
}

#[derive(Default)]
pub struct MetricMapToBenchmarkParams<T: MetricMap>(pub T);
impl<T: MetricMap + 'static> MetricMapToBenchmarkParams<T> {
    pub fn new(t: T) -> Self {
        Self(t)
    }
}
impl<T: MetricMap + 'static> BenchmarkParameterUpdates for MetricMapToBenchmarkParams<T> {
    fn apply(self, params: &mut BenchmarkParameters, scale: &mut u64) {
        if let Some(new_scale) = self.0.get_scale_metric() {
            *scale = new_scale;
        }
        params.set_all(self.0.export_metrics())
    }

    fn apply_all<I>(params: &mut BenchmarkParameters, scale: &mut u64, iter: I)
    where
        Self: Sized,
        I: Iterator<Item = Self>,
    {
        let combined = Self(T::merge_all(iter.map(|s| s.0)));
        combined.apply(params, scale);
    }
}

impl<T: MetricMap> From<T> for MetricMapToBenchmarkParams<T> {
    fn from(value: T) -> Self {
        MetricMapToBenchmarkParams(value)
    }
}

////////////////////////////////////////////////////////////////////////////////
//  detailed percentile tracking

#[derive(Debug, Clone)]
pub struct PercentileTracker {
    histogram: BTreeMap<u64, usize>,
}

impl PercentileTracker {
    pub fn new() -> Self {
        Self {
            histogram: BTreeMap::new(),
        }
    }

    pub fn record(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        *self.histogram.entry(micros).or_insert(0) += 1;
    }

    pub fn percentile(&self, total_count: usize, percentile: f64) -> Duration {
        if total_count == 0 {
            return Duration::ZERO;
        }

        let target_count = (total_count as f64 * percentile / 100.0).ceil() as usize;
        let mut current_count = 0;

        for (&micros, &count) in &self.histogram {
            current_count += count;
            if current_count >= target_count {
                return Duration::from_micros(micros);
            }
        }

        Duration::ZERO
    }

    pub fn merge(&mut self, other: &Self) {
        for (&micros, &count) in &other.histogram {
            *self.histogram.entry(micros).or_insert(0) += count;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
//  track duration metrics (min/max/avg) and percentiles, if enabled

#[derive(Clone, Debug)]
pub struct DurationStats {
    metric: Metric<Duration>,
    percentile: PercentileTracker,
    count: usize,
    track_percentile: bool,
}

impl Default for DurationStats {
    fn default() -> Self {
        Self::new(cfg!(feature = "trace-percentiles"))
    }
}

impl DurationStats {
    pub fn new(track_percentile: bool) -> Self {
        Self {
            metric: Metric::default(),
            percentile: PercentileTracker::new(),
            count: 0,
            track_percentile,
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }
    pub fn min(&self) -> Duration {
        self.metric.min
    }
    pub fn max(&self) -> Duration {
        self.metric.max
    }
    pub fn avg(&self) -> Duration {
        self.metric.avg(self.count)
    }

    pub fn record(&mut self, duration: Duration) {
        self.metric.record(duration);
        self.count += 1;
        if self.track_percentile {
            self.percentile.record(duration);
        }
    }

    pub fn percentile(&self, percentile: f64) -> Duration {
        if self.track_percentile {
            return self.percentile.percentile(self.count, percentile);
        }
        Duration::MAX
    }

    pub fn percentile_us_str(&self, percentile: f64) -> String {
        self.percentile(percentile).as_micros().to_string()
    }

    pub fn merge(&mut self, other: &Self) {
        self.metric.merge(&other.metric);
        self.count += other.count;
        if self.track_percentile {
            debug_assert!(other.track_percentile);
            self.percentile.merge(&other.percentile)
        } else if other.track_percentile {
            panic!("merging with only one DurationStats instance configured to track percentiles.")
        }
    }

    pub fn tracks_percentiles(&self) -> bool {
        self.track_percentile
    }

    pub async fn trace_async_op<T>(&mut self, op: impl Future<Output = T>) -> T {
        let start = Instant::now();
        let result = op.await;
        self.record(start.elapsed());
        result
    }
}

use crate::trace::MetricMap;
use std::time::Duration;
use std::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// with latency tracing enabled

crate::util::ifcfg! {
    [feature = "trace-latencies"]

    use crate::trace::{Metric, DurationStats};

    #[derive(Clone)]
    pub struct WalStats {
        io_latency: DurationStats,
        buffer_age: DurationStats,
        buffer_used_bytes: Metric<usize>,
        io_bytes: Metric<usize>,
        chain_length: Metric<usize>,
        chain_records: usize,
        yield_polls: usize,
        push_count: usize,
        flush_count_sync: usize,
        flush_count_async: usize,
        flush_count_explicit: usize,
    }

    impl Default for WalStats {
        fn default() -> Self {
            Self::new()
        }
    }

    #[rustfmt::skip]
    impl WalStats {
        pub(super) fn new() -> Self {
            Self {
                io_latency: DurationStats::default(),
                buffer_age: DurationStats::default(),
                buffer_used_bytes: Metric::default(),
                io_bytes: Metric::default(),
                chain_length: Metric::default(),
                chain_records: 0,
                yield_polls: 0,
                push_count: 0,
                flush_count_sync: 0,
                flush_count_async: 0,
                flush_count_explicit: 0
            }
        }

        #[inline] pub(super) fn record_io(&mut self, _start: Instant, bytes: usize) {
            self.io_latency.record(_start.elapsed());
            self.io_bytes.record(bytes);
        }

        #[inline] pub(super) fn record_sync_buffer_flush(&mut self, age: Duration, bytes: usize) {
            self.buffer_age.record(age);
            self.buffer_used_bytes.record(bytes);
            self.flush_count_sync += 1;
        }

        #[inline] pub(super) fn on_background_flush(&mut self) {
            self.flush_count_async += 1;
        }

        #[inline] pub(super) fn on_explicit_flush(&mut self) {
            self.flush_count_explicit += 1;
        }

        #[inline] pub(super) fn record_chain_length(&mut self, _len: usize) {
            self.chain_length.record(_len);
            self.chain_records += 1;
        }

        #[inline] pub(super) fn on_yield(&mut self) {
            self.yield_polls += 1;
        }

        #[inline] pub(super) fn on_push(&mut self) {
            self.push_count += 1;
        }
    }

    impl MetricMap for WalStats {
        fn merge_from(&mut self, other: &Self) {
            self.io_latency.merge(&other.io_latency);
            self.buffer_age.merge(&other.buffer_age);
            self.buffer_used_bytes.merge(&other.buffer_used_bytes);
            self.io_bytes.merge(&other.io_bytes);
            self.chain_length.merge(&other.chain_length);
            self.chain_records += other.chain_records;
            self.yield_polls += other.yield_polls;
            self.push_count += other.push_count;
            self.flush_count_sync += other.flush_count_sync;
            self.flush_count_async += other.flush_count_async;
            self.flush_count_explicit += other.flush_count_explicit;
        }

        fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)> {
            let basic = [
                ("wal_push_count", self.push_count.to_string()),
                ("wal_yield_polls", self.yield_polls.to_string()),
                ("wal_avg_yields", self.yield_polls.checked_div(self.push_count).unwrap_or(0).to_string()),
                ("wal_chain_max", self.chain_length.max.to_string()),
                ("wal_chain_avg", self.chain_length.avg(self.chain_records).to_string()),
                ("wal_chain_uses", self.chain_records.to_string()),
                ("wal_io_count", self.io_latency.count().to_string()),
                ("wal_io_bytes_min", self.io_bytes.min.to_string()),
                ("wal_io_bytes_max", self.io_bytes.max.to_string()),
                ("wal_io_bytes_avg", self.io_bytes.avg(self.io_latency.count()).to_string()),
                ("wal_io_lat_avg", self.io_latency.avg().as_micros().to_string()),
                ("wal_io_lat_max", self.io_latency.max().as_micros().to_string()),
                ("wal_bufage_us_avg", self.buffer_age.avg().as_micros().to_string()),
                ("wal_bufage_us_max", self.buffer_age.max().as_micros().to_string()),
                ("wal_bufbytes_avg", self.buffer_used_bytes.avg(self.flush_count_sync).to_string()),
                ("wal_bufbytes_max", self.buffer_used_bytes.max.to_string()),
                ("wal_bufbytes_min", self.buffer_used_bytes.min.to_string()),
                ("wal_flush_sync", self.flush_count_sync.to_string()),
                ("wal_flush_async", self.flush_count_async.to_string()),
                ("wal_flush_explicit", self.flush_count_explicit.to_string()),
            ];

            #[cfg(not(feature = "trace-percentiles"))]
            return basic;

            #[cfg(feature = "trace-percentiles")]
            basic.into_iter().chain([
                ("wal_bufage_us_p99", self.buffer_age.percentile_us_str(99.0)),
                ("wal_bufage_us_p95", self.buffer_age.percentile_us_str(95.0)),
                ("wal_bufage_us_p50", self.buffer_age.percentile_us_str(50.0)),
                ("wal_io_us_p99", self.io_latency.percentile_us_str(99.0)),
                ("wal_io_us_p95", self.io_latency.percentile_us_str(95.0)),
                ("wal_io_us_p50", self.io_latency.percentile_us_str(50.0)),
            ])
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// without latency tracing

crate::util::ifcfg! {
    [not(feature = "trace-latencies")]

    #[derive(Default, Clone)]
    pub struct WalStats {}

    #[rustfmt::skip]
    impl WalStats {
        pub(super) fn new() -> Self {Self {}}
        #[inline] pub(super) fn record_io(&mut self, _start: Instant, _bytes: usize) {}
        #[inline] pub(super) fn record_sync_buffer_flush(&mut self, _age: Duration, _bytes: usize) {}
        #[inline] pub(super) fn on_background_flush(&mut self) {}
        #[inline] pub(super) fn on_explicit_flush(&mut self) {}
        #[inline] pub(super) fn record_chain_length(&mut self, _len: usize) {}
        #[inline] pub(super) fn on_yield(&mut self) {}
        #[inline] pub(super) fn on_push(&mut self) {}
    }

    impl MetricMap for WalStats {
        fn merge_from(&mut self, _other: &Self) {}

        fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)> {
            []
        }
    }
}

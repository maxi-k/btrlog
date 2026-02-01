use crate::trace::MetricMap;
use std::time::Instant;

////////////////////////////////////////////////////////////////////////////////
// with latency tracing enabled

crate::util::ifcfg! {
    [feature = "trace-latencies"]

    use crate::trace::{Metric, DurationStats};

    #[derive(Clone)]
    pub struct JournalStats {
        wal_latency: DurationStats,
        e2e_latency: DurationStats,
        push_requests: usize,
        push_errors: usize,
        push_superseded: usize,
        journal_yields: usize,
        blob_flushes: usize,
    }

    impl Default for JournalStats {
        fn default() -> Self {
            Self::new()
        }
    }

    #[rustfmt::skip]
    #[allow(dead_code)]
    impl JournalStats {
        pub(super) fn new() -> Self {
            Self {
                wal_latency: DurationStats::default(),
                e2e_latency: DurationStats::default(),
                push_requests: 0,
                push_errors: 0,
                push_superseded: 0,
                journal_yields: 0,
                blob_flushes: 0,
            }
        }

        #[inline]
        pub(super) fn record_wal_latency(&mut self, start: Instant) {
            self.wal_latency.record(start.elapsed());
        }

        #[inline]
        pub(super) fn record_e2e_latency(&mut self, start: Instant) {
            self.e2e_latency.record(start.elapsed());
        }

        #[inline] pub(super) fn on_push(&mut self) {self.push_requests += 1;}
        #[inline] pub(super) fn on_push_error(&mut self) {self.push_errors += 1;}
        #[inline] pub(super) fn on_push_superseded(&mut self) {self.push_superseded += 1;}
        #[inline] pub(super) fn on_flush(&mut self) {self.blob_flushes += 1;}
        #[inline] pub(super) fn on_yield(&mut self) {self.journal_yields += 1;}
    }

    impl MetricMap for JournalStats {
        fn merge_from(&mut self, other: &Self) {
            self.push_requests += other.push_requests;
            self.blob_flushes += other.blob_flushes;
            self.push_errors += other.push_errors;
            self.push_superseded += other.push_superseded;
            self.journal_yields += other.journal_yields;
            self.wal_latency.merge(&other.wal_latency);
            self.e2e_latency.merge(&other.e2e_latency);
        }

        fn get_scale_metric(&self) -> Option<u64> {
            Some(self.push_requests as u64)
        }

        fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)> {
            let basic = [
                ("blob_flushes", self.blob_flushes.to_string()),
                ("push_count", self.push_requests.to_string()),
                ("push_errors", self.push_errors.to_string()),
                ("push_superseded", self.push_superseded.to_string()),
                ("journal_yields", self.journal_yields.to_string()),
                ("e2e_lat_avg", self.e2e_latency.avg().as_micros().to_string()),
                ("e2e_lat_max", self.e2e_latency.max().as_micros().to_string()),
                ("wal_lat_avg", self.wal_latency.avg().as_micros().to_string()),
                ("wal_lat_max", self.wal_latency.max().as_micros().to_string()),
            ];

            #[cfg(not(feature = "trace-percentiles"))]
            return basic;

            #[cfg(feature = "trace-percentiles")]
            basic.into_iter().chain([
                ("e2e_us_p99", self.e2e_latency.percentile_us_str(99.0)),
                ("e2e_us_p95", self.e2e_latency.percentile_us_str(95.0)),
                ("e2e_us_p50", self.e2e_latency.percentile_us_str(50.0)),
                ("wal_us_p99", self.wal_latency.percentile_us_str(99.0)),
                ("wal_us_p95", self.wal_latency.percentile_us_str(95.0)),
                ("wal_us_p50", self.wal_latency.percentile_us_str(50.0)),
            ])
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// without latency tracing

crate::util::ifcfg! {
   [not(feature = "trace-latencies")]

    #[derive(Clone, Default)]
    pub struct JournalStats {}

    #[rustfmt::skip]
    #[allow(dead_code)]
    impl JournalStats {
        pub(super) fn new() -> Self {Self {}}
        #[inline] pub(super) fn record_wal_latency(&mut self, _start: Instant) {}
        #[inline] pub(super) fn record_e2e_latency(&mut self, _start: Instant) {}
        #[inline] pub(super) fn on_push(&mut self) {}
        #[inline] pub(super) fn on_push_error(&mut self) {}
        #[inline] pub(super) fn on_push_superseded(&mut self) {}
        #[inline] pub(super) fn on_flush(&mut self) {}
        #[inline] pub(super) fn on_yield(&mut self) {}
    }

    impl MetricMap for JournalStats {
        fn merge_from(&mut self, _other: &Self) {}

        fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)> {
            []
        }
    }

}

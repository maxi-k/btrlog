use perf_event_block::{BenchmarkParameterUpdates, BenchmarkParameters};

use crate::trace::{Metric, MetricMap, PercentileTracker};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClientStats {
    pub majority_latency: Metric<Duration>,
    pub send_latency: Metric<Duration>,
    pub request_size: Metric<usize>,
    pub retries: Metric<usize>,
    pub request_count: usize,
    pub journals_created: usize,
    pub majority_latency_percentiles: PercentileTracker,
    pub send_latency_percentiles: PercentileTracker,
    pub track_percentiles: bool,
}

impl Default for ClientStats {
    fn default() -> Self {
        Self::new(cfg!(feature = "trace-percentiles"))
    }
}

impl ClientStats {
    pub fn new(track_percentiles: bool) -> Self {
        Self {
            majority_latency: Metric::default(),
            send_latency: Metric::default(),
            request_size: Metric::default(),
            retries: Metric::default(),
            request_count: 0,
            journals_created: 0,
            majority_latency_percentiles: PercentileTracker::new(),
            send_latency_percentiles: PercentileTracker::new(),
            track_percentiles,
        }
    }

    pub fn merge_all(stats: impl Iterator<Item = ClientStats>) -> ClientStats {
        let mut merged = ClientStats::new(false);
        for stat in stats {
            if stat.track_percentiles {
                merged.track_percentiles = true;
            }
            merged.merge_from(&stat);
        }
        merged
    }

    pub fn on_create_journal(&mut self) {
        self.journals_created += 1;
    }

    pub fn record_request(&mut self, majority_latency: Duration, send_latency: Duration, request_size: usize, retries: usize) {
        self.majority_latency.record(majority_latency);
        self.send_latency.record(send_latency);
        self.request_size.record(request_size);
        self.retries.record(retries);
        self.request_count += 1;

        if self.track_percentiles {
            self.majority_latency_percentiles.record(majority_latency);
            self.send_latency_percentiles.record(send_latency);
        }
    }

    pub fn avg_majority_latency(&self) -> Duration {
        self.majority_latency.avg(self.request_count)
    }

    pub fn avg_request_size(&self) -> f64 {
        if self.request_count == 0 {
            0.0
        } else {
            self.request_size.total as f64 / self.request_count as f64
        }
    }

    pub fn avg_send_latency(&self) -> Duration {
        self.send_latency.avg(self.request_count)
    }

    pub fn avg_retries(&self) -> f64 {
        if self.request_count == 0 {
            0.0
        } else {
            self.retries.total as f64 / self.request_count as f64
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

impl MetricMap for ClientStats {
    fn merge_from(&mut self, other: &Self) {
        if other.request_count == 0 {
            return;
        }
        if self.request_count == 0 {
            *self = other.clone();
            return;
        }

        self.majority_latency.merge(&other.majority_latency);
        self.send_latency.merge(&other.send_latency);
        self.request_size.merge(&other.request_size);
        self.retries.merge(&other.retries);
        self.request_count += other.request_count;
        self.majority_latency_percentiles.merge(&other.majority_latency_percentiles);
        self.send_latency_percentiles.merge(&other.send_latency_percentiles);
        self.journals_created += other.journals_created;
    }

    fn export_metrics(self) -> impl IntoIterator<Item = (&'static str, String)> {
        // subselection for narrower csv
        let mut columns = vec![
            // ("maj_lat_min_us", self.majority_latency.min.as_micros().to_string()),
            // ("maj_lat_max_us", self.majority_latency.max.as_micros().to_string()),
            ("maj_lat_avg_us", self.avg_majority_latency().as_micros().to_string()),
            // ("send_lat_min_us", self.send_latency.min.as_micros().to_string()),
            // ("send_lat_max_us", self.send_latency.max.as_micros().to_string()),
            ("send_lat_avg_us", self.avg_send_latency().as_micros().to_string()),
            // ("req_size_min", self.request_size.min.to_string()),
            // ("req_size_max", self.request_size.max.to_string()),
            ("req_size_avg", format!("{:.2}", self.avg_request_size())),
            // ("retries_min", self.retries.min.to_string()),
            ("retries_max", self.retries.max.to_string()),
            ("retries_avg", format!("{:.8}", self.avg_retries())),
            ("req_cnt", self.request_count.to_string()),
            ("journals_created", self.journals_created.to_string()),
        ];

        if self.track_percentiles {
            let maj = &self.majority_latency_percentiles;
            let snd = &self.send_latency_percentiles;
            let tot = self.request_count;
            columns.push(("maj_lat_p50_us", maj.percentile(tot, 50.0).as_micros().to_string()));
            columns.push(("maj_lat_p95_us", maj.percentile(tot, 95.0).as_micros().to_string()));
            columns.push(("maj_lat_p99_us", maj.percentile(tot, 99.0).as_micros().to_string()));
            columns.push(("maj_lat_p99.9_us", maj.percentile(tot, 99.9).as_micros().to_string()));
            columns.push(("snd_lat_p50_us", snd.percentile(tot, 50.0).as_micros().to_string()));
            columns.push(("snd_lat_p95_us", snd.percentile(tot, 95.0).as_micros().to_string()));
            columns.push(("snd_lat_p99_us", snd.percentile(tot, 99.0).as_micros().to_string()));
            columns.push(("snd_lat_p99.9_us", snd.percentile(tot, 99.9).as_micros().to_string()));
        }
        columns
    }
}

////////////////////////////////////////////////////////////////////////////////

impl BenchmarkParameterUpdates for ClientStats {
    fn apply(self, params: &mut BenchmarkParameters, scale: &mut u64) {
        *scale = self.request_count as u64;
        params.set_all(self.export_metrics())
    }

    fn apply_all<I>(params: &mut BenchmarkParameters, scale: &mut u64, iter: I)
    where
        Self: Sized,
        I: Iterator<Item = Self>,
    {
        let combined = Self::merge_all(iter);
        combined.apply(params, scale);
    }
}

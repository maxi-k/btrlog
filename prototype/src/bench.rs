use perf_event_block::PerfEventTimesliced;
use serde::{Deserialize, Serialize};

use crate::{
    config::{IOConfig, ServerConfig},
    node::health::ClusterHealth,
    server::{ServerShutdownResult, server_exec},
    trace::MetricMap,
    types::message::NodeState,
};
use std::time::Duration;

pub static MAX_MSG_SIZE: usize = 1 << 15;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BenchmarkEnvironment {
    cluster_size: String,
    journal_platform: String,
    logger_instance_type: String,
    primary_instance_type: String,
    placement_group: String,
    placement_partition: String,
    aws_az_id: String,
    aws_region: String,
    timestamp: String,
}

pub fn get_benchmark_environment() -> BenchmarkEnvironment {
    let timestamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs().to_string();
    BenchmarkEnvironment {
        journal_platform: std::env::var("JOURNAL_PLATFORM").unwrap_or_else(|_| "unknown".to_string()),
        cluster_size: std::env::var("JOURNAL_CLUSTER_SIZE").unwrap_or_else(|_| "unknown".to_string()),
        logger_instance_type: std::env::var("JOURNAL_LOGGER_INSTANCE").unwrap_or_else(|_| "unknown".to_string()),
        primary_instance_type: std::env::var("JOURNAL_PRIMARY_INSTANCE").unwrap_or_else(|_| "unknown".to_string()),
        placement_group: std::env::var("AWS_PLACEMENT_GROUP").unwrap_or_else(|_| "unknown".to_string()),
        placement_partition: std::env::var("AWS_PLACEMENT_PARTITION").unwrap_or_else(|_| "unknown".to_string()),
        aws_region: std::env::var("AWS_REGION").unwrap_or_else(|_| "unknown".to_string()),
        aws_az_id: std::env::var("AWS_AZ_ID").unwrap_or_else(|_| "unknown".to_string()),
        timestamp,
    }
}

pub fn output_benchmark_result<Cfg: Serialize>(benchmark_name: &str, env: BenchmarkEnvironment, cfg: Cfg, stats: impl MetricMap) {
    let mut output = serde_json::Map::new();
    output.insert("system".to_string(), serde_json::Value::String("btrlog".to_string()));
    output.insert("benchmark_name".to_string(), serde_json::Value::String(benchmark_name.to_string()));
    output.insert("environment".to_string(), serde_json::to_value(env).expect("error serializing BenchEnvironment"));
    output.insert("config".to_string(), serde_json::to_value(cfg).expect("error serializing BenchConfig"));
    output.insert(
        "statistics".to_string(),
        serde_json::Value::Object(
            stats
                .export_metrics()
                .into_iter()
                .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.clone())))
                .collect(),
        ),
    );
    // Output JSON to stdout for easy copying from remote servers
    match serde_json::to_string(&output) {
        // to_string_pretty
        Ok(json_content) => {
            println!("\n=== BENCHMARK RESULT JSON ===");
            println!("{}", json_content);
            println!("=== END BENCHMARK RESULT ===\n");
        }
        Err(e) => {
            log::warn!("Failed to serialize benchmark result to JSON: {}", e);
        }
    }
}

pub async fn logger(
    myid: usize,
    op_count: usize,
    server_cfg: &'static ServerConfig,
    io_config: &'static IOConfig,
) -> Result<(), anyhow::Error> {
    use perf_event_block::{BenchmarkParameters, PerfEventBlock};
    // wait for cluster
    let health = ClusterHealth::init_cluster(server_cfg).await?;
    log::info!("node {} got initialized cluster, starting logger...", myid);
    // start server runtime
    let base_addr = (health.ip(), server_cfg.port).into();
    server_exec().spawn_task(async move {
        // this tries to ensure all threads are ready and listening
        // on network sockets before the primary starts the benchmark.
        // it does mean that perf block timing on logger nodes may be
        // ~100ms too large, but primary timing is what matters anyway
        // (since the primary may start arbitrariliy late)
        server_exec().sleep(Duration::from_millis(2000)).await;
        health.set_state(NodeState::Ready);
    });
    let results = crate::with_backend!(*server_cfg, (store, wal) => {
        let store = store.await; let wal = wal.await;
        let params = BenchmarkParameters::new([
            ("node", format!("{}", myid)),
            ("blob", format!("{:?}", server_cfg.blob_store_impl)),
        ]);
        // let _perf = PerfEventBlock::default_events(op_count as u64, params, perf_event_block::PrintMode::Transposed);
        let (results, stats) = crate::server::run_message_server(
            server_cfg,
            io_config,
            base_addr,
            |id| crate::server::journal::make_message_receiver(id, store.clone(), wal.clone(), io_config.lsn_window)
        ).await;
        log::info!("message server stopped");
        // let (results, stats) = crate::server::run_journal_server(
        //     server_cfg.clone(),
        //     base_addr,
        //     |id| make_message_receiver(id, store.clone(), wal.clone())
        // ).await;
        // _perf.finalize_with(stats.export_metrics());
        results
    });
    // publish own 'stopping' state
    health.set_state(NodeState::Stopping);
    // wait for other nodes to be stopped
    if health.await_cluster_stopped(Some(Duration::from_millis(5000))).await {
        log::info!("all nodes stopped or unreachable, stopping logger {}", myid);
    } else {
        eprintln!(
            "ERROR: cluster did not stop in < {}ms; exiting logger {} anyway. manual cleanup required!",
            myid, 5000
        );
    }
    // check whether any thread returned an error
    match results.iter().find(|f| **f != ServerShutdownResult::Ok) {
        Some(err) => Err(anyhow::anyhow!(format!("server shutdown error {:?}", err))),
        None => Ok(()),
    }
}

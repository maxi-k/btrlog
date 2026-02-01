use clap::Parser;
use serde::{Deserialize, Serialize};

use v2::{
    bench::logger,
    config::{IOConfig, ServerConfig},
    runtime::ThreadRuntime,
    server::server_rt,
};

#[derive(Parser, Debug, Clone, Serialize, Deserialize)]
#[command(author, version, about = "Journal Service Logger")]
struct LoggerConfig {
    #[command(flatten)]
    server: ServerConfig,

    #[command(flatten)]
    io: IOConfig,
}

fn main() -> anyhow::Result<()> {
    #[cfg(feature = "env-logger")]
    env_logger::init_from_env("RUST_LOG");
    // println!("log level {}", std::env::var("RUST_LOG").unwrap_or("unset".to_string()));
    log::info!("args {:?}", std::env::args_os());
    let bench_cfg = Box::leak(Box::new(LoggerConfig::parse()));
    log::info!("using bench cfg {:?}", bench_cfg);
    let (myid, am_primary) = v2::node::cluster::node_id_from_env()?;
    if am_primary {
        panic!("trying to start logger binary on primary cluster");
    }
    server_rt().with_executor(Default::default(), || async move {
        logger(myid, 1, &bench_cfg.server, &bench_cfg.io).await.expect("error in logger")
    });
    Ok(())
}

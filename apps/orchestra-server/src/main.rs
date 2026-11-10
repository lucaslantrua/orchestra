//! ORCHESTRA control plane: state engine, controllers and API in one process.

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context;
use clap::Parser;
use orchestra_controller::Config;
use orchestra_state::Store;

#[derive(Parser)]
#[command(version, about = "ORCHESTRA control plane")]
struct Args {
    /// Address of the REST API. The API is unauthenticated for now: keep it
    /// on loopback or a trusted network.
    #[arg(long, env = "ORCHESTRA_LISTEN", default_value = "127.0.0.1:7600")]
    listen: SocketAddr,

    /// Directory holding the durable state log.
    #[arg(long, env = "ORCHESTRA_DATA_DIR", default_value = "orchestra-data")]
    data_dir: PathBuf,

    /// Keep all state in memory (lost on exit).
    #[arg(long, conflicts_with = "data_dir")]
    ephemeral: bool,

    /// Seconds without heartbeat before a node's instances are rescheduled.
    #[arg(long, default_value_t = 20)]
    node_grace_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();

    let store = if args.ephemeral {
        tracing::warn!("running with ephemeral state");
        Store::in_memory()
    } else {
        let path = args.data_dir.join("state.log");
        let store = Store::open(&path).with_context(|| format!("opening state log {}", path.display()))?;
        tracing::info!(path = %path.display(), revision = store.revision(), "state loaded");
        store
    };
    let store = Arc::new(store);

    let config = Config {
        node_grace: Duration::from_secs(args.node_grace_secs),
        observing_since_ms: orchestra_domain::now_ms(),
        ..Config::default()
    };
    let controllers = tokio::spawn(orchestra_controller::run(store.clone(), config));

    if !args.listen.ip().is_loopback() {
        tracing::warn!(listen = %args.listen, "API is unauthenticated and exposed beyond loopback");
    }
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(listen = %args.listen, "orchestra-server ready");
    axum::serve(listener, orchestra_api::router(store))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;
    controllers.abort();
    Ok(())
}

//! ORCHESTRA control plane: state engine, controllers and API in one process.

mod tls;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use clap::Parser;
use orchestra_api::{AppState, AuditLog};
use orchestra_controller::Config;
use orchestra_pki::{CertificateAuthority, Identity, KeyMaterial, write_private_file};
use orchestra_state::Store;

const SERVER_CERT_TTL: Duration = Duration::from_secs(365 * 24 * 3600);
const ADMIN_CERT_TTL: Duration = Duration::from_secs(365 * 24 * 3600);

#[derive(Parser)]
#[command(version, about = "ORCHESTRA control plane")]
struct Args {
    /// Address of the API (HTTPS, mutual TLS).
    #[arg(long, env = "ORCHESTRA_LISTEN", default_value = "127.0.0.1:7600")]
    listen: SocketAddr,

    /// Directory holding the state log, the cluster PKI and the audit log.
    #[arg(long, env = "ORCHESTRA_DATA_DIR", default_value = "orchestra-data")]
    data_dir: PathBuf,

    /// Keep resource state in memory (lost on exit). The PKI stays on disk.
    #[arg(long)]
    ephemeral: bool,

    /// Seconds without heartbeat before a node's instances are rescheduled.
    #[arg(long, default_value_t = 20)]
    node_grace_secs: u64,

    /// Lifetime of node certificates; agents renew them automatically.
    #[arg(long, default_value_t = 24 * 3600, value_parser = clap::value_parser!(u64).range(60..))]
    node_cert_ttl_secs: u64,

    /// Extra DNS name or IP address clients use to reach this server;
    /// repeatable. Loopback names and the listen address are always included.
    #[arg(long = "tls-san")]
    tls_sans: Vec<String>,
}

fn read(path: &Path) -> anyhow::Result<String> {
    std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))
}

/// Loads the cluster CA, creating it on first start.
fn load_or_create_ca(pki_dir: &Path) -> anyhow::Result<CertificateAuthority> {
    let (cert_path, key_path) = (pki_dir.join("ca.crt"), pki_dir.join("ca.key"));
    if cert_path.exists() || key_path.exists() {
        return CertificateAuthority::from_pem(&read(&cert_path)?, &read(&key_path)?)
            .context("loading the cluster CA");
    }
    let ca = CertificateAuthority::generate("orchestra")?;
    write_private_file(&key_path, ca.key_pem())?;
    std::fs::write(&cert_path, ca.cert_pem())?;
    tracing::info!(path = %cert_path.display(), "created cluster CA");
    Ok(ca)
}

/// Makes sure a usable administrator credential exists next to the CA.
fn ensure_admin_credentials(pki_dir: &Path, ca: &CertificateAuthority) -> anyhow::Result<()> {
    let (cert_path, key_path) = (pki_dir.join("admin.crt"), pki_dir.join("admin.key"));
    let now = (orchestra_domain::now_ms() / 1000) as i64;
    let usable = key_path.exists()
        && std::fs::read_to_string(&cert_path)
            .ok()
            .and_then(|pem| orchestra_pki::validity(&pem).ok())
            .is_some_and(|validity| !validity.should_renew_at(now));
    if usable {
        return Ok(());
    }
    let admin = ca.issue_client(&Identity::Admin("admin".to_string()), ADMIN_CERT_TTL)?;
    write_private_file(&key_path, &admin.key_pem)?;
    std::fs::write(&cert_path, &admin.cert_pem)?;
    tracing::info!(cert = %cert_path.display(), key = %key_path.display(), "issued administrator certificate");
    Ok(())
}

fn issue_server_certificate(args: &Args, ca: &CertificateAuthority) -> anyhow::Result<KeyMaterial> {
    let mut names = vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
        "::1".to_string(),
    ];
    if !args.listen.ip().is_unspecified() {
        names.push(args.listen.ip().to_string());
    }
    names.extend(args.tls_sans.iter().cloned());
    names.sort();
    names.dedup();
    tracing::info!(?names, "issuing server certificate");
    ca.issue_server(&names, SERVER_CERT_TTL)
        .context("issuing the server certificate (check --tls-san values)")
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    tracing_subscriber::fmt().init();
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run() -> anyhow::Result<()> {
    orchestra_pki::install_crypto_provider();
    let args = Args::parse();

    let pki_dir = args.data_dir.join("pki");
    std::fs::create_dir_all(&pki_dir).with_context(|| format!("creating {}", pki_dir.display()))?;
    let ca = load_or_create_ca(&pki_dir)?;
    ensure_admin_credentials(&pki_dir, &ca)?;
    let tls_config = tls::server_config(&ca, &issue_server_certificate(&args, &ca)?)?;

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

    let audit_path = args.data_dir.join("audit.log");
    let audit = AuditLog::open(&audit_path).with_context(|| format!("opening {}", audit_path.display()))?;
    let app = orchestra_api::router(AppState {
        store,
        ca: Arc::new(ca),
        node_cert_ttl: Duration::from_secs(args.node_cert_ttl_secs),
        audit: Arc::new(audit),
    });

    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .with_context(|| format!("binding {}", args.listen))?;
    tracing::info!(listen = %args.listen, "orchestra-server ready (mutual TLS)");
    tls::serve(listener, tls_config, app, async {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutting down");
    })
    .await;
    controllers.abort();
    Ok(())
}

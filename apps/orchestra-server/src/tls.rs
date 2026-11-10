//! Mutual-TLS transport for the API.
//!
//! Every connection is TLS. Client certificates are optional at the
//! handshake (enrollment has to work before a node owns one) but, when
//! presented, must chain to the cluster CA. The verified certificate becomes
//! the [`Caller`] every request on that connection is attributed to.

use std::{future::Future, sync::Arc, time::Duration};

use anyhow::Context;
use axum::{Extension, Router};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto::Builder,
    service::TowerToHyperService,
};
use orchestra_api::Caller;
use orchestra_pki::{CertificateAuthority, KeyMaterial};
use rustls::{
    RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
    server::WebPkiClientVerifier,
};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

pub fn server_config(ca: &CertificateAuthority, server: &KeyMaterial) -> anyhow::Result<ServerConfig> {
    let mut roots = RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(ca.cert_pem().as_bytes()).context("parsing CA certificate")?)?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
        .allow_unauthenticated()
        .build()?;
    let chain = vec![
        CertificateDer::from_pem_slice(server.cert_pem.as_bytes()).context("parsing server certificate")?,
    ];
    let key = PrivateKeyDer::from_pem_slice(server.key_pem.as_bytes()).context("parsing server key")?;
    let mut config = ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(chain, key)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

/// Accepts connections until `shutdown` resolves.
pub async fn serve(
    listener: TcpListener,
    config: ServerConfig,
    app: Router,
    shutdown: impl Future<Output = ()>,
) {
    let acceptor = TlsAcceptor::from(Arc::new(config));
    tokio::pin!(shutdown);
    loop {
        let (tcp, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(connection) => connection,
                Err(error) => {
                    tracing::warn!(%error, "failed to accept connection");
                    // Typically file descriptor exhaustion: do not spin.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            _ = &mut shutdown => return,
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(error)) => return tracing::debug!(%peer, %error, "TLS handshake failed"),
                Err(_) => return tracing::debug!(%peer, "TLS handshake timed out"),
            };
            // rustls has already verified the chain; only the subject is read here.
            let caller = match stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|chain| chain.first())
            {
                None => Caller::Anonymous,
                Some(certificate) => match orchestra_pki::identity_from_der(certificate) {
                    Ok(identity) => identity.into(),
                    Err(error) => {
                        tracing::warn!(%peer, %error, "client certificate carries no usable identity");
                        Caller::Anonymous
                    }
                },
            };
            let service = TowerToHyperService::new(app.layer(Extension(caller)));
            let connection = Builder::new(TokioExecutor::new());
            if let Err(error) = connection.serve_connection(TokioIo::new(stream), service).await {
                tracing::debug!(%peer, %error, "connection closed with error");
            }
        });
    }
}

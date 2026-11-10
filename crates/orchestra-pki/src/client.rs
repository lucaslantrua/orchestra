//! HTTPS client construction shared by the agent and the CLI.

use std::time::Duration;

use crate::KeyMaterial;

/// Builds a client that trusts **only** the cluster CA (never the system
/// trust store) and, when `identity` is given, authenticates with it.
pub fn https_client(ca_pem: &str, identity: Option<&KeyMaterial>) -> reqwest::Result<reqwest::Client> {
    crate::install_crypto_provider();
    let roots = reqwest::Certificate::from_pem_bundle(ca_pem.as_bytes())?;
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .https_only(true)
        .tls_certs_only(roots);
    if let Some(material) = identity {
        let pem = format!("{}\n{}", material.cert_pem, material.key_pem);
        builder = builder.identity(reqwest::Identity::from_pem(pem.as_bytes())?);
    }
    builder.build()
}

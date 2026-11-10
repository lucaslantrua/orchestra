//! Node credentials: enrollment with a join token, storage and renewal.

use std::path::Path;

use anyhow::{Context, bail};
use orchestra_domain::{CertificateRequest, CertificateResponse, JoinRequest, now_ms};
use orchestra_pki::{KeyMaterial, client::https_client, generate_csr, validity, write_private_file};

fn unix_now() -> i64 {
    (now_ms() / 1000) as i64
}

/// Loads the stored credentials if they are still valid.
pub fn load(path: &Path) -> anyhow::Result<Option<KeyMaterial>> {
    if !path.exists() {
        return Ok(None);
    }
    let bundle = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let material =
        KeyMaterial::from_bundle(&bundle).with_context(|| format!("parsing {}", path.display()))?;
    if !validity(&material.cert_pem)?.is_valid_at(unix_now()) {
        tracing::warn!(path = %path.display(), "stored node certificate has expired");
        return Ok(None);
    }
    Ok(Some(material))
}

pub fn store(path: &Path, material: &KeyMaterial) -> anyhow::Result<()> {
    write_private_file(path, &material.to_bundle()).with_context(|| format!("writing {}", path.display()))
}

pub fn renewal_due(material: &KeyMaterial) -> bool {
    // An unreadable certificate cannot be trusted to still work: replace it.
    validity(&material.cert_pem).map_or(true, |validity| validity.should_renew_at(unix_now()))
}

async fn error_message(response: reqwest::Response) -> String {
    let status = response.status();
    let body: serde_json::Value = response.json().await.unwrap_or_default();
    match body.get("error").and_then(|e| e.as_str()) {
        Some(message) => format!("{message} ({status})"),
        None => status.to_string(),
    }
}

/// Redeems a join token for a node certificate. The private key is generated
/// here and never leaves the machine.
pub async fn join(server: &str, ca_pem: &str, node: &str, token: &str) -> anyhow::Result<KeyMaterial> {
    let (csr, key_pem) = generate_csr()?;
    let request = JoinRequest {
        token: token.to_string(),
        node_name: node.to_string(),
        csr,
    };
    let response = https_client(ca_pem, None)?
        .post(format!("{server}/api/v1/join"))
        .json(&request)
        .send()
        .await
        .with_context(|| format!("cannot reach the control plane at {server}"))?;
    if !response.status().is_success() {
        bail!("enrollment refused: {}", error_message(response).await);
    }
    let issued: CertificateResponse = response.json().await?;
    Ok(KeyMaterial {
        cert_pem: issued.certificate,
        key_pem,
    })
}

/// Obtains a fresh certificate, with a new key, using the current one.
pub async fn renew(http: &reqwest::Client, server: &str, node: &str) -> anyhow::Result<KeyMaterial> {
    let (csr, key_pem) = generate_csr()?;
    let response = http
        .post(format!("{server}/api/v1/nodes/{node}/certificate"))
        .json(&CertificateRequest { csr })
        .send()
        .await?;
    if !response.status().is_success() {
        bail!("{}", error_message(response).await);
    }
    let issued: CertificateResponse = response.json().await?;
    Ok(KeyMaterial {
        cert_pem: issued.certificate,
        key_pem,
    })
}

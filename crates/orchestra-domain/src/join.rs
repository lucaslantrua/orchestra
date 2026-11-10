//! Node enrollment: join tokens and the certificate exchange.

use serde::{Deserialize, Serialize};

use crate::meta::{ValidationError, resource};

resource! {
    /// A bootstrap credential letting new agents obtain a node certificate.
    /// Only a hash of the secret is stored; the object name is the token id.
    JoinToken, spec = JoinTokenSpec, status = JoinTokenStatus,
    kind = "JoinToken", plural = "jointokens", namespaced = false
}

impl JoinToken {
    fn validate_spec(&self) -> Result<(), ValidationError> {
        if self.spec.secret_hash.is_empty() {
            return Err(ValidationError::new("secretHash must not be empty"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinTokenSpec {
    /// Hex-encoded SHA-256 of the token secret.
    pub secret_hash: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct JoinTokenStatus {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateJoinTokenRequest {
    pub ttl_seconds: u64,
}

/// The only time the full token is ever visible.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateJoinTokenResponse {
    pub token: String,
    pub expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinRequest {
    pub token: String,
    pub node_name: String,
    /// PEM certificate signing request for the node's freshly generated key.
    pub csr: String,
}

/// Renewal of a node certificate, authenticated by the current one.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateRequest {
    pub csr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CertificateResponse {
    /// PEM client certificate identifying the node.
    pub certificate: String,
}

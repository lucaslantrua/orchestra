//! Caller identity, authorization rules and join tokens.

use std::{convert::Infallible, fmt};

use axum::{extract::FromRequestParts, http::StatusCode, http::request::Parts};
use orchestra_domain::{JoinToken, JoinTokenSpec};
use orchestra_pki::Identity;
use orchestra_state::Store;
use sha2::{Digest, Sha256};

use crate::ApiError;

/// Who is making a request, as established by the TLS layer from the client
/// certificate. The transport inserts it as a request extension; a request
/// without one is anonymous.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Caller {
    Anonymous,
    Admin(String),
    Node(String),
}

impl From<Identity> for Caller {
    fn from(identity: Identity) -> Self {
        match identity {
            Identity::Admin(name) => Caller::Admin(name),
            Identity::Node(name) => Caller::Node(name),
        }
    }
}

impl fmt::Display for Caller {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Caller::Anonymous => write!(f, "anonymous"),
            Caller::Admin(name) => write!(f, "admin:{name}"),
            Caller::Node(name) => write!(f, "node:{name}"),
        }
    }
}

impl Caller {
    fn denied(&self, action: &str) -> ApiError {
        match self {
            Caller::Anonymous => ApiError::new(StatusCode::UNAUTHORIZED, "a client certificate is required"),
            _ => ApiError::new(
                StatusCode::FORBIDDEN,
                format!("{self} is not allowed to {action}"),
            ),
        }
    }

    /// Administrators may do everything.
    pub fn require_admin(&self) -> Result<(), ApiError> {
        match self {
            Caller::Admin(_) => Ok(()),
            _ => Err(self.denied("perform this operation")),
        }
    }

    /// Node-scoped operations are reserved to that node's own agent.
    pub fn require_node(&self, node: &str) -> Result<(), ApiError> {
        match self {
            Caller::Node(name) if name == node => Ok(()),
            _ => Err(self.denied(&format!("act as node {node:?}"))),
        }
    }
}

impl<S: Send + Sync> FromRequestParts<S> for Caller {
    type Rejection = Infallible;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(parts
            .extensions
            .get::<Caller>()
            .cloned()
            .unwrap_or(Caller::Anonymous))
    }
}

fn hash_secret(secret: &str) -> String {
    Sha256::digest(secret.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Creates a join token and returns it as `<id>.<secret>`.
pub(crate) fn create_join_token(store: &Store, expires_at_ms: u64) -> Result<String, ApiError> {
    let random = || uuid::Uuid::new_v4().simple().to_string();
    let id = format!("jt{}", &random()[..10]);
    let secret = format!("{}{}", random(), random());
    store.create(JoinToken::new(
        "",
        &id,
        JoinTokenSpec {
            secret_hash: hash_secret(&secret),
            expires_at_ms,
        },
    ))?;
    Ok(format!("{id}.{secret}"))
}

/// Checks a presented token. Every failure looks the same to the caller.
pub(crate) fn verify_join_token(store: &Store, token: &str, now_ms: u64) -> Result<(), ApiError> {
    let invalid = || ApiError::new(StatusCode::UNAUTHORIZED, "invalid or expired join token");
    let (id, secret) = token.split_once('.').ok_or_else(invalid)?;
    // Hash before the lookup so that unknown ids cost the same as known ones.
    let presented = hash_secret(secret);
    let stored = store.get::<JoinToken>("", id)?.ok_or_else(invalid)?;
    if !constant_time_eq(&stored.spec.secret_hash, &presented) || now_ms >= stored.spec.expires_at_ms {
        return Err(invalid());
    }
    Ok(())
}

//! Metadata and behaviour shared by every ORCHESTRA resource.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub const API_VERSION: &str = "orchestra.io/v1";
pub const DEFAULT_NAMESPACE: &str = "default";

pub(crate) fn api_version() -> String {
    API_VERSION.to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ValidationError(pub String);

impl ValidationError {
    pub fn new(message: impl Into<String>) -> Self {
        ValidationError(message.into())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OwnerRef {
    pub kind: String,
    pub name: String,
    pub uid: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    /// Empty for cluster-scoped resources.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub namespace: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub uid: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
    /// Store revision of the last write; used for optimistic concurrency.
    #[serde(default)]
    pub resource_version: u64,
    /// Incremented by the store every time `spec` changes.
    #[serde(default)]
    pub generation: u64,
    #[serde(default)]
    pub created_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<OwnerRef>,
}

/// A persisted, versioned API resource.
pub trait Object: Serialize + DeserializeOwned + Clone + Send + Sync + 'static {
    const KIND: &'static str;
    /// Lower-case plural used in REST paths.
    const PLURAL: &'static str;
    const NAMESPACED: bool;

    fn meta(&self) -> &ObjectMeta;
    fn meta_mut(&mut self) -> &mut ObjectMeta;
    /// Fills in type information and scoping defaults.
    fn normalize(&mut self);
    fn validate(&self) -> Result<(), ValidationError>;

    fn owner_ref(&self) -> OwnerRef {
        OwnerRef {
            kind: Self::KIND.to_string(),
            name: self.meta().name.clone(),
            uid: self.meta().uid.clone(),
        }
    }
}

/// Names are DNS-subdomain-like: lower-case alphanumerics, `-` and `.`.
pub fn validate_name(what: &str, name: &str) -> Result<(), ValidationError> {
    let edge_ok = |c: Option<char>| c.is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit());
    let chars_ok = name
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-' || c == '.');
    if name.len() > 253 || !chars_ok || !edge_ok(name.chars().next()) || !edge_ok(name.chars().last()) {
        return Err(ValidationError(format!(
            "{what} {name:?} must be at most 253 lower-case alphanumeric, '-' or '.' characters \
             and start and end with an alphanumeric"
        )));
    }
    Ok(())
}

/// Declares a resource envelope (`apiVersion`/`kind`/`metadata`/`spec`/`status`).
///
/// Must be invoked at most once per module, and the type must provide an
/// inherent `validate_spec(&self) -> Result<(), ValidationError>`.
macro_rules! resource {
    (
        $(#[$doc:meta])*
        $name:ident, spec = $spec:ty, status = $status:ty,
        kind = $kind:literal, plural = $plural:literal, namespaced = $namespaced:literal
    ) => {
        fn default_kind() -> String {
            $kind.to_string()
        }

        $(#[$doc])*
        #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
        #[serde(rename_all = "camelCase")]
        pub struct $name {
            #[serde(default = "crate::meta::api_version")]
            pub api_version: String,
            #[serde(default = "default_kind")]
            pub kind: String,
            pub metadata: crate::meta::ObjectMeta,
            pub spec: $spec,
            #[serde(default)]
            pub status: $status,
        }

        impl $name {
            pub fn new(namespace: &str, name: &str, spec: $spec) -> Self {
                let mut object = $name {
                    api_version: crate::meta::api_version(),
                    kind: default_kind(),
                    metadata: crate::meta::ObjectMeta {
                        name: name.to_string(),
                        namespace: namespace.to_string(),
                        ..Default::default()
                    },
                    spec,
                    status: Default::default(),
                };
                crate::meta::Object::normalize(&mut object);
                object
            }
        }

        impl crate::meta::Object for $name {
            const KIND: &'static str = $kind;
            const PLURAL: &'static str = $plural;
            const NAMESPACED: bool = $namespaced;

            fn meta(&self) -> &crate::meta::ObjectMeta {
                &self.metadata
            }

            fn meta_mut(&mut self) -> &mut crate::meta::ObjectMeta {
                &mut self.metadata
            }

            fn normalize(&mut self) {
                self.api_version = crate::meta::api_version();
                self.kind = default_kind();
                if !$namespaced {
                    self.metadata.namespace.clear();
                } else if self.metadata.namespace.is_empty() {
                    self.metadata.namespace = crate::meta::DEFAULT_NAMESPACE.to_string();
                }
            }

            fn validate(&self) -> Result<(), crate::meta::ValidationError> {
                crate::meta::validate_name("name", &self.metadata.name)?;
                if $namespaced {
                    crate::meta::validate_name("namespace", &self.metadata.namespace)?;
                }
                self.validate_spec()
            }
        }
    };
}
pub(crate) use resource;

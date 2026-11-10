//! A single scheduled unit of execution (one container or one VM).

use serde::{Deserialize, Serialize};

use crate::{
    meta::{ValidationError, resource},
    workload::InstanceTemplate,
};

resource! {
    /// One replica of a [`crate::Workload`], bound to at most one node.
    Instance, spec = InstanceSpec, status = InstanceStatus,
    kind = "Instance", plural = "instances", namespaced = true
}

impl Instance {
    fn validate_spec(&self) -> Result<(), ValidationError> {
        self.spec.template.validate()
    }

    pub fn is_bound_to(&self, node: &str) -> bool {
        self.spec.node_name.as_deref() == Some(node)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceSpec {
    pub template: InstanceTemplate,
    /// Set by the scheduler.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    /// Device ids on `node_name` reserved for this instance.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpu_devices: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum InstancePhase {
    /// Waiting for a node.
    #[default]
    Pending,
    /// Bound to a node, not yet confirmed by its agent.
    Scheduled,
    Running,
    Failed,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceStatus {
    #[serde(default)]
    pub phase: InstancePhase,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub message: String,
}

/// Status update sent by the agent hosting an instance.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceStatusReport {
    /// Reporting node; must match the instance's binding.
    pub node: String,
    pub phase: InstancePhase,
    #[serde(default)]
    pub message: String,
}

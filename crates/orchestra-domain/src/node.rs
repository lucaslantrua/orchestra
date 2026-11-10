//! Infrastructure nodes and the accelerators they expose.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    meta::{ValidationError, resource},
    quantity::{Cpu, Memory},
};

resource! {
    /// A Linux machine running an ORCHESTRA agent. Nodes are created by agent
    /// heartbeats, never by manifests.
    Node, spec = NodeSpec, status = NodeStatus,
    kind = "Node", plural = "nodes", namespaced = false
}

impl Node {
    fn validate_spec(&self) -> Result<(), ValidationError> {
        Ok(())
    }

    pub fn is_schedulable(&self) -> bool {
        self.status.phase == NodePhase::Ready && !self.spec.unschedulable
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeSpec {
    /// Set by `cordon`: existing instances keep running, no new ones land here.
    #[serde(default)]
    pub unschedulable: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePhase {
    #[default]
    Unknown,
    Ready,
    NotReady,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Capacity {
    #[serde(default)]
    pub cpu: Cpu,
    #[serde(default)]
    pub memory: Memory,
}

/// One physical accelerator. Devices are allocated whole; MIG and other
/// partitioning schemes are not modelled yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GpuDevice {
    /// Stable identifier, unique within the node.
    pub id: String,
    pub vendor: String,
    #[serde(default)]
    pub model: String,
    pub memory: Memory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub numa_node: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    #[serde(default)]
    pub phase: NodePhase,
    #[serde(default)]
    pub capacity: Capacity,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpus: Vec<GpuDevice>,
    #[serde(default)]
    pub last_heartbeat_ms: u64,
    #[serde(default)]
    pub agent_version: String,
}

/// Periodic report sent by an agent; the first one registers the node.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeHeartbeat {
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    pub capacity: Capacity,
    #[serde(default)]
    pub gpus: Vec<GpuDevice>,
    #[serde(default)]
    pub agent_version: String,
}

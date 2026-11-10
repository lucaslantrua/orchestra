//! Replicated container and virtual machine workloads.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    meta::{ValidationError, resource},
    quantity::{Cpu, Memory},
};

resource! {
    /// A set of identical instances (containers or VMs) kept at a replica count.
    Workload, spec = WorkloadSpec, status = WorkloadStatus,
    kind = "Workload", plural = "workloads", namespaced = true
}

impl Workload {
    fn validate_spec(&self) -> Result<(), ValidationError> {
        self.spec.template.validate()
    }
}

fn one() -> u32 {
    1
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadSpec {
    #[serde(default = "one")]
    pub replicas: u32,
    pub template: InstanceTemplate,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstanceTemplate {
    pub runtime: RuntimeSpec,
    #[serde(default)]
    pub resources: ResourceRequirements,
    /// Labels a node must carry to host an instance.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub node_selector: BTreeMap<String, String>,
}

impl InstanceTemplate {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.runtime.image().trim().is_empty() {
            return Err(ValidationError::new("template.runtime.image must not be empty"));
        }
        if let Some(gpu) = &self.resources.gpu {
            gpu.validate()?;
        }
        Ok(())
    }
}

/// Containers and VMs are both first-class: they share scheduling, lifecycle
/// and status, and differ only in the runtime the agent hands them to.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub enum RuntimeSpec {
    Container {
        image: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        command: Vec<String>,
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        env: BTreeMap<String, String>,
    },
    VirtualMachine {
        image: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        root_disk: Option<Memory>,
    },
}

impl RuntimeSpec {
    pub fn image(&self) -> &str {
        match self {
            RuntimeSpec::Container { image, .. } | RuntimeSpec::VirtualMachine { image, .. } => image,
        }
    }

    pub fn type_name(&self) -> &'static str {
        match self {
            RuntimeSpec::Container { .. } => "container",
            RuntimeSpec::VirtualMachine { .. } => "vm",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRequirements {
    #[serde(default)]
    pub cpu: Cpu,
    #[serde(default)]
    pub memory: Memory,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu: Option<GpuRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GpuRequest {
    /// Matched case-insensitively against the device vendor; any if absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor: Option<String>,
    /// Number of whole devices, all on the same node.
    pub count: u32,
    /// Minimum memory of each device.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
}

impl GpuRequest {
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.count == 0 {
            return Err(ValidationError::new("gpu.count must be at least 1"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadStatus {
    #[serde(default)]
    pub replicas: u32,
    #[serde(default)]
    pub ready_replicas: u32,
    #[serde(default)]
    pub observed_generation: u64,
}

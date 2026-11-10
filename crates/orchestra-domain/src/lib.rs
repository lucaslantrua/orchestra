//! ORCHESTRA resource model: the versioned contracts shared by the control
//! plane, the node agents and the CLI.

pub mod ai;
pub mod instance;
pub mod meta;
pub mod node;
pub mod quantity;
pub mod workload;

pub use ai::{AIWorkload, AIWorkloadSpec, Engine, MODEL_LABEL};
pub use instance::{Instance, InstancePhase, InstanceSpec, InstanceStatus, InstanceStatusReport};
pub use meta::{API_VERSION, DEFAULT_NAMESPACE, Object, ObjectMeta, OwnerRef, ValidationError};
pub use node::{Capacity, GpuDevice, Node, NodeHeartbeat, NodePhase, NodeSpec, NodeStatus};
pub use quantity::{Cpu, Memory, QuantityError};
pub use workload::{
    GpuRequest, InstanceTemplate, ResourceRequirements, RuntimeSpec, Workload, WorkloadSpec, WorkloadStatus,
};

/// Wall-clock time in milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

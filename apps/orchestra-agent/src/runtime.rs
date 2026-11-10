//! The boundary between the agent and whatever actually runs workloads.
//!
//! ORCHESTRA never implements a container runtime or a hypervisor itself:
//! real implementations of [`Runtime`] will delegate to containerd (CRI) and
//! to KVM via Cloud Hypervisor. Until those land (phases 2 and 4),
//! [`SimulatedRuntime`] stands in so the control loops can be exercised
//! end to end.

use std::collections::BTreeMap;

use orchestra_domain::Instance;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RuntimeError(pub String);

#[allow(async_fn_in_trait)]
pub trait Runtime {
    /// Starts the instance; must be idempotent for a given instance uid.
    async fn start(&mut self, instance: &Instance) -> Result<(), RuntimeError>;
    /// Stops and removes the instance with this uid, if present.
    async fn stop(&mut self, uid: &str) -> Result<(), RuntimeError>;
    /// Uids of the instances currently running.
    fn running(&self) -> Vec<String>;
}

/// Pretends to run instances: it only remembers which ones were started.
/// Images whose name contains `simulate-failure` refuse to start.
#[derive(Default)]
pub struct SimulatedRuntime {
    instances: BTreeMap<String, String>,
}

impl Runtime for SimulatedRuntime {
    async fn start(&mut self, instance: &Instance) -> Result<(), RuntimeError> {
        let runtime = &instance.spec.template.runtime;
        if runtime.image().contains("simulate-failure") {
            return Err(RuntimeError(format!(
                "image {:?} failed to start (simulated)",
                runtime.image()
            )));
        }
        let name = format!("{}/{}", instance.metadata.namespace, instance.metadata.name);
        tracing::info!(
            instance = %name,
            kind = runtime.type_name(),
            image = runtime.image(),
            gpus = ?instance.spec.gpu_devices,
            "simulated start"
        );
        self.instances.insert(instance.metadata.uid.clone(), name);
        Ok(())
    }

    async fn stop(&mut self, uid: &str) -> Result<(), RuntimeError> {
        if let Some(name) = self.instances.remove(uid) {
            tracing::info!(instance = %name, "simulated stop");
        }
        Ok(())
    }

    fn running(&self) -> Vec<String> {
        self.instances.keys().cloned().collect()
    }
}

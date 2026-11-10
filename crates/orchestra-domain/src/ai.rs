//! LLM inference workloads (ORCHESTRA AI).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    meta::{ValidationError, resource},
    quantity::{Cpu, Memory},
    workload::{GpuRequest, InstanceTemplate, ResourceRequirements, RuntimeSpec},
};

resource! {
    /// A served model. The control plane lowers it to a GPU-backed
    /// [`crate::Workload`] running the chosen inference engine.
    AIWorkload, spec = AIWorkloadSpec, status = AIWorkloadStatus,
    kind = "AIWorkload", plural = "aiworkloads", namespaced = true
}

/// Label set on everything derived from an `AIWorkload`.
pub const MODEL_LABEL: &str = "orchestra.io/model";

impl AIWorkload {
    fn validate_spec(&self) -> Result<(), ValidationError> {
        let spec = &self.spec;
        if spec.model.name.trim().is_empty() {
            return Err(ValidationError::new("model.name must not be empty"));
        }
        spec.resources.gpu.validate()?;
        if spec.scaling.min_replicas > spec.scaling.max_replicas {
            return Err(ValidationError::new(
                "scaling.minReplicas must not exceed scaling.maxReplicas",
            ));
        }
        Ok(())
    }

    /// Template of the inference server instances backing this workload.
    pub fn instance_template(&self) -> InstanceTemplate {
        let spec = &self.spec;
        let env = BTreeMap::from([
            ("ORCHESTRA_MODEL_NAME".to_string(), spec.model.name.clone()),
            ("ORCHESTRA_MODEL_VERSION".to_string(), spec.model.version.clone()),
            ("ORCHESTRA_INFERENCE_API".to_string(), spec.inference.api.clone()),
            (
                "ORCHESTRA_INFERENCE_STREAMING".to_string(),
                spec.inference.streaming.to_string(),
            ),
        ]);
        InstanceTemplate {
            runtime: RuntimeSpec::Container {
                image: spec
                    .serving
                    .image
                    .clone()
                    .unwrap_or_else(|| spec.serving.engine.default_image().to_string()),
                command: Vec::new(),
                env,
            },
            resources: ResourceRequirements {
                cpu: spec.resources.cpu.unwrap_or(DEFAULT_SERVER_CPU),
                memory: spec.resources.memory.unwrap_or(DEFAULT_SERVER_MEMORY),
                gpu: Some(spec.resources.gpu.clone()),
            },
            node_selector: BTreeMap::new(),
        }
    }
}

/// Host resources reserved per inference server when the manifest is silent
/// (tokenization and request handling are CPU-bound).
const DEFAULT_SERVER_CPU: Cpu = Cpu::from_cores(4);
const DEFAULT_SERVER_MEMORY: Memory = Memory::from_gib(16);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AIWorkloadSpec {
    pub model: ModelRef,
    pub serving: Serving,
    pub resources: AIResources,
    #[serde(default)]
    pub scaling: Scaling,
    #[serde(default)]
    pub inference: Inference,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRef {
    pub name: String,
    #[serde(default)]
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Serving {
    pub engine: Engine,
    /// Overrides the engine's default image.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Engine {
    Vllm,
    Sglang,
    Triton,
    RayServe,
}

impl Engine {
    pub fn name(self) -> &'static str {
        match self {
            Engine::Vllm => "vllm",
            Engine::Sglang => "sglang",
            Engine::Triton => "triton",
            Engine::RayServe => "ray-serve",
        }
    }

    /// Placeholder references into the platform registry: engine images are
    /// expected to be mirrored there (air-gap), not pulled from the Internet.
    pub fn default_image(self) -> &'static str {
        match self {
            Engine::Vllm => "registry.orchestra.internal/inference/vllm:stable",
            Engine::Sglang => "registry.orchestra.internal/inference/sglang:stable",
            Engine::Triton => "registry.orchestra.internal/inference/triton:stable",
            Engine::RayServe => "registry.orchestra.internal/inference/ray-serve:stable",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AIResources {
    /// Per-replica GPU requirement; `memory` is the minimum per device.
    pub gpu: GpuRequest,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Cpu>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<Memory>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Scaling {
    pub min_replicas: u32,
    pub max_replicas: u32,
}

impl Default for Scaling {
    fn default() -> Self {
        Scaling {
            min_replicas: 1,
            max_replicas: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Inference {
    pub api: String,
    #[serde(default)]
    pub streaming: bool,
}

impl Default for Inference {
    fn default() -> Self {
        Inference {
            api: "openai-compatible".to_string(),
            streaming: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AIWorkloadStatus {
    #[serde(default)]
    pub replicas: u32,
    #[serde(default)]
    pub ready_replicas: u32,
    #[serde(default)]
    pub observed_generation: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Object;

    /// The manifest from the reference architecture (§8.4) must parse as-is.
    #[test]
    fn parses_reference_manifest() {
        let manifest = r#"
apiVersion: orchestra.io/v1
kind: AIWorkload
metadata:
  name: llm-production
spec:
  model:
    name: internal-llm
    version: "1.0"
  serving:
    engine: vllm
  resources:
    gpu:
      vendor: nvidia
      count: 4
      memory: 80Gi
  scaling:
    minReplicas: 2
    maxReplicas: 8
  inference:
    api: openai-compatible
    streaming: true
"#;
        let mut ai: AIWorkload = serde_norway::from_str(manifest).unwrap();
        ai.normalize();
        ai.validate().unwrap();
        assert_eq!(ai.metadata.namespace, "default");
        assert_eq!(ai.spec.serving.engine, Engine::Vllm);
        assert_eq!(ai.spec.scaling.max_replicas, 8);

        let template = ai.instance_template();
        let gpu = template.resources.gpu.unwrap();
        assert_eq!(gpu.count, 4);
        assert_eq!(gpu.memory, Some(Memory::from_gib(80)));
    }

    #[test]
    fn rejects_inverted_scaling_bounds() {
        let mut ai = AIWorkload::new(
            "default",
            "m",
            AIWorkloadSpec {
                model: ModelRef {
                    name: "m".into(),
                    version: "1".into(),
                },
                serving: Serving {
                    engine: Engine::Sglang,
                    image: None,
                },
                resources: AIResources {
                    gpu: GpuRequest {
                        vendor: None,
                        count: 1,
                        memory: None,
                    },
                    cpu: None,
                    memory: None,
                },
                scaling: Scaling {
                    min_replicas: 3,
                    max_replicas: 2,
                },
                inference: Inference::default(),
            },
        );
        assert!(ai.validate().is_err());
        ai.spec.scaling.max_replicas = 3;
        assert!(ai.validate().is_ok());
    }
}

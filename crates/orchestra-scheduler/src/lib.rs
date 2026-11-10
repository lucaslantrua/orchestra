//! ORCHESTRA scheduler: a pure placement function.
//!
//! Given one unbound instance and the observed cluster, pick a node and the
//! GPU devices to reserve on it. The scheduler holds no state of its own,
//! which keeps every decision reproducible and testable.
//!
//! Policy:
//! * **Filter** nodes that are not ready, cordoned, do not match the node
//!   selector, or lack CPU, memory or matching free GPUs.
//! * **Score** CPU-only instances by spreading (least allocated), steering
//!   them away from GPU nodes; score GPU instances by packing, so that large
//!   multi-GPU requests keep finding whole nodes.
//! * **Select devices** on a single NUMA node when possible, using the
//!   smallest devices that satisfy the request.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use orchestra_domain::{Cpu, GpuDevice, GpuRequest, Instance, InstancePhase, Memory, Node, NodePhase};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    pub node: String,
    /// Device ids reserved on `node`; empty for CPU-only instances.
    pub gpu_devices: Vec<String>,
    pub score: i64,
}

/// No node can host the instance; `reasons` explains why, per node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unschedulable {
    pub reasons: BTreeMap<String, String>,
}

impl fmt::Display for Unschedulable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.reasons.is_empty() {
            return write!(f, "no nodes registered");
        }
        write!(f, "0/{} nodes available: ", self.reasons.len())?;
        for (index, (node, reason)) in self.reasons.iter().enumerate() {
            if index > 0 {
                write!(f, "; ")?;
            }
            write!(f, "{node}: {reason}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Unschedulable {}

/// Resources already committed on a node.
#[derive(Default)]
struct Usage<'a> {
    cpu: Cpu,
    memory: Memory,
    gpus: BTreeSet<&'a str>,
}

fn usage<'a>(node: &str, instance: &Instance, instances: &'a [Instance]) -> Usage<'a> {
    let mut usage = Usage::default();
    for other in instances {
        let is_self = other.metadata.namespace == instance.metadata.namespace
            && other.metadata.name == instance.metadata.name;
        if is_self || !other.is_bound_to(node) || other.status.phase == InstancePhase::Failed {
            continue;
        }
        usage.cpu += other.spec.template.resources.cpu;
        usage.memory += other.spec.template.resources.memory;
        usage
            .gpus
            .extend(other.spec.gpu_devices.iter().map(String::as_str));
    }
    usage
}

struct GpuSelection {
    devices: Vec<String>,
    single_numa: bool,
}

/// Picks devices for `request`, or reports how many suitable ones are free.
fn select_gpus(node: &Node, used: &BTreeSet<&str>, request: &GpuRequest) -> Result<GpuSelection, usize> {
    let count = request.count as usize;
    let mut free: Vec<&GpuDevice> = node
        .status
        .gpus
        .iter()
        .filter(|gpu| !used.contains(gpu.id.as_str()))
        .filter(|gpu| {
            request
                .vendor
                .as_ref()
                .is_none_or(|v| v.eq_ignore_ascii_case(&gpu.vendor))
        })
        .filter(|gpu| request.memory.is_none_or(|m| gpu.memory >= m))
        .collect();
    if free.len() < count {
        return Err(free.len());
    }
    // Smallest sufficient devices first: keep the big ones for those who need them.
    free.sort_by(|a, b| (a.memory, &a.id).cmp(&(b.memory, &b.id)));

    let mut by_numa: BTreeMap<u32, Vec<&GpuDevice>> = BTreeMap::new();
    for gpu in &free {
        if let Some(numa) = gpu.numa_node {
            by_numa.entry(numa).or_default().push(gpu);
        }
    }
    // Tightest NUMA node that fits, to limit fragmentation.
    let colocated = by_numa
        .values()
        .filter(|group| group.len() >= count)
        .min_by_key(|group| group.len());
    let (chosen, single_numa) = match colocated {
        Some(group) => (&group[..count], true),
        None => (&free[..count], count == 1),
    };
    Ok(GpuSelection {
        devices: chosen.iter().map(|gpu| gpu.id.clone()).collect(),
        single_numa,
    })
}

fn free_percent(capacity: u64, used: u64) -> i64 {
    if capacity == 0 {
        return 0;
    }
    (capacity.saturating_sub(used) as u128 * 100 / capacity as u128) as i64
}

fn evaluate(instance: &Instance, node: &Node, instances: &[Instance]) -> Result<Placement, String> {
    let name = &node.metadata.name;
    if node.status.phase != NodePhase::Ready {
        return Err("node is not ready".into());
    }
    if node.spec.unschedulable {
        return Err("node is cordoned".into());
    }
    let template = &instance.spec.template;
    for (key, value) in &template.node_selector {
        if node.metadata.labels.get(key) != Some(value) {
            return Err(format!("node selector {key}={value} does not match"));
        }
    }

    let used = usage(name, instance, instances);
    let request = &template.resources;
    let capacity = node.status.capacity;
    let cpu_after = used.cpu + request.cpu;
    let memory_after = used.memory + request.memory;
    if cpu_after > capacity.cpu {
        return Err(format!(
            "insufficient cpu (requested {}, free {})",
            request.cpu,
            capacity.cpu.saturating_sub(used.cpu)
        ));
    }
    if memory_after > capacity.memory {
        return Err(format!(
            "insufficient memory (requested {}, free {})",
            request.memory,
            capacity.memory.saturating_sub(used.memory)
        ));
    }

    let Some(gpu_request) = &request.gpu else {
        let spread = (free_percent(capacity.cpu.millis(), cpu_after.millis())
            + free_percent(capacity.memory.bytes(), memory_after.bytes()))
            / 2;
        // Keep accelerator nodes free for workloads that need them.
        let penalty = if node.status.gpus.is_empty() { 0 } else { 100 };
        return Ok(Placement {
            node: name.clone(),
            gpu_devices: Vec::new(),
            score: spread - penalty,
        });
    };

    let selection = select_gpus(node, &used.gpus, gpu_request).map_err(|free| {
        format!(
            "insufficient gpu (requested {}, {free} suitable device(s) free)",
            gpu_request.count
        )
    })?;
    let total = node.status.gpus.len() as i64;
    let allocated = (used.gpus.len() + selection.devices.len()) as i64;
    let packing = allocated * 100 / total.max(1);
    let locality = if selection.single_numa { 10 } else { 0 };
    Ok(Placement {
        node: name.clone(),
        gpu_devices: selection.devices,
        score: packing + locality,
    })
}

/// Chooses a node for `instance`.
///
/// `instances` is every instance in the cluster; those already bound count
/// against their node's capacity. Ties are broken by node name so the result
/// is deterministic.
pub fn schedule(
    instance: &Instance,
    nodes: &[Node],
    instances: &[Instance],
) -> Result<Placement, Unschedulable> {
    let mut best: Option<Placement> = None;
    let mut reasons = BTreeMap::new();
    for node in nodes {
        match evaluate(instance, node, instances) {
            Ok(candidate) => {
                let better = best.as_ref().is_none_or(|current| {
                    (candidate.score, &current.node) > (current.score, &candidate.node)
                });
                if better {
                    best = Some(candidate);
                }
            }
            Err(reason) => {
                reasons.insert(node.metadata.name.clone(), reason);
            }
        }
    }
    best.ok_or(Unschedulable { reasons })
}

#[cfg(test)]
mod tests {
    use orchestra_domain::{
        Capacity, InstanceSpec, InstanceTemplate, NodeSpec, ResourceRequirements, RuntimeSpec,
    };

    use super::*;

    fn node(name: &str, cores: u64, gib: u64, gpus: Vec<GpuDevice>) -> Node {
        let mut node = Node::new("", name, NodeSpec::default());
        node.status.phase = NodePhase::Ready;
        node.status.capacity = Capacity {
            cpu: Cpu::from_cores(cores),
            memory: Memory::from_gib(gib),
        };
        node.status.gpus = gpus;
        node
    }

    fn gpu(id: &str, gib: u64, numa: u32) -> GpuDevice {
        GpuDevice {
            id: id.into(),
            vendor: "nvidia".into(),
            model: "test".into(),
            memory: Memory::from_gib(gib),
            numa_node: Some(numa),
        }
    }

    fn instance(name: &str, cores: u64, gib: u64, gpu: Option<GpuRequest>) -> Instance {
        Instance::new(
            "default",
            name,
            InstanceSpec {
                template: InstanceTemplate {
                    runtime: RuntimeSpec::Container {
                        image: "img".into(),
                        command: vec![],
                        env: Default::default(),
                    },
                    resources: ResourceRequirements {
                        cpu: Cpu::from_cores(cores),
                        memory: Memory::from_gib(gib),
                        gpu,
                    },
                    node_selector: Default::default(),
                },
                node_name: None,
                gpu_devices: vec![],
            },
        )
    }

    fn bound(mut instance: Instance, node: &str, gpus: &[&str]) -> Instance {
        instance.spec.node_name = Some(node.into());
        instance.spec.gpu_devices = gpus.iter().map(|g| g.to_string()).collect();
        instance.status.phase = InstancePhase::Running;
        instance
    }

    fn gpus(count: u32, gib: Option<u64>) -> Option<GpuRequest> {
        Some(GpuRequest {
            vendor: Some("NVIDIA".into()),
            count,
            memory: gib.map(Memory::from_gib),
        })
    }

    #[test]
    fn spreads_cpu_workloads_and_accounts_for_usage() {
        let nodes = [node("a", 8, 32, vec![]), node("b", 8, 32, vec![])];
        let existing = [bound(instance("x", 4, 8, None), "a", &[])];
        let placement = schedule(&instance("new", 2, 4, None), &nodes, &existing).unwrap();
        assert_eq!(placement.node, "b");

        let too_big = schedule(&instance("big", 6, 4, None), &nodes[..1], &existing).unwrap_err();
        assert!(too_big.reasons["a"].contains("insufficient cpu"), "{too_big}");
    }

    #[test]
    fn skips_unready_cordoned_and_mismatched_nodes() {
        let mut down = node("down", 8, 32, vec![]);
        down.status.phase = NodePhase::NotReady;
        let mut cordoned = node("cordoned", 8, 32, vec![]);
        cordoned.spec.unschedulable = true;
        let mut labelled = node("edge", 8, 32, vec![]);
        labelled.metadata.labels.insert("zone".into(), "edge".into());
        let nodes = [down, cordoned, labelled];

        let mut wanted = instance("i", 1, 1, None);
        assert_eq!(schedule(&wanted, &nodes, &[]).unwrap().node, "edge");

        wanted
            .spec
            .template
            .node_selector
            .insert("zone".into(), "core".into());
        let err = schedule(&wanted, &nodes, &[]).unwrap_err();
        assert_eq!(err.reasons.len(), 3);
        assert!(err.to_string().starts_with("0/3 nodes available"));
    }

    #[test]
    fn cpu_workloads_avoid_gpu_nodes_when_possible() {
        let nodes = [
            node("gpu", 64, 512, vec![gpu("g0", 80, 0)]),
            node("cpu", 4, 8, vec![]),
        ];
        assert_eq!(
            schedule(&instance("i", 1, 1, None), &nodes, &[]).unwrap().node,
            "cpu"
        );
        // ...but still use them rather than stay pending.
        assert_eq!(
            schedule(&instance("i", 16, 1, None), &nodes, &[]).unwrap().node,
            "gpu"
        );
    }

    #[test]
    fn gpu_devices_are_exclusive_and_filtered() {
        let nodes = [node("a", 32, 256, vec![gpu("g0", 24, 0), gpu("g1", 80, 0)])];
        // Only g1 has 80Gi.
        let first = schedule(&instance("one", 1, 1, gpus(1, Some(80))), &nodes, &[]).unwrap();
        assert_eq!(first.gpu_devices, ["g1"]);
        // Best fit: a small request takes the small device.
        let small = schedule(&instance("small", 1, 1, gpus(1, None)), &nodes, &[]).unwrap();
        assert_eq!(small.gpu_devices, ["g0"]);

        let existing = [bound(instance("one", 1, 1, gpus(1, Some(80))), "a", &["g1"])];
        let err = schedule(&instance("two", 1, 1, gpus(1, Some(80))), &nodes, &existing).unwrap_err();
        assert!(err.reasons["a"].contains("insufficient gpu"));

        let mut amd = instance("amd", 1, 1, gpus(1, None));
        amd.spec.template.resources.gpu.as_mut().unwrap().vendor = Some("amd".into());
        assert!(schedule(&amd, &nodes, &[]).is_err());
    }

    #[test]
    fn multi_gpu_requests_stay_on_one_numa_node() {
        let devices = vec![
            gpu("g0", 80, 0),
            gpu("g1", 80, 0),
            gpu("g2", 80, 1),
            gpu("g3", 80, 1),
        ];
        let nodes = [node("a", 64, 512, devices)];
        // g0 is taken, so only NUMA node 1 can host a colocated pair.
        let existing = [bound(instance("x", 1, 1, gpus(1, None)), "a", &["g0"])];
        let pair = schedule(&instance("pair", 1, 1, gpus(2, None)), &nodes, &existing).unwrap();
        assert_eq!(pair.gpu_devices, ["g2", "g3"]);
        // Three devices cannot be colocated; fall back to spanning NUMA nodes.
        let triple = schedule(&instance("triple", 1, 1, gpus(3, None)), &nodes, &existing).unwrap();
        assert_eq!(triple.gpu_devices.len(), 3);
        assert!(triple.score < pair.score + 100);
    }

    #[test]
    fn gpu_workloads_pack_to_preserve_whole_nodes() {
        let four = |prefix: &str| {
            (0..4)
                .map(|i| gpu(&format!("{prefix}{i}"), 80, 0))
                .collect::<Vec<_>>()
        };
        let nodes = [node("a", 64, 512, four("a")), node("b", 64, 512, four("b"))];
        let existing = [bound(instance("x", 1, 1, gpus(1, None)), "b", &["b0"])];
        // Lands next to the existing instance, leaving node `a` whole...
        assert_eq!(
            schedule(&instance("s", 1, 1, gpus(1, None)), &nodes, &existing)
                .unwrap()
                .node,
            "b"
        );
        // ...so a 4-GPU request still fits.
        assert_eq!(
            schedule(&instance("l", 1, 1, gpus(4, None)), &nodes, &existing)
                .unwrap()
                .node,
            "a"
        );
    }
}

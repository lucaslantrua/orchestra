//! ORCHESTRA controllers: drive the observed state toward the desired state.
//!
//! [`reconcile_once`] is a synchronous, level-triggered pass over the whole
//! store. It is idempotent: running it on a converged cluster writes nothing,
//! so it can be re-run after any crash or on any notification. [`run`] wraps
//! it in a loop woken by store changes and a periodic tick.

use std::{collections::BTreeSet, sync::Arc, time::Duration};

use orchestra_domain::{
    AIWorkload, Instance, InstancePhase, InstanceSpec, JoinToken, MODEL_LABEL, Node, NodePhase, Object,
    Workload, WorkloadSpec, now_ms,
};
use orchestra_scheduler::schedule;
use orchestra_state::{Result, Store, StoreError};
use tokio::sync::broadcast::error::RecvError;

#[derive(Clone, Debug)]
pub struct Config {
    /// A node whose last heartbeat is older than this is considered lost and
    /// its instances are rescheduled.
    pub node_grace: Duration,
    /// Upper bound between two passes when nothing changes in the store;
    /// time-based conditions (missed heartbeats) are detected on this tick.
    pub tick: Duration,
    /// When this control plane started observing the cluster. Heartbeats
    /// cannot have been received before that, so node silence is counted from
    /// here at the earliest: a control plane outage must not look like a
    /// cluster-wide node failure.
    pub observing_since_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            node_grace: Duration::from_secs(20),
            tick: Duration::from_secs(1),
            observing_since_ms: 0,
        }
    }
}

/// What a reconciliation pass changed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub nodes_lost: usize,
    pub instances_created: usize,
    pub instances_deleted: usize,
    pub instances_scheduled: usize,
    pub instances_unschedulable: usize,
}

/// Runs the control loops until the store is dropped.
pub async fn run(store: Arc<Store>, config: Config) {
    let mut events = store.subscribe();
    loop {
        match reconcile_once(&store, now_ms(), &config) {
            Ok(summary) if summary != Summary::default() => tracing::info!(?summary, "reconciled"),
            Ok(_) => {}
            Err(error) => tracing::error!(%error, "reconciliation failed; will retry"),
        }
        tokio::select! {
            event = events.recv() => {
                if matches!(event, Err(RecvError::Closed)) {
                    return;
                }
            }
            _ = tokio::time::sleep(config.tick) => {}
        }
        // Coalesce bursts: one pass observes every change made so far.
        events = events.resubscribe();
    }
}

pub fn reconcile_once(store: &Store, now_ms: u64, config: &Config) -> Result<Summary> {
    let mut summary = Summary::default();
    expire_join_tokens(store, now_ms)?;
    node_lifecycle(store, now_ms, config, &mut summary)?;
    lower_ai_workloads(store)?;
    garbage_collect(store, &mut summary)?;
    reconcile_workloads(store, &mut summary)?;
    schedule_pending(store, &mut summary)?;
    update_status(store)?;
    Ok(summary)
}

/// Deleting something that is already gone is success for a controller.
fn ignore_missing<T>(result: Result<T>) -> Result<()> {
    match result {
        Ok(_) | Err(StoreError::NotFound { .. }) => Ok(()),
        Err(error) => Err(error),
    }
}

fn expire_join_tokens(store: &Store, now_ms: u64) -> Result<()> {
    for token in store.list::<JoinToken>(None)? {
        if token.spec.expires_at_ms <= now_ms {
            store.delete::<JoinToken>("", &token.metadata.name)?;
        }
    }
    Ok(())
}

fn node_lifecycle(store: &Store, now_ms: u64, config: &Config, summary: &mut Summary) -> Result<()> {
    let grace_ms = config.node_grace.as_millis() as u64;
    for node in store.list::<Node>(None)? {
        let last_seen = node.status.last_heartbeat_ms.max(config.observing_since_ms);
        let silent_for = now_ms.saturating_sub(last_seen);
        if node.status.phase == NodePhase::Ready && silent_for > grace_ms {
            tracing::warn!(node = %node.metadata.name, silent_for_ms = silent_for, "node lost");
            ignore_missing(store.modify::<Node>("", &node.metadata.name, |n| {
                n.status.phase = NodePhase::NotReady;
            }))?;
            summary.nodes_lost += 1;
        }
    }
    Ok(())
}

/// Each `AIWorkload` owns a `Workload` of the same name running its
/// inference servers.
fn lower_ai_workloads(store: &Store) -> Result<()> {
    for ai in store.list::<AIWorkload>(None)? {
        let (namespace, name) = (&ai.metadata.namespace, &ai.metadata.name);
        let scaling = ai.spec.scaling;
        let template = ai.instance_template();
        let mut labels = ai.metadata.labels.clone();
        labels.insert(MODEL_LABEL.to_string(), ai.spec.model.name.clone());

        match store.get::<Workload>(namespace, name)? {
            None => {
                let mut workload = Workload::new(
                    namespace,
                    name,
                    WorkloadSpec {
                        replicas: scaling.min_replicas,
                        template,
                    },
                );
                workload.metadata.labels = labels;
                workload.metadata.owner = Some(ai.owner_ref());
                match store.create(workload) {
                    Ok(_) | Err(StoreError::AlreadyExists { .. }) => {}
                    Err(error) => return Err(error),
                }
            }
            Some(existing) if existing.metadata.owner.as_ref() == Some(&ai.owner_ref()) => {
                ignore_missing(store.modify::<Workload>(namespace, name, |w| {
                    // The replica count belongs to the autoscaler; only keep
                    // it within the declared bounds.
                    w.spec.replicas = w.spec.replicas.clamp(scaling.min_replicas, scaling.max_replicas);
                    w.spec.template = template;
                    w.metadata.labels = labels;
                }))?;
            }
            Some(_) => {
                tracing::warn!(%namespace, %name, "AIWorkload shadows a Workload it does not own; skipping");
            }
        }
    }
    Ok(())
}

/// Removes objects whose owner no longer exists.
fn garbage_collect(store: &Store, summary: &mut Summary) -> Result<()> {
    let ai_uids: BTreeSet<String> = store
        .list::<AIWorkload>(None)?
        .into_iter()
        .map(|ai| ai.metadata.uid)
        .collect();
    for workload in store.list::<Workload>(None)? {
        let orphaned = workload
            .metadata
            .owner
            .as_ref()
            .is_some_and(|owner| owner.kind == AIWorkload::KIND && !ai_uids.contains(&owner.uid));
        if orphaned {
            store.delete::<Workload>(&workload.metadata.namespace, &workload.metadata.name)?;
        }
    }

    let workload_uids: BTreeSet<String> = store
        .list::<Workload>(None)?
        .into_iter()
        .map(|w| w.metadata.uid)
        .collect();
    for instance in store.list::<Instance>(None)? {
        let orphaned = instance
            .metadata
            .owner
            .as_ref()
            .is_some_and(|owner| owner.kind == Workload::KIND && !workload_uids.contains(&owner.uid));
        if orphaned && store.delete::<Instance>(&instance.metadata.namespace, &instance.metadata.name)? {
            summary.instances_deleted += 1;
        }
    }
    Ok(())
}

fn owned_instances(store: &Store, workload: &Workload) -> Result<Vec<Instance>> {
    let owner = workload.owner_ref();
    let mut instances = store.list::<Instance>(Some(&workload.metadata.namespace))?;
    instances.retain(|instance| instance.metadata.owner.as_ref() == Some(&owner));
    Ok(instances)
}

/// Keeps each workload at its replica count with healthy, up-to-date instances.
fn reconcile_workloads(store: &Store, summary: &mut Summary) -> Result<()> {
    let healthy_nodes: BTreeSet<String> = store
        .list::<Node>(None)?
        .into_iter()
        .filter(|node| node.status.phase == NodePhase::Ready)
        .map(|node| node.metadata.name)
        .collect();

    for workload in store.list::<Workload>(None)? {
        let namespace = &workload.metadata.namespace;
        let (mut active, obsolete): (Vec<Instance>, Vec<Instance>) = owned_instances(store, &workload)?
            .into_iter()
            .partition(|instance| {
                let node_ok = instance
                    .spec
                    .node_name
                    .as_ref()
                    .is_none_or(|node| healthy_nodes.contains(node));
                instance.status.phase != InstancePhase::Failed
                    && instance.spec.template == workload.spec.template
                    && node_ok
            });

        // Surplus goes first where it hurts least: unbound, then not yet
        // running, then the youngest.
        active.sort_by_key(|instance| {
            let rank = match instance.status.phase {
                InstancePhase::Running => 0,
                InstancePhase::Scheduled => 1,
                _ => 2,
            };
            (
                rank,
                std::cmp::Reverse(instance.metadata.created_at_ms),
                instance.metadata.name.clone(),
            )
        });
        let desired = workload.spec.replicas as usize;
        let surplus = active.split_off(desired.min(active.len()));

        for instance in obsolete.iter().chain(&surplus) {
            if store.delete::<Instance>(namespace, &instance.metadata.name)? {
                summary.instances_deleted += 1;
            }
        }
        for _ in active.len()..desired {
            let suffix = &uuid::Uuid::new_v4().simple().to_string()[..6];
            let mut instance = Instance::new(
                namespace,
                &format!("{}-{suffix}", workload.metadata.name),
                InstanceSpec {
                    template: workload.spec.template.clone(),
                    node_name: None,
                    gpu_devices: Vec::new(),
                },
            );
            instance.metadata.labels = workload.metadata.labels.clone();
            instance.metadata.owner = Some(workload.owner_ref());
            store.create(instance)?;
            summary.instances_created += 1;
        }
    }
    Ok(())
}

/// Binds unbound instances to nodes, oldest first.
fn schedule_pending(store: &Store, summary: &mut Summary) -> Result<()> {
    let nodes = store.list::<Node>(None)?;
    let mut instances = store.list::<Instance>(None)?;
    let mut pending: Vec<usize> = (0..instances.len())
        .filter(|&i| instances[i].spec.node_name.is_none())
        .collect();
    pending.sort_by_key(|&i| {
        (
            instances[i].metadata.created_at_ms,
            instances[i].metadata.name.clone(),
        )
    });

    for index in pending {
        let (namespace, name) = (
            instances[index].metadata.namespace.clone(),
            instances[index].metadata.name.clone(),
        );
        let updated = match schedule(&instances[index], &nodes, &instances) {
            Ok(placement) => {
                summary.instances_scheduled += 1;
                tracing::info!(%namespace, %name, node = %placement.node, gpus = ?placement.gpu_devices, "scheduled");
                store.modify::<Instance>(&namespace, &name, |instance| {
                    instance.spec.node_name = Some(placement.node);
                    instance.spec.gpu_devices = placement.gpu_devices;
                    instance.status.phase = InstancePhase::Scheduled;
                    instance.status.message.clear();
                })
            }
            Err(unschedulable) => {
                summary.instances_unschedulable += 1;
                store.modify::<Instance>(&namespace, &name, |instance| {
                    instance.status.message = unschedulable.to_string();
                })
            }
        };
        match updated {
            // Later decisions in this pass must see this binding.
            Ok(instance) => instances[index] = instance,
            Err(StoreError::NotFound { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn update_status(store: &Store) -> Result<()> {
    for workload in store.list::<Workload>(None)? {
        let instances = owned_instances(store, &workload)?;
        let replicas = instances.len() as u32;
        let ready = instances
            .iter()
            .filter(|i| i.status.phase == InstancePhase::Running)
            .count() as u32;
        let generation = workload.metadata.generation;
        ignore_missing(store.modify::<Workload>(
            &workload.metadata.namespace,
            &workload.metadata.name,
            |w| {
                w.status.replicas = replicas;
                w.status.ready_replicas = ready;
                w.status.observed_generation = generation;
            },
        ))?;
    }
    for ai in store.list::<AIWorkload>(None)? {
        let (namespace, name) = (&ai.metadata.namespace, &ai.metadata.name);
        let Some(workload) = store.get::<Workload>(namespace, name)? else {
            continue;
        };
        if workload.metadata.owner.as_ref() != Some(&ai.owner_ref()) {
            continue;
        }
        let generation = ai.metadata.generation;
        ignore_missing(store.modify::<AIWorkload>(namespace, name, |a| {
            a.status.replicas = workload.status.replicas;
            a.status.ready_replicas = workload.status.ready_replicas;
            a.status.observed_generation = generation;
        }))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use orchestra_domain::{
        Capacity, Cpu, GpuDevice, InstanceTemplate, Memory, NodeSpec, ResourceRequirements, RuntimeSpec,
    };

    use super::*;

    const T0: u64 = 1_000_000;

    fn add_node(store: &Store, name: &str, gpus: u32) {
        let mut node = Node::new("", name, NodeSpec::default());
        node.status.phase = NodePhase::Ready;
        node.status.last_heartbeat_ms = T0;
        node.status.capacity = Capacity {
            cpu: Cpu::from_cores(16),
            memory: Memory::from_gib(64),
        };
        node.status.gpus = (0..gpus)
            .map(|i| GpuDevice {
                id: format!("gpu{i}"),
                vendor: "nvidia".into(),
                model: "test".into(),
                memory: Memory::from_gib(80),
                numa_node: Some(0),
            })
            .collect();
        store.create(node).unwrap();
    }

    fn web(replicas: u32, image: &str) -> Workload {
        Workload::new(
            "prod",
            "web",
            WorkloadSpec {
                replicas,
                template: InstanceTemplate {
                    runtime: RuntimeSpec::Container {
                        image: image.into(),
                        command: vec![],
                        env: Default::default(),
                    },
                    resources: ResourceRequirements {
                        cpu: Cpu::from_cores(2),
                        memory: Memory::from_gib(4),
                        gpu: None,
                    },
                    node_selector: Default::default(),
                },
            },
        )
    }

    fn reconcile(store: &Store, now: u64) -> Summary {
        reconcile_once(store, now, &Config::default()).unwrap()
    }

    /// Reconciles until a pass changes nothing, as the event loop would.
    fn settle(store: &Store, now: u64) {
        for _ in 0..10 {
            let before = store.revision();
            reconcile(store, now);
            if store.revision() == before {
                return;
            }
        }
        panic!("reconciliation did not converge");
    }

    fn mark_all_running(store: &Store) {
        for instance in store.list::<Instance>(None).unwrap() {
            if instance.spec.node_name.is_some() {
                store
                    .modify::<Instance>(&instance.metadata.namespace, &instance.metadata.name, |i| {
                        i.status.phase = InstancePhase::Running
                    })
                    .unwrap();
            }
        }
    }

    #[test]
    fn creates_schedules_and_converges() {
        let store = Store::in_memory();
        add_node(&store, "a", 0);
        add_node(&store, "b", 0);
        store.create(web(4, "web:1")).unwrap();

        let summary = reconcile(&store, T0);
        assert_eq!(summary.instances_created, 4);
        assert_eq!(summary.instances_scheduled, 4);

        let instances = store.list::<Instance>(None).unwrap();
        let on_a = instances.iter().filter(|i| i.is_bound_to("a")).count();
        assert_eq!(on_a, 2, "instances should spread across both nodes");

        mark_all_running(&store);
        settle(&store, T0);
        let workload = store.get::<Workload>("prod", "web").unwrap().unwrap();
        assert_eq!((workload.status.replicas, workload.status.ready_replicas), (4, 4));

        // Converged: a further pass is a strict no-op.
        let revision = store.revision();
        assert_eq!(reconcile(&store, T0), Summary::default());
        assert_eq!(store.revision(), revision);
    }

    #[test]
    fn scales_down_and_rolls_out_template_changes() {
        let store = Store::in_memory();
        add_node(&store, "a", 0);
        store.create(web(3, "web:1")).unwrap();
        settle(&store, T0);
        mark_all_running(&store);

        store.apply(web(1, "web:1")).unwrap();
        settle(&store, T0);
        let instances = store.list::<Instance>(None).unwrap();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].status.phase, InstancePhase::Running);
        let survivor = instances[0].metadata.name.clone();

        store.apply(web(1, "web:2")).unwrap();
        settle(&store, T0);
        let instances = store.list::<Instance>(None).unwrap();
        assert_eq!(instances.len(), 1);
        assert_ne!(instances[0].metadata.name, survivor);
        assert_eq!(instances[0].spec.template.runtime.image(), "web:2");
    }

    #[test]
    fn reschedules_instances_from_lost_nodes() {
        let store = Store::in_memory();
        add_node(&store, "a", 0);
        store.create(web(2, "web:1")).unwrap();
        settle(&store, T0);
        mark_all_running(&store);

        // Node `b` joins later and keeps sending heartbeats; `a` goes silent.
        let later = T0 + 60_000;
        add_node(&store, "b", 0);
        store
            .modify::<Node>("", "b", |n| n.status.last_heartbeat_ms = later)
            .unwrap();

        let summary = reconcile(&store, later);
        assert_eq!(summary.nodes_lost, 1);
        assert_eq!(summary.instances_deleted, 2);
        assert_eq!(summary.instances_scheduled, 2);
        assert!(
            store
                .list::<Instance>(None)
                .unwrap()
                .iter()
                .all(|i| i.is_bound_to("b"))
        );
        assert_eq!(
            store.get::<Node>("", "a").unwrap().unwrap().status.phase,
            NodePhase::NotReady
        );
    }

    #[test]
    fn control_plane_downtime_is_not_node_failure() {
        let store = Store::in_memory();
        add_node(&store, "a", 0);
        store.create(web(1, "web:1")).unwrap();
        settle(&store, T0);

        // The control plane comes back after an hour: the node gets a full
        // grace period to report in before being declared lost.
        let restart = T0 + 3_600_000;
        let config = Config {
            observing_since_ms: restart,
            ..Config::default()
        };
        let summary = reconcile_once(&store, restart + 5_000, &config).unwrap();
        assert_eq!(summary.nodes_lost, 0);
        let summary = reconcile_once(&store, restart + 25_000, &config).unwrap();
        assert_eq!(summary.nodes_lost, 1);
    }

    #[test]
    fn replaces_failed_instances_and_reports_unschedulable() {
        let store = Store::in_memory();
        add_node(&store, "a", 0);
        store.create(web(1, "web:1")).unwrap();
        settle(&store, T0);
        let first = store.list::<Instance>(None).unwrap().remove(0);
        store
            .modify::<Instance>("prod", &first.metadata.name, |i| {
                i.status.phase = InstancePhase::Failed
            })
            .unwrap();
        settle(&store, T0);
        let instances = store.list::<Instance>(None).unwrap();
        assert_eq!(instances.len(), 1);
        assert_ne!(instances[0].metadata.uid, first.metadata.uid);

        // 16 cores / 2 per replica = 8 fit; the 9th stays pending with a reason.
        store.apply(web(9, "web:1")).unwrap();
        settle(&store, T0);
        let pending: Vec<Instance> = store
            .list::<Instance>(None)
            .unwrap()
            .into_iter()
            .filter(|i| i.spec.node_name.is_none())
            .collect();
        assert_eq!(pending.len(), 1);
        assert!(
            pending[0].status.message.contains("insufficient cpu"),
            "{}",
            pending[0].status.message
        );
    }

    #[test]
    fn ai_workload_is_lowered_to_gpu_instances_and_cleaned_up() {
        let store = Store::in_memory();
        add_node(&store, "gpu-a", 4);
        add_node(&store, "gpu-b", 4);
        add_node(&store, "cpu-a", 0);
        let manifest = serde_json::json!({
            "metadata": {"name": "llm", "namespace": "ai"},
            "spec": {
                "model": {"name": "internal-llm", "version": "1.0"},
                "serving": {"engine": "vllm"},
                "resources": {"gpu": {"vendor": "nvidia", "count": 2, "memory": "80Gi"}},
                "scaling": {"minReplicas": 3, "maxReplicas": 8}
            }
        });
        let ai: AIWorkload = serde_json::from_value(manifest).unwrap();
        store.create(ai).unwrap();
        settle(&store, T0);

        let workload = store.get::<Workload>("ai", "llm").unwrap().unwrap();
        assert_eq!(workload.spec.replicas, 3);
        assert_eq!(workload.metadata.labels[MODEL_LABEL], "internal-llm");

        let instances = store.list::<Instance>(Some("ai")).unwrap();
        assert_eq!(instances.len(), 3);
        let mut reserved = BTreeSet::new();
        for instance in &instances {
            let node = instance.spec.node_name.clone().expect("all replicas fit");
            assert!(node.starts_with("gpu-"));
            assert_eq!(instance.spec.gpu_devices.len(), 2);
            for device in &instance.spec.gpu_devices {
                assert!(
                    reserved.insert((node.clone(), device.clone())),
                    "device reserved twice"
                );
            }
        }
        // Packing: the first node is filled before the second one is opened.
        assert_eq!(instances.iter().filter(|i| i.is_bound_to("gpu-a")).count(), 2);

        mark_all_running(&store);
        settle(&store, T0);
        let ai = store.get::<AIWorkload>("ai", "llm").unwrap().unwrap();
        assert_eq!((ai.status.replicas, ai.status.ready_replicas), (3, 3));

        store.delete::<AIWorkload>("ai", "llm").unwrap();
        settle(&store, T0);
        assert!(store.list::<Workload>(None).unwrap().is_empty());
        assert!(store.list::<Instance>(None).unwrap().is_empty());
    }
}

//! ORCHESTRA node agent: registers the machine, reports its inventory and
//! converges the local runtime on the instances assigned to it.

mod runtime;

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use anyhow::{Context, bail};
use clap::Parser;
use orchestra_domain::{
    Capacity, Cpu, GpuDevice, Instance, InstancePhase, InstanceStatusReport, Memory, NodeHeartbeat,
};

use crate::runtime::{Runtime, SimulatedRuntime};

#[derive(Parser)]
#[command(version, about = "ORCHESTRA node agent")]
struct Args {
    /// Control plane URL.
    #[arg(long, env = "ORCHESTRA_SERVER", default_value = "http://127.0.0.1:7600")]
    server: String,

    /// Node name; defaults to the hostname.
    #[arg(long, env = "ORCHESTRA_NODE_NAME")]
    name: Option<String>,

    /// Node label as `key=value`; repeatable.
    #[arg(long = "label", value_parser = parse_label)]
    labels: Vec<(String, String)>,

    /// Advertised CPU capacity; detected if omitted.
    #[arg(long)]
    cpu: Option<Cpu>,

    /// Advertised memory capacity; detected if omitted.
    #[arg(long)]
    memory: Option<Memory>,

    /// Advertise simulated GPUs as `vendor:count:memory[:model]`, e.g.
    /// `nvidia:4:80Gi:H100`; repeatable. Real device discovery is not
    /// implemented yet.
    #[arg(long = "simulate-gpu", value_parser = parse_gpus)]
    simulated_gpus: Vec<SimulatedGpus>,

    /// Seconds between two synchronizations with the control plane.
    #[arg(long, default_value_t = 5)]
    interval_secs: u64,
}

fn parse_label(text: &str) -> Result<(String, String), String> {
    match text.split_once('=') {
        Some((key, value)) if !key.is_empty() => Ok((key.to_string(), value.to_string())),
        _ => Err("expected key=value".to_string()),
    }
}

#[derive(Clone)]
struct SimulatedGpus {
    vendor: String,
    count: u32,
    memory: Memory,
    model: String,
}

fn parse_gpus(text: &str) -> Result<SimulatedGpus, String> {
    let parts: Vec<&str> = text.split(':').collect();
    let (vendor, count, memory) = match parts.as_slice() {
        [vendor, count, memory] | [vendor, count, memory, _] => (vendor, count, memory),
        _ => return Err("expected vendor:count:memory[:model]".to_string()),
    };
    Ok(SimulatedGpus {
        vendor: vendor.to_string(),
        count: count
            .parse()
            .map_err(|_| format!("invalid gpu count {count:?}"))?,
        memory: memory.parse().map_err(|e| format!("{e}"))?,
        model: parts.get(3).unwrap_or(&"simulated").to_string(),
    })
}

fn detect_hostname() -> Option<String> {
    let name = std::fs::read_to_string("/proc/sys/kernel/hostname").ok()?;
    let name = name.trim().to_lowercase();
    (!name.is_empty()).then_some(name)
}

fn detect_memory() -> Option<Memory> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(Memory::from_bytes(kib * 1024))
}

fn inventory(args: &Args) -> anyhow::Result<NodeHeartbeat> {
    let cpu = match args.cpu {
        Some(cpu) => cpu,
        None => Cpu::from_cores(std::thread::available_parallelism()?.get() as u64),
    };
    let memory = match args.memory.or_else(detect_memory) {
        Some(memory) => memory,
        None => bail!("cannot detect memory capacity; pass --memory"),
    };
    let mut gpus = Vec::new();
    for group in &args.simulated_gpus {
        for _ in 0..group.count {
            let index = gpus.len() as u32;
            gpus.push(GpuDevice {
                id: format!("gpu{index}"),
                vendor: group.vendor.clone(),
                model: group.model.clone(),
                memory: group.memory,
                // Simulated topology: two devices per NUMA node.
                numa_node: Some(index / 2),
            });
        }
    }
    Ok(NodeHeartbeat {
        labels: args.labels.iter().cloned().collect(),
        capacity: Capacity { cpu, memory },
        gpus,
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

struct Agent<R> {
    http: reqwest::Client,
    server: String,
    node: String,
    heartbeat: NodeHeartbeat,
    runtime: R,
    /// Uids whose latest outcome the control plane has acknowledged.
    reported: BTreeSet<String>,
}

impl<R: Runtime> Agent<R> {
    async fn desired_instances(&self) -> anyhow::Result<Vec<Instance>> {
        self.http
            .post(format!("{}/api/v1/nodes/{}/heartbeat", self.server, self.node))
            .json(&self.heartbeat)
            .send()
            .await?
            .error_for_status()?;
        let instances = self
            .http
            .get(format!("{}/api/v1/instances", self.server))
            .query(&[("node", &self.node)])
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        Ok(instances)
    }

    async fn report(&self, instance: &Instance, phase: InstancePhase, message: String) -> anyhow::Result<()> {
        let meta = &instance.metadata;
        self.http
            .put(format!(
                "{}/api/v1/namespaces/{}/instances/{}/status",
                self.server, meta.namespace, meta.name
            ))
            .json(&InstanceStatusReport {
                node: self.node.clone(),
                phase,
                message,
            })
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    /// One convergence pass. If the control plane cannot be reached nothing
    /// is touched: running workloads keep running until it is back.
    async fn sync(&mut self) -> anyhow::Result<()> {
        let desired = self.desired_instances().await.with_context(|| {
            format!(
                "control plane unreachable; keeping {} instance(s) running",
                self.runtime.running().len()
            )
        })?;
        let desired: BTreeMap<&str, &Instance> = desired
            .iter()
            .map(|instance| (instance.metadata.uid.as_str(), instance))
            .collect();

        for uid in self.runtime.running() {
            if !desired.contains_key(uid.as_str()) {
                if let Err(error) = self.runtime.stop(&uid).await {
                    tracing::error!(%uid, %error, "failed to stop instance; will retry");
                }
            }
        }
        self.reported.retain(|uid| desired.contains_key(uid.as_str()));

        let running: BTreeSet<String> = self.runtime.running().into_iter().collect();
        for (uid, instance) in desired {
            let outcome = if running.contains(uid) {
                if self.reported.contains(uid) && instance.status.phase == InstancePhase::Running {
                    continue;
                }
                Ok(())
            } else if self.reported.contains(uid) {
                // Start already failed and was reported; the control plane replaces it.
                continue;
            } else {
                self.runtime.start(instance).await
            };
            let (phase, message) = match outcome {
                Ok(()) => (InstancePhase::Running, String::new()),
                Err(error) => {
                    tracing::error!(instance = %instance.metadata.name, %error, "failed to start instance");
                    (InstancePhase::Failed, error.to_string())
                }
            };
            match self.report(instance, phase, message).await {
                Ok(()) => {
                    self.reported.insert(uid.to_string());
                }
                Err(error) => tracing::warn!(%error, "failed to report instance status; will retry"),
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();
    let node = match args.name.clone().or_else(detect_hostname) {
        Some(node) => node,
        None => bail!("cannot detect hostname; pass --name"),
    };
    let heartbeat = inventory(&args)?;
    tracing::info!(
        %node,
        server = %args.server,
        cpu = %heartbeat.capacity.cpu,
        memory = %heartbeat.capacity.memory,
        gpus = heartbeat.gpus.len(),
        "orchestra-agent starting (simulated runtime)"
    );

    let mut agent = Agent {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?,
        server: args.server.trim_end_matches('/').to_string(),
        node,
        heartbeat,
        runtime: SimulatedRuntime::default(),
        reported: BTreeSet::new(),
    };
    let mut ticker = tokio::time::interval(Duration::from_secs(args.interval_secs.max(1)));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                if let Err(error) = agent.sync().await {
                    tracing::warn!("{error:#}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
                return Ok(());
            }
        }
    }
}

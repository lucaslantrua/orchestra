//! `orchestra`: command-line client for the ORCHESTRA native API.

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use orchestra_domain::{AIWorkload, Instance, Node, Workload, now_ms};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;

#[derive(Parser)]
#[command(name = "orchestra", version, about = "ORCHESTRA command-line client")]
struct Cli {
    /// Control plane URL.
    #[arg(
        long,
        global = true,
        env = "ORCHESTRA_SERVER",
        default_value = "http://127.0.0.1:7600"
    )]
    server: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create or update resources from a YAML or JSON manifest.
    Apply {
        /// Manifest file; may contain several YAML documents.
        #[arg(short, long)]
        file: PathBuf,
    },
    /// List resources.
    Get {
        resource: Resource,
        /// Restrict to one namespace.
        #[arg(short, long)]
        namespace: Option<String>,
        /// Print raw JSON instead of a table.
        #[arg(long)]
        json: bool,
    },
    /// Delete a workload or AI workload (its instances follow).
    Delete {
        resource: Resource,
        name: String,
        #[arg(short, long, default_value = "default")]
        namespace: String,
    },
    /// Stop scheduling new instances on a node.
    Cordon { node: String },
    /// Allow scheduling on a node again.
    Uncordon { node: String },
}

#[derive(Clone, Copy, PartialEq, ValueEnum)]
enum Resource {
    #[value(alias = "node")]
    Nodes,
    #[value(alias = "workload")]
    Workloads,
    #[value(alias = "aiworkload", alias = "ai")]
    Aiworkloads,
    #[value(alias = "instance")]
    Instances,
}

impl Resource {
    fn plural(self) -> &'static str {
        match self {
            Resource::Nodes => "nodes",
            Resource::Workloads => "workloads",
            Resource::Aiworkloads => "aiworkloads",
            Resource::Instances => "instances",
        }
    }
}

struct Client {
    http: reqwest::Client,
    server: String,
}

impl Client {
    async fn send(&self, request: reqwest::RequestBuilder) -> anyhow::Result<reqwest::Response> {
        let response = request
            .send()
            .await
            .with_context(|| format!("cannot reach the control plane at {}", self.server))?;
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status();
        let body: Value = response.json().await.unwrap_or_default();
        match body.get("error").and_then(Value::as_str) {
            Some(message) => bail!("{message}"),
            None => bail!("server answered {status}"),
        }
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> anyhow::Result<T> {
        Ok(self
            .send(self.http.get(format!("{}{path}", self.server)))
            .await?
            .json()
            .await?)
    }
}

fn print_table(header: &[&str], rows: Vec<Vec<String>>) {
    let mut widths: Vec<usize> = header.iter().map(|h| h.len()).collect();
    for row in &rows {
        for (width, cell) in widths.iter_mut().zip(row) {
            *width = (*width).max(cell.chars().count());
        }
    }
    let print_row = |cells: Vec<&str>| {
        let line: Vec<String> = cells
            .iter()
            .zip(&widths)
            .map(|(cell, width)| format!("{cell:<width$}"))
            .collect();
        println!("{}", line.join("  ").trim_end());
    };
    print_row(header.to_vec());
    for row in &rows {
        print_row(row.iter().map(String::as_str).collect());
    }
}

fn age(since_ms: u64) -> String {
    let seconds = now_ms().saturating_sub(since_ms) / 1000;
    match seconds {
        0..60 => format!("{seconds}s"),
        60..3600 => format!("{}m", seconds / 60),
        3600..86400 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86400),
    }
}

async fn get(
    client: &Client,
    resource: Resource,
    namespace: Option<String>,
    json: bool,
) -> anyhow::Result<()> {
    let path = match (&namespace, resource) {
        (Some(ns), r) if r != Resource::Nodes => format!("/api/v1/namespaces/{ns}/{}", r.plural()),
        (_, r) => format!("/api/v1/{}", r.plural()),
    };
    if json {
        let value: Value = client.get(&path).await?;
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }
    match resource {
        Resource::Nodes => {
            let nodes: Vec<Node> = client.get(&path).await?;
            let rows = nodes
                .iter()
                .map(|n| {
                    let status = if n.spec.unschedulable {
                        format!("{:?},Cordoned", n.status.phase)
                    } else {
                        format!("{:?}", n.status.phase)
                    };
                    vec![
                        n.metadata.name.clone(),
                        status,
                        n.status.capacity.cpu.to_string(),
                        n.status.capacity.memory.to_string(),
                        n.status.gpus.len().to_string(),
                        age(n.status.last_heartbeat_ms),
                    ]
                })
                .collect();
            print_table(&["NAME", "STATUS", "CPU", "MEMORY", "GPUS", "HEARTBEAT"], rows);
        }
        Resource::Workloads => {
            let workloads: Vec<Workload> = client.get(&path).await?;
            let rows = workloads
                .iter()
                .map(|w| {
                    vec![
                        w.metadata.namespace.clone(),
                        w.metadata.name.clone(),
                        w.spec.template.runtime.type_name().to_string(),
                        format!("{}/{}", w.status.ready_replicas, w.spec.replicas),
                        w.spec.template.runtime.image().to_string(),
                        age(w.metadata.created_at_ms),
                    ]
                })
                .collect();
            print_table(&["NAMESPACE", "NAME", "TYPE", "READY", "IMAGE", "AGE"], rows);
        }
        Resource::Aiworkloads => {
            let workloads: Vec<AIWorkload> = client.get(&path).await?;
            let rows = workloads
                .iter()
                .map(|a| {
                    vec![
                        a.metadata.namespace.clone(),
                        a.metadata.name.clone(),
                        format!("{}:{}", a.spec.model.name, a.spec.model.version),
                        a.spec.serving.engine.name().to_string(),
                        a.spec.resources.gpu.count.to_string(),
                        format!("{}/{}", a.status.ready_replicas, a.status.replicas),
                        format!("{}-{}", a.spec.scaling.min_replicas, a.spec.scaling.max_replicas),
                    ]
                })
                .collect();
            print_table(
                &[
                    "NAMESPACE",
                    "NAME",
                    "MODEL",
                    "ENGINE",
                    "GPUS/REPLICA",
                    "READY",
                    "SCALING",
                ],
                rows,
            );
        }
        Resource::Instances => {
            let instances: Vec<Instance> = client.get(&path).await?;
            let rows = instances
                .iter()
                .map(|i| {
                    vec![
                        i.metadata.namespace.clone(),
                        i.metadata.name.clone(),
                        format!("{:?}", i.status.phase),
                        i.spec.node_name.clone().unwrap_or_else(|| "-".into()),
                        if i.spec.gpu_devices.is_empty() {
                            "-".into()
                        } else {
                            i.spec.gpu_devices.join(",")
                        },
                        i.status.message.clone(),
                    ]
                })
                .collect();
            print_table(&["NAMESPACE", "NAME", "PHASE", "NODE", "GPUS", "MESSAGE"], rows);
        }
    }
    Ok(())
}

async fn apply(client: &Client, file: PathBuf) -> anyhow::Result<()> {
    let text = std::fs::read_to_string(&file).with_context(|| format!("reading {}", file.display()))?;
    let mut applied = 0;
    for document in serde_norway::Deserializer::from_str(&text) {
        let manifest = Value::deserialize(document).with_context(|| format!("parsing {}", file.display()))?;
        if manifest.is_null() {
            continue;
        }
        let request = client
            .http
            .post(format!("{}/api/v1/apply", client.server))
            .json(&manifest);
        let result: Value = client.send(request).await?.json().await?;
        let field = |name: &str| result[name].as_str().unwrap_or("?").to_string();
        println!(
            "{} {}/{} {}",
            field("kind"),
            field("namespace"),
            field("name"),
            field("outcome")
        );
        applied += 1;
    }
    if applied == 0 {
        bail!("{} contains no manifest", file.display());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run(Cli::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let client = Client {
        http: reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()?,
        server: cli.server.trim_end_matches('/').to_string(),
    };
    match cli.command {
        Command::Apply { file } => apply(&client, file).await,
        Command::Get {
            resource,
            namespace,
            json,
        } => get(&client, resource, namespace, json).await,
        Command::Delete {
            resource,
            name,
            namespace,
        } => {
            if !matches!(resource, Resource::Workloads | Resource::Aiworkloads) {
                bail!("only workloads and aiworkloads can be deleted");
            }
            let url = format!(
                "{}/api/v1/namespaces/{namespace}/{}/{name}",
                client.server,
                resource.plural()
            );
            client.send(client.http.delete(url)).await?;
            println!("{namespace}/{name} deleted");
            Ok(())
        }
        Command::Cordon { node } => {
            client
                .send(
                    client
                        .http
                        .post(format!("{}/api/v1/nodes/{node}/cordon", client.server)),
                )
                .await?;
            println!("{node} cordoned");
            Ok(())
        }
        Command::Uncordon { node } => {
            client
                .send(
                    client
                        .http
                        .post(format!("{}/api/v1/nodes/{node}/uncordon", client.server)),
                )
                .await?;
            println!("{node} uncordoned");
            Ok(())
        }
    }
}

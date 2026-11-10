//! ORCHESTRA native REST API (`/api/v1`).
//!
//! **Security status:** this API has no authentication, authorization or TLS
//! yet (phase 7). Until then the server must only listen on a trusted
//! interface; it binds to loopback by default.

use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use orchestra_domain::{
    AIWorkload, Instance, InstancePhase, InstanceStatusReport, Node, NodeHeartbeat, NodePhase, NodeSpec,
    Object, Workload, now_ms,
};
use orchestra_state::{Store, StoreError};
use serde::Deserialize;
use serde_json::{Value, json};

type AppState = Arc<Store>;

pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        ApiError {
            status,
            message: message.into(),
        }
    }

    fn not_found(kind: &str, name: &str) -> Self {
        ApiError::new(StatusCode::NOT_FOUND, format!("{kind} {name:?} not found"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({ "error": self.message }))).into_response()
    }
}

impl From<StoreError> for ApiError {
    fn from(error: StoreError) -> Self {
        let status = match &error {
            StoreError::NotFound { .. } => StatusCode::NOT_FOUND,
            StoreError::AlreadyExists { .. } | StoreError::Conflict { .. } => StatusCode::CONFLICT,
            StoreError::Invalid(_) => StatusCode::UNPROCESSABLE_ENTITY,
            StoreError::Io(_) | StoreError::Serde(_) | StoreError::CorruptLog { .. } => {
                tracing::error!(%error, "internal error while serving a request");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        ApiError::new(status, error.to_string())
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

pub fn router(store: Arc<Store>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/v1/apply", post(apply))
        .route("/api/v1/nodes", get(list_all::<Node>))
        .route("/api/v1/nodes/{name}", get(get_node).delete(delete_node))
        .route("/api/v1/nodes/{name}/heartbeat", post(heartbeat))
        .route("/api/v1/nodes/{name}/cordon", post(cordon))
        .route("/api/v1/nodes/{name}/uncordon", post(uncordon))
        .route("/api/v1/instances", get(list_instances))
        .route(
            "/api/v1/namespaces/{namespace}/instances/{name}/status",
            put(report_instance_status),
        )
        .merge(namespaced_routes::<Workload>(true))
        .merge(namespaced_routes::<AIWorkload>(true))
        .merge(namespaced_routes::<Instance>(false))
        .with_state(store)
}

/// `/api/v1/<plural>` and `/api/v1/namespaces/{ns}/<plural>[/{name}]`.
/// Instances are owned by controllers, so they are exposed read-only.
fn namespaced_routes<T: Object>(deletable: bool) -> Router<AppState> {
    let plural = T::PLURAL;
    let item = if deletable {
        get(get_one::<T>).delete(delete_one::<T>)
    } else {
        get(get_one::<T>)
    };
    let mut router = Router::new()
        .route(
            &format!("/api/v1/namespaces/{{namespace}}/{plural}"),
            get(list_namespace::<T>),
        )
        .route(
            &format!("/api/v1/namespaces/{{namespace}}/{plural}/{{name}}"),
            item,
        );
    if T::KIND != Instance::KIND {
        router = router.route(&format!("/api/v1/{plural}"), get(list_all::<T>));
    }
    router
}

async fn list_all<T: Object>(State(store): State<AppState>) -> ApiResult<Vec<T>> {
    Ok(Json(store.list::<T>(None)?))
}

async fn list_namespace<T: Object>(
    State(store): State<AppState>,
    Path(namespace): Path<String>,
) -> ApiResult<Vec<T>> {
    Ok(Json(store.list::<T>(Some(&namespace))?))
}

async fn get_one<T: Object>(
    State(store): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<T> {
    store
        .get::<T>(&namespace, &name)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(T::KIND, &name))
}

async fn delete_one<T: Object>(
    State(store): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    if store.delete::<T>(&namespace, &name)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(T::KIND, &name))
    }
}

/// Declarative entry point: accepts any user-facing manifest and dispatches on `kind`.
async fn apply(State(store): State<AppState>, Json(manifest): Json<Value>) -> ApiResult<Value> {
    let kind = manifest
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    match kind.as_str() {
        Workload::KIND => apply_as::<Workload>(&store, manifest),
        AIWorkload::KIND => apply_as::<AIWorkload>(&store, manifest),
        "" => Err(ApiError::new(StatusCode::BAD_REQUEST, "manifest has no `kind`")),
        other => Err(ApiError::new(
            StatusCode::BAD_REQUEST,
            format!("kind {other:?} cannot be applied (supported: Workload, AIWorkload)"),
        )),
    }
}

fn apply_as<T: Object>(store: &Store, manifest: Value) -> ApiResult<Value> {
    let object: T = serde_json::from_value(manifest).map_err(|e| {
        ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!("invalid {}: {e}", T::KIND),
        )
    })?;
    let (stored, outcome) = store.apply(object)?;
    Ok(Json(json!({
        "kind": T::KIND,
        "namespace": stored.meta().namespace,
        "name": stored.meta().name,
        "outcome": outcome,
    })))
}

async fn get_node(State(store): State<AppState>, Path(name): Path<String>) -> ApiResult<Node> {
    store
        .get::<Node>("", &name)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("Node", &name))
}

async fn delete_node(
    State(store): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    if store.delete::<Node>("", &name)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("Node", &name))
    }
}

/// Registers the node on first contact, then refreshes its liveness and inventory.
async fn heartbeat(
    State(store): State<AppState>,
    Path(name): Path<String>,
    Json(report): Json<NodeHeartbeat>,
) -> ApiResult<Node> {
    let refresh = |node: &mut Node| {
        node.metadata.labels = report.labels.clone();
        node.status.phase = NodePhase::Ready;
        node.status.capacity = report.capacity;
        node.status.gpus = report.gpus.clone();
        node.status.agent_version = report.agent_version.clone();
        node.status.last_heartbeat_ms = now_ms();
    };
    if store.get::<Node>("", &name)?.is_none() {
        let mut node = Node::new("", &name, NodeSpec::default());
        refresh(&mut node);
        match store.create(node) {
            Ok(node) => return Ok(Json(node)),
            // Lost a registration race with ourselves: fall through to refresh.
            Err(StoreError::AlreadyExists { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(Json(store.modify::<Node>("", &name, refresh)?))
}

async fn cordon(State(store): State<AppState>, Path(name): Path<String>) -> ApiResult<Node> {
    Ok(Json(store.modify::<Node>("", &name, |node| {
        node.spec.unschedulable = true
    })?))
}

async fn uncordon(State(store): State<AppState>, Path(name): Path<String>) -> ApiResult<Node> {
    Ok(Json(store.modify::<Node>("", &name, |node| {
        node.spec.unschedulable = false
    })?))
}

#[derive(Deserialize)]
struct InstanceFilter {
    /// Only instances bound to this node.
    node: Option<String>,
}

async fn list_instances(
    State(store): State<AppState>,
    Query(filter): Query<InstanceFilter>,
) -> ApiResult<Vec<Instance>> {
    let mut instances = store.list::<Instance>(None)?;
    if let Some(node) = &filter.node {
        instances.retain(|instance| instance.is_bound_to(node));
    }
    Ok(Json(instances))
}

async fn report_instance_status(
    State(store): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Json(report): Json<InstanceStatusReport>,
) -> ApiResult<Instance> {
    if !matches!(report.phase, InstancePhase::Running | InstancePhase::Failed) {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "agents may only report the Running and Failed phases",
        ));
    }
    let mut bound = true;
    let instance = store.modify::<Instance>(&namespace, &name, |instance| {
        bound = instance.is_bound_to(&report.node);
        if bound {
            instance.status.phase = report.phase;
            instance.status.message = report.message.clone();
        }
    })?;
    if !bound {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            format!("instance {name:?} is not bound to node {:?}", report.node),
        ));
    }
    Ok(Json(instance))
}

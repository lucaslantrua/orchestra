//! ORCHESTRA native REST API (`/api/v1`).
//!
//! Every request is attributed to a [`Caller`] derived from its TLS client
//! certificate, and every handler states who may call it:
//!
//! * **administrators** manage all resources;
//! * **node agents** may only send their own heartbeat, read the instances
//!   bound to them, report those instances' status and renew their certificate;
//! * **anonymous** callers may only check health and redeem a join token.

pub mod audit;
pub mod auth;

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, Query, Request, State},
    http::{Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post, put},
};
use orchestra_domain::{
    AIWorkload, CertificateRequest, CertificateResponse, CreateJoinTokenRequest, CreateJoinTokenResponse,
    Instance, InstancePhase, InstanceStatusReport, JoinRequest, Node, NodeHeartbeat, NodePhase, NodeSpec,
    Object, Workload, meta::validate_name, now_ms,
};
use orchestra_pki::{CertificateAuthority, Identity, PkiError};
use orchestra_state::{Store, StoreError};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::audit::AuditSubject;
pub use crate::{audit::AuditLog, auth::Caller};

/// Longest lifetime a join token may be given.
const MAX_JOIN_TOKEN_TTL: Duration = Duration::from_secs(24 * 3600);

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub ca: Arc<CertificateAuthority>,
    /// Lifetime of the node certificates issued on join and renewal.
    pub node_cert_ttl: Duration,
    pub audit: Arc<AuditLog>,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    pub(crate) fn new(status: StatusCode, message: impl Into<String>) -> Self {
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

impl From<PkiError> for ApiError {
    fn from(error: PkiError) -> Self {
        match &error {
            PkiError::InvalidCsr(_) => ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, error.to_string()),
            _ => {
                tracing::error!(%error, "certificate issuance failed");
                ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, "certificate issuance failed")
            }
        }
    }
}

type ApiResult<T> = Result<Json<T>, ApiError>;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/api/v1/join", post(join))
        .route("/api/v1/jointokens", post(create_join_token))
        .route("/api/v1/apply", post(apply))
        .route("/api/v1/nodes", get(list_all::<Node>))
        .route("/api/v1/nodes/{name}", get(get_node).delete(delete_node))
        .route("/api/v1/nodes/{name}/heartbeat", post(heartbeat))
        .route("/api/v1/nodes/{name}/certificate", post(renew_certificate))
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
        .layer(middleware::from_fn_with_state(state.clone(), audit_requests))
        .with_state(state)
}

/// Records every denied request and every change, with who made it. Reads
/// and the agents' routine reports are left out to keep the trail readable.
async fn audit_requests(
    State(state): State<AppState>,
    caller: Caller,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let response = next.run(request).await;

    let status = response.status();
    let denied = matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN);
    let routine = matches!(caller, Caller::Node(_))
        && status.is_success()
        && (path.ends_with("/heartbeat") || path.ends_with("/status"));
    if denied || (method != Method::GET && !routine) {
        let subject = response
            .extensions()
            .get::<AuditSubject>()
            .map(|subject| subject.0.clone());
        state.audit.record(
            caller.to_string(),
            method.as_str(),
            &path,
            status.as_u16(),
            subject,
        );
    }
    response
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

async fn list_all<T: Object>(caller: Caller, State(state): State<AppState>) -> ApiResult<Vec<T>> {
    caller.require_admin()?;
    Ok(Json(state.store.list::<T>(None)?))
}

async fn list_namespace<T: Object>(
    caller: Caller,
    State(state): State<AppState>,
    Path(namespace): Path<String>,
) -> ApiResult<Vec<T>> {
    caller.require_admin()?;
    Ok(Json(state.store.list::<T>(Some(&namespace))?))
}

async fn get_one<T: Object>(
    caller: Caller,
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<T> {
    caller.require_admin()?;
    state
        .store
        .get::<T>(&namespace, &name)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found(T::KIND, &name))
}

async fn delete_one<T: Object>(
    caller: Caller,
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
) -> Result<StatusCode, ApiError> {
    caller.require_admin()?;
    if state.store.delete::<T>(&namespace, &name)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found(T::KIND, &name))
    }
}

/// Declarative entry point: accepts any user-facing manifest and dispatches on `kind`.
async fn apply(
    caller: Caller,
    State(state): State<AppState>,
    Json(manifest): Json<Value>,
) -> ApiResult<Value> {
    caller.require_admin()?;
    let kind = manifest
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    match kind.as_str() {
        Workload::KIND => apply_as::<Workload>(&state.store, manifest),
        AIWorkload::KIND => apply_as::<AIWorkload>(&state.store, manifest),
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

async fn create_join_token(
    caller: Caller,
    State(state): State<AppState>,
    Json(request): Json<CreateJoinTokenRequest>,
) -> ApiResult<CreateJoinTokenResponse> {
    caller.require_admin()?;
    if request.ttl_seconds == 0 || request.ttl_seconds > MAX_JOIN_TOKEN_TTL.as_secs() {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            format!(
                "ttlSeconds must be between 1 and {}",
                MAX_JOIN_TOKEN_TTL.as_secs()
            ),
        ));
    }
    let expires_at_ms = now_ms() + request.ttl_seconds * 1000;
    let token = auth::create_join_token(&state.store, expires_at_ms)?;
    Ok(Json(CreateJoinTokenResponse { token, expires_at_ms }))
}

/// Enrolls a new node: a valid join token buys a certificate for a node name
/// that is not in use. Open to anonymous callers by necessity.
async fn join(State(state): State<AppState>, Json(request): Json<JoinRequest>) -> Response {
    let subject = AuditSubject(format!("node:{}", request.node_name));
    let mut response = enroll(&state, request).into_response();
    response.extensions_mut().insert(subject);
    response
}

fn enroll(state: &AppState, request: JoinRequest) -> ApiResult<CertificateResponse> {
    auth::verify_join_token(&state.store, &request.token, now_ms())?;
    validate_name("node name", &request.node_name)
        .map_err(|e| ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, e.to_string()))?;
    // A token must not let its holder take over the identity of an existing
    // node: re-enrolling one requires an administrator to delete it first.
    if state.store.get::<Node>("", &request.node_name)?.is_some() {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            format!(
                "node {:?} is already enrolled; delete it before joining again",
                request.node_name
            ),
        ));
    }
    let identity = Identity::Node(request.node_name.clone());
    let certificate = state
        .ca
        .sign_client_csr(&request.csr, &identity, state.node_cert_ttl)?;
    // Reserve the name so that the same token cannot enroll it twice.
    state
        .store
        .create(Node::new("", &request.node_name, NodeSpec::default()))?;
    Ok(Json(CertificateResponse { certificate }))
}

async fn renew_certificate(
    caller: Caller,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<CertificateRequest>,
) -> ApiResult<CertificateResponse> {
    caller.require_node(&name)?;
    // Deleting a node is how an administrator evicts it: its certificate
    // stays valid until it expires, but can no longer be renewed.
    if state.store.get::<Node>("", &name)?.is_none() {
        return Err(ApiError::not_found("Node", &name));
    }
    let certificate = state
        .ca
        .sign_client_csr(&request.csr, &Identity::Node(name), state.node_cert_ttl)?;
    Ok(Json(CertificateResponse { certificate }))
}

async fn get_node(
    caller: Caller,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Node> {
    caller.require_admin()?;
    state
        .store
        .get::<Node>("", &name)?
        .map(Json)
        .ok_or_else(|| ApiError::not_found("Node", &name))
}

async fn delete_node(
    caller: Caller,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    caller.require_admin()?;
    if state.store.delete::<Node>("", &name)? {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::not_found("Node", &name))
    }
}

/// Refreshes a node's liveness and inventory. The node object itself is
/// created by enrollment, so a deleted node cannot heartbeat itself back.
async fn heartbeat(
    caller: Caller,
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(report): Json<NodeHeartbeat>,
) -> ApiResult<Node> {
    caller.require_node(&name)?;
    let node = state.store.modify::<Node>("", &name, |node| {
        node.metadata.labels = report.labels;
        node.status.phase = NodePhase::Ready;
        node.status.capacity = report.capacity;
        node.status.gpus = report.gpus;
        node.status.agent_version = report.agent_version;
        node.status.last_heartbeat_ms = now_ms();
    })?;
    Ok(Json(node))
}

async fn cordon(caller: Caller, State(state): State<AppState>, Path(name): Path<String>) -> ApiResult<Node> {
    caller.require_admin()?;
    Ok(Json(state.store.modify::<Node>("", &name, |node| {
        node.spec.unschedulable = true
    })?))
}

async fn uncordon(
    caller: Caller,
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> ApiResult<Node> {
    caller.require_admin()?;
    Ok(Json(state.store.modify::<Node>("", &name, |node| {
        node.spec.unschedulable = false
    })?))
}

#[derive(Deserialize)]
struct InstanceFilter {
    /// Only instances bound to this node.
    node: Option<String>,
}

/// Administrators see every instance; a node sees only its own.
async fn list_instances(
    caller: Caller,
    State(state): State<AppState>,
    Query(filter): Query<InstanceFilter>,
) -> ApiResult<Vec<Instance>> {
    match &filter.node {
        Some(node) if !matches!(caller, Caller::Admin(_)) => caller.require_node(node)?,
        _ => caller.require_admin()?,
    }
    let mut instances = state.store.list::<Instance>(None)?;
    if let Some(node) = &filter.node {
        instances.retain(|instance| instance.is_bound_to(node));
    }
    Ok(Json(instances))
}

async fn report_instance_status(
    caller: Caller,
    State(state): State<AppState>,
    Path((namespace, name)): Path<(String, String)>,
    Json(report): Json<InstanceStatusReport>,
) -> ApiResult<Instance> {
    caller.require_node(&report.node)?;
    if !matches!(report.phase, InstancePhase::Running | InstancePhase::Failed) {
        return Err(ApiError::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "agents may only report the Running and Failed phases",
        ));
    }
    let mut bound = true;
    let instance = state.store.modify::<Instance>(&namespace, &name, |instance| {
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

#[cfg(test)]
mod tests;

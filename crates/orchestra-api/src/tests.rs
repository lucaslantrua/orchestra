use axum::{
    Extension,
    body::Body,
    http::{Request, header},
};
use http_body_util::BodyExt;
use orchestra_domain::{Capacity, Cpu, InstanceSpec, InstanceTemplate, Memory, RuntimeSpec};
use tower::ServiceExt;

use super::*;

struct Harness {
    state: AppState,
}

impl Harness {
    fn new() -> Harness {
        let state = AppState {
            store: Arc::new(Store::in_memory()),
            ca: Arc::new(CertificateAuthority::generate("test").unwrap()),
            node_cert_ttl: Duration::from_secs(3600),
            audit: Arc::new(AuditLog::in_memory()),
        };
        Harness { state }
    }

    /// Sends a request as `caller`, the way the TLS layer would attribute it.
    async fn call(
        &self,
        caller: &Caller,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder().method(method).uri(path);
        let request = match body {
            Some(body) => request
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap())),
            None => request.body(Body::empty()),
        }
        .unwrap();
        let app = router(self.state.clone()).layer(Extension(caller.clone()));
        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
    }

    async fn token(&self) -> String {
        let (status, body) = self
            .call(
                &admin(),
                "POST",
                "/api/v1/jointokens",
                Some(json!({"ttlSeconds": 600})),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        body["token"].as_str().unwrap().to_string()
    }

    async fn join(&self, token: &str, node: &str) -> (StatusCode, Value) {
        let (csr, _key) = orchestra_pki::generate_csr().unwrap();
        let body = json!({"token": token, "nodeName": node, "csr": csr});
        self.call(&Caller::Anonymous, "POST", "/api/v1/join", Some(body))
            .await
    }
}

fn admin() -> Caller {
    Caller::Admin("alice".into())
}

fn node(name: &str) -> Caller {
    Caller::Node(name.into())
}

fn heartbeat_body() -> Value {
    serde_json::to_value(NodeHeartbeat {
        labels: Default::default(),
        capacity: Capacity {
            cpu: Cpu::from_cores(4),
            memory: Memory::from_gib(8),
        },
        gpus: vec![],
        agent_version: "test".into(),
    })
    .unwrap()
}

fn workload_manifest() -> Value {
    json!({
        "kind": "Workload",
        "metadata": {"name": "web"},
        "spec": {"template": {"runtime": {"type": "container", "image": "web:1"}}}
    })
}

#[tokio::test]
async fn anonymous_callers_are_limited_to_health() {
    let h = Harness::new();
    let anonymous = Caller::Anonymous;
    assert_eq!(
        h.call(&anonymous, "GET", "/healthz", None).await.0,
        StatusCode::OK
    );
    for (method, path, body) in [
        ("GET", "/api/v1/nodes", None),
        ("GET", "/api/v1/workloads", None),
        ("GET", "/api/v1/instances", None),
        ("GET", "/api/v1/instances?node=a", None),
        ("POST", "/api/v1/apply", Some(workload_manifest())),
        ("POST", "/api/v1/jointokens", Some(json!({"ttlSeconds": 60}))),
        ("POST", "/api/v1/nodes/a/heartbeat", Some(heartbeat_body())),
        ("POST", "/api/v1/nodes/a/cordon", None),
        ("DELETE", "/api/v1/namespaces/default/workloads/web", None),
    ] {
        let (status, _) = h.call(&anonymous, method, path, body).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{method} {path}");
    }
}

#[tokio::test]
async fn nodes_can_only_act_for_themselves() {
    let h = Harness::new();
    let token = h.token().await;
    assert_eq!(h.join(&token, "a").await.0, StatusCode::OK);
    assert_eq!(h.join(&token, "b").await.0, StatusCode::OK);

    // Their own business is allowed...
    assert_eq!(
        h.call(
            &node("a"),
            "POST",
            "/api/v1/nodes/a/heartbeat",
            Some(heartbeat_body())
        )
        .await
        .0,
        StatusCode::OK
    );
    assert_eq!(
        h.call(&node("a"), "GET", "/api/v1/instances?node=a", None)
            .await
            .0,
        StatusCode::OK
    );

    // ...everything else is forbidden.
    for (method, path, body) in [
        ("POST", "/api/v1/nodes/b/heartbeat", Some(heartbeat_body())),
        ("GET", "/api/v1/instances?node=b", None),
        ("GET", "/api/v1/instances", None),
        ("GET", "/api/v1/nodes", None),
        ("POST", "/api/v1/apply", Some(workload_manifest())),
        ("POST", "/api/v1/jointokens", Some(json!({"ttlSeconds": 60}))),
        ("POST", "/api/v1/nodes/a/cordon", None),
        ("DELETE", "/api/v1/nodes/b", None),
    ] {
        let (status, _) = h.call(&node("a"), method, path, body).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {path}");
    }

    // An administrator is not a node either: it cannot forge heartbeats.
    let (status, _) = h
        .call(
            &admin(),
            "POST",
            "/api/v1/nodes/a/heartbeat",
            Some(heartbeat_body()),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn nodes_cannot_report_status_for_instances_of_other_nodes() {
    let h = Harness::new();
    let token = h.token().await;
    h.join(&token, "a").await;
    h.join(&token, "b").await;
    let mut instance = Instance::new(
        "default",
        "web-1",
        InstanceSpec {
            template: InstanceTemplate {
                runtime: RuntimeSpec::Container {
                    image: "web:1".into(),
                    command: vec![],
                    env: Default::default(),
                },
                resources: Default::default(),
                node_selector: Default::default(),
            },
            node_name: Some("a".into()),
            gpu_devices: vec![],
        },
    );
    instance.status.phase = InstancePhase::Scheduled;
    h.state.store.create(instance).unwrap();
    let path = "/api/v1/namespaces/default/instances/web-1/status";

    // Node b claiming to be node a.
    let spoofed = json!({"node": "a", "phase": "Failed", "message": "sabotage"});
    assert_eq!(
        h.call(&node("b"), "PUT", path, Some(spoofed)).await.0,
        StatusCode::FORBIDDEN
    );
    // Node b reporting honestly about an instance it does not host.
    let foreign = json!({"node": "b", "phase": "Failed", "message": "sabotage"});
    assert_eq!(
        h.call(&node("b"), "PUT", path, Some(foreign)).await.0,
        StatusCode::CONFLICT
    );

    let genuine = json!({"node": "a", "phase": "Running"});
    assert_eq!(
        h.call(&node("a"), "PUT", path, Some(genuine)).await.0,
        StatusCode::OK
    );
    let stored = h
        .state
        .store
        .get::<Instance>("default", "web-1")
        .unwrap()
        .unwrap();
    assert_eq!(stored.status.phase, InstancePhase::Running);
}

#[tokio::test]
async fn join_requires_a_valid_token_and_a_free_name() {
    let h = Harness::new();
    let token = h.token().await;

    let (status, body) = h.join(&token, "node-a").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["certificate"]
            .as_str()
            .unwrap()
            .contains("BEGIN CERTIFICATE")
    );

    // The token cannot be used to take over an enrolled node...
    assert_eq!(h.join(&token, "node-a").await.0, StatusCode::CONFLICT);
    // ...and wrong, malformed or unknown tokens all fail the same way.
    let (id, _) = token.split_once('.').unwrap();
    for bad in [
        format!("{id}.wrong"),
        "garbage".to_string(),
        "jtunknown00.secret".to_string(),
        String::new(),
    ] {
        let (status, body) = h.join(&bad, "node-b").await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{bad:?}");
        assert_eq!(body["error"], "invalid or expired join token");
    }
    assert_eq!(
        h.join(&token, "Bad_Name").await.0,
        StatusCode::UNPROCESSABLE_ENTITY
    );
    let no_csr = json!({"token": token, "nodeName": "node-c", "csr": "nope"});
    let (status, _) = h
        .call(&Caller::Anonymous, "POST", "/api/v1/join", Some(no_csr))
        .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(h.state.store.get::<Node>("", "node-c").unwrap().is_none());
}

#[tokio::test]
async fn expired_tokens_are_rejected() {
    let h = Harness::new();
    let token = auth::create_join_token(&h.state.store, now_ms() - 1).unwrap();
    assert_eq!(h.join(&token, "node-a").await.0, StatusCode::UNAUTHORIZED);
    for ttl in [0, 7 * 24 * 3600] {
        let (status, _) = h
            .call(
                &admin(),
                "POST",
                "/api/v1/jointokens",
                Some(json!({"ttlSeconds": ttl})),
            )
            .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }
}

#[tokio::test]
async fn deleted_nodes_cannot_renew_or_come_back() {
    let h = Harness::new();
    let token = h.token().await;
    h.join(&token, "a").await;
    let (csr, _key) = orchestra_pki::generate_csr().unwrap();
    let renewal = json!({"csr": csr});

    let (status, body) = h
        .call(
            &node("a"),
            "POST",
            "/api/v1/nodes/a/certificate",
            Some(renewal.clone()),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        body["certificate"]
            .as_str()
            .unwrap()
            .contains("BEGIN CERTIFICATE")
    );
    assert_eq!(
        h.call(
            &node("a"),
            "POST",
            "/api/v1/nodes/b/certificate",
            Some(renewal.clone())
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    assert_eq!(
        h.call(&admin(), "DELETE", "/api/v1/nodes/a", None).await.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        h.call(&node("a"), "POST", "/api/v1/nodes/a/certificate", Some(renewal))
            .await
            .0,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        h.call(
            &node("a"),
            "POST",
            "/api/v1/nodes/a/heartbeat",
            Some(heartbeat_body())
        )
        .await
        .0,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn audit_records_changes_and_denials_but_not_routine_traffic() {
    let h = Harness::new();
    let token = h.token().await;
    h.join(&token, "a").await;
    h.call(&admin(), "POST", "/api/v1/apply", Some(workload_manifest()))
        .await;
    h.call(&admin(), "GET", "/api/v1/workloads", None).await;
    h.call(
        &node("a"),
        "POST",
        "/api/v1/nodes/a/heartbeat",
        Some(heartbeat_body()),
    )
    .await;
    h.call(&node("a"), "GET", "/api/v1/nodes", None).await;
    h.call(&Caller::Anonymous, "GET", "/api/v1/workloads", None).await;

    let trail: Vec<(String, String, String, u16)> = h
        .state
        .audit
        .entries()
        .into_iter()
        .map(|e| {
            (
                e.caller
                    + e.subject
                        .as_deref()
                        .map(|s| format!(" -> {s}"))
                        .as_deref()
                        .unwrap_or(""),
                e.method,
                e.path,
                e.status,
            )
        })
        .collect();
    let entry = |caller: &str, method: &str, path: &str, status: u16| {
        (caller.to_string(), method.to_string(), path.to_string(), status)
    };
    assert_eq!(
        trail,
        [
            entry("admin:alice", "POST", "/api/v1/jointokens", 200),
            entry("anonymous -> node:a", "POST", "/api/v1/join", 200),
            entry("admin:alice", "POST", "/api/v1/apply", 200),
            entry("node:a", "GET", "/api/v1/nodes", 403),
            entry("anonymous", "GET", "/api/v1/workloads", 401),
        ]
    );
}

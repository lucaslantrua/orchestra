//! ORCHESTRA state engine.
//!
//! A versioned object store with optimistic concurrency, change notifications
//! and an optional write-ahead log for durability on a single node.
//!
//! This is the seam where consensus plugs in: once the control plane is
//! replicated (Raft, phase 3), [`Store`] becomes the state machine that
//! committed log entries are applied to, and its public API stays the same.

mod wal;

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use orchestra_domain::{Object, ValidationError, now_ms};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::wal::Wal;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{kind} {name:?} not found")]
    NotFound { kind: &'static str, name: String },
    #[error("{kind} {name:?} already exists")]
    AlreadyExists { kind: &'static str, name: String },
    #[error("{kind} {name:?} was modified concurrently (expected version {expected}, found {actual})")]
    Conflict {
        kind: &'static str,
        name: String,
        expected: u64,
        actual: u64,
    },
    #[error("invalid object: {0}")]
    Invalid(#[from] ValidationError),
    #[error("state log I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("state log is corrupt at line {line}")]
    CorruptLog { line: usize },
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Key {
    pub kind: String,
    pub namespace: String,
    pub name: String,
}

impl Key {
    fn of<T: Object>(namespace: &str, name: &str) -> Key {
        Key {
            kind: T::KIND.to_string(),
            namespace: if T::NAMESPACED {
                namespace.to_string()
            } else {
                String::new()
            },
            name: name.to_string(),
        }
    }

    fn of_object<T: Object>(object: &T) -> Key {
        Key::of::<T>(&object.meta().namespace, &object.meta().name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeType {
    Put,
    Delete,
}

/// Notification of a committed change. Carries no payload: consumers are
/// level-triggered and re-read the state they care about.
#[derive(Clone, Debug)]
pub struct WatchEvent {
    pub revision: u64,
    pub change: ChangeType,
    pub key: Key,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ApplyOutcome {
    Created,
    Configured,
    Unchanged,
}

struct Inner {
    revision: u64,
    objects: BTreeMap<Key, Value>,
    wal: Option<Wal>,
}

pub struct Store {
    inner: Mutex<Inner>,
    events: broadcast::Sender<WatchEvent>,
}

impl Store {
    /// A volatile store; state is lost when the process exits.
    pub fn in_memory() -> Store {
        Store::from_parts(0, BTreeMap::new(), None)
    }

    /// Opens (or creates) a durable store backed by the log at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Store> {
        let (wal, revision, objects) = Wal::open(path.as_ref())?;
        Ok(Store::from_parts(revision, objects, Some(wal)))
    }

    fn from_parts(revision: u64, objects: BTreeMap<Key, Value>, wal: Option<Wal>) -> Store {
        let (events, _) = broadcast::channel(1024);
        Store {
            inner: Mutex::new(Inner {
                revision,
                objects,
                wal,
            }),
            events,
        }
    }

    fn lock(&self) -> MutexGuard<'_, Inner> {
        // Every mutation is applied in a single step after all fallible work,
        // so the state behind a poisoned lock is still consistent.
        self.inner.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Revision of the latest committed change.
    pub fn revision(&self) -> u64 {
        self.lock().revision
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WatchEvent> {
        self.events.subscribe()
    }

    pub fn get<T: Object>(&self, namespace: &str, name: &str) -> Result<Option<T>> {
        let inner = self.lock();
        match inner.objects.get(&Key::of::<T>(namespace, name)) {
            Some(value) => Ok(Some(serde_json::from_value(value.clone())?)),
            None => Ok(None),
        }
    }

    /// Lists objects of a kind, optionally restricted to one namespace,
    /// ordered by namespace then name.
    pub fn list<T: Object>(&self, namespace: Option<&str>) -> Result<Vec<T>> {
        let inner = self.lock();
        let start = Key::of::<T>(namespace.unwrap_or(""), "");
        inner
            .objects
            .range(start..)
            .take_while(|(key, _)| {
                key.kind == T::KIND && namespace.is_none_or(|ns| !T::NAMESPACED || key.namespace == ns)
            })
            .map(|(_, value)| Ok(serde_json::from_value(value.clone())?))
            .collect()
    }

    pub fn create<T: Object>(&self, mut object: T) -> Result<T> {
        object.normalize();
        object.validate()?;
        let key = Key::of_object(&object);
        let mut inner = self.lock();
        if inner.objects.contains_key(&key) {
            return Err(StoreError::AlreadyExists {
                kind: T::KIND,
                name: key.name,
            });
        }
        let meta = object.meta_mut();
        meta.uid = uuid::Uuid::new_v4().to_string();
        meta.created_at_ms = now_ms();
        meta.generation = 1;
        self.commit(&mut inner, key, object)
    }

    /// Replaces an object. A non-zero `resourceVersion` must match the stored
    /// one, otherwise the write is rejected with [`StoreError::Conflict`].
    pub fn update<T: Object>(&self, object: T) -> Result<T> {
        let key = Key::of_object(&object);
        let mut inner = self.lock();
        let Some(old) = inner.objects.get(&key).cloned() else {
            return Err(StoreError::NotFound {
                kind: T::KIND,
                name: key.name,
            });
        };
        let expected = object.meta().resource_version;
        let actual = stored_version(&old);
        if expected != 0 && expected != actual {
            return Err(StoreError::Conflict {
                kind: T::KIND,
                name: key.name,
                expected,
                actual,
            });
        }
        self.replace(&mut inner, key, &old, object)
    }

    /// Atomic read-modify-write. Nothing is committed (and no event is
    /// emitted) if `mutate` leaves the object unchanged.
    pub fn modify<T: Object>(&self, namespace: &str, name: &str, mutate: impl FnOnce(&mut T)) -> Result<T> {
        let key = Key::of::<T>(namespace, name);
        let mut inner = self.lock();
        let Some(old) = inner.objects.get(&key).cloned() else {
            return Err(StoreError::NotFound {
                kind: T::KIND,
                name: key.name,
            });
        };
        let mut object: T = serde_json::from_value(old.clone())?;
        mutate(&mut object);
        self.replace(&mut inner, key, &old, object)
    }

    /// Declarative write: creates the object, or replaces the `spec` and
    /// labels of an existing one while preserving its status and identity.
    pub fn apply<T: Object>(&self, mut object: T) -> Result<(T, ApplyOutcome)> {
        object.normalize();
        let key = Key::of_object(&object);
        let mut inner = self.lock();
        let Some(old) = inner.objects.get(&key).cloned() else {
            drop(inner);
            return Ok((self.create(object)?, ApplyOutcome::Created));
        };
        let incoming = serde_json::to_value(&object)?;
        let mut merged = old.clone();
        merged["spec"] = incoming["spec"].clone();
        match incoming["metadata"].get("labels") {
            Some(labels) => merged["metadata"]["labels"] = labels.clone(),
            None => {
                if let Some(metadata) = merged["metadata"].as_object_mut() {
                    metadata.remove("labels");
                }
            }
        }
        let merged: T = serde_json::from_value(merged)?;
        let before = inner.revision;
        let stored = self.replace(&mut inner, key, &old, merged)?;
        let outcome = if inner.revision == before {
            ApplyOutcome::Unchanged
        } else {
            ApplyOutcome::Configured
        };
        Ok((stored, outcome))
    }

    /// Returns whether the object existed.
    pub fn delete<T: Object>(&self, namespace: &str, name: &str) -> Result<bool> {
        let key = Key::of::<T>(namespace, name);
        let mut inner = self.lock();
        if !inner.objects.contains_key(&key) {
            return Ok(false);
        }
        let revision = inner.revision + 1;
        if let Some(wal) = &mut inner.wal {
            wal.append(revision, &key, None)?;
        }
        inner.revision = revision;
        inner.objects.remove(&key);
        let _ = self.events.send(WatchEvent {
            revision,
            change: ChangeType::Delete,
            key,
        });
        Ok(true)
    }

    fn replace<T: Object>(&self, inner: &mut Inner, key: Key, old: &Value, mut object: T) -> Result<T> {
        object.normalize();
        object.validate()?;
        // Identity and bookkeeping fields are owned by the store.
        let previous: T = serde_json::from_value(old.clone())?;
        let previous = previous.meta();
        let meta = object.meta_mut();
        meta.name = previous.name.clone();
        meta.namespace = previous.namespace.clone();
        meta.uid = previous.uid.clone();
        meta.created_at_ms = previous.created_at_ms;
        meta.generation = previous.generation;
        meta.resource_version = previous.resource_version;

        let value = serde_json::to_value(&object)?;
        if value == *old {
            return Ok(object);
        }
        if value.get("spec") != old.get("spec") {
            object.meta_mut().generation += 1;
        }
        self.commit(inner, key, object)
    }

    fn commit<T: Object>(&self, inner: &mut Inner, key: Key, mut object: T) -> Result<T> {
        let revision = inner.revision + 1;
        object.meta_mut().resource_version = revision;
        let value = serde_json::to_value(&object)?;
        if let Some(wal) = &mut inner.wal {
            wal.append(revision, &key, Some(&value))?;
        }
        inner.revision = revision;
        inner.objects.insert(key.clone(), value);
        // Sending only fails when nobody is subscribed.
        let _ = self.events.send(WatchEvent {
            revision,
            change: ChangeType::Put,
            key,
        });
        Ok(object)
    }
}

fn stored_version(value: &Value) -> u64 {
    value["metadata"]["resourceVersion"].as_u64().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use orchestra_domain::{
        InstanceTemplate, Node, NodeSpec, ResourceRequirements, RuntimeSpec, Workload, WorkloadSpec,
    };

    use super::*;

    fn workload(namespace: &str, name: &str, replicas: u32) -> Workload {
        Workload::new(
            namespace,
            name,
            WorkloadSpec {
                replicas,
                template: InstanceTemplate {
                    runtime: RuntimeSpec::Container {
                        image: "registry.internal/web:1.0".into(),
                        command: vec![],
                        env: Default::default(),
                    },
                    resources: ResourceRequirements::default(),
                    node_selector: Default::default(),
                },
            },
        )
    }

    #[test]
    fn create_get_list_delete() {
        let store = Store::in_memory();
        let created = store.create(workload("prod", "web", 2)).unwrap();
        assert_eq!(created.metadata.generation, 1);
        assert_eq!(created.metadata.resource_version, 1);
        assert!(!created.metadata.uid.is_empty());
        store.create(workload("dev", "web", 1)).unwrap();
        store
            .create(Node::new("", "node-a", NodeSpec::default()))
            .unwrap();

        assert!(matches!(
            store.create(workload("prod", "web", 2)),
            Err(StoreError::AlreadyExists { .. })
        ));
        assert_eq!(store.list::<Workload>(None).unwrap().len(), 2);
        assert_eq!(store.list::<Workload>(Some("prod")).unwrap().len(), 1);
        assert_eq!(store.list::<Workload>(Some("nope")).unwrap().len(), 0);
        assert_eq!(store.list::<Node>(None).unwrap().len(), 1);

        assert!(store.delete::<Workload>("prod", "web").unwrap());
        assert!(!store.delete::<Workload>("prod", "web").unwrap());
        assert!(store.get::<Workload>("prod", "web").unwrap().is_none());
    }

    #[test]
    fn rejects_invalid_objects() {
        let store = Store::in_memory();
        assert!(matches!(
            store.create(workload("prod", "Not_Valid", 1)),
            Err(StoreError::Invalid(_))
        ));
    }

    #[test]
    fn update_detects_conflicts() {
        let store = Store::in_memory();
        let mut first = store.create(workload("prod", "web", 1)).unwrap();
        let mut stale = first.clone();

        first.spec.replicas = 2;
        let updated = store.update(first).unwrap();
        assert_eq!(updated.metadata.generation, 2);

        stale.spec.replicas = 5;
        assert!(matches!(store.update(stale), Err(StoreError::Conflict { .. })));
        assert_eq!(
            store
                .get::<Workload>("prod", "web")
                .unwrap()
                .unwrap()
                .spec
                .replicas,
            2
        );
    }

    #[test]
    fn generation_tracks_spec_only_and_noop_writes_are_silent() {
        let store = Store::in_memory();
        store.create(workload("prod", "web", 1)).unwrap();
        let mut events = store.subscribe();

        let after_status = store
            .modify::<Workload>("prod", "web", |w| w.status.ready_replicas = 1)
            .unwrap();
        assert_eq!(after_status.metadata.generation, 1);
        assert_eq!(after_status.metadata.resource_version, 2);

        store.modify::<Workload>("prod", "web", |_| {}).unwrap();
        assert_eq!(store.revision(), 2);

        assert_eq!(events.try_recv().unwrap().revision, 2);
        assert!(events.try_recv().is_err());
    }

    #[test]
    fn apply_preserves_status_and_identity() {
        let store = Store::in_memory();
        let (created, outcome) = store.apply(workload("prod", "web", 1)).unwrap();
        assert_eq!(outcome, ApplyOutcome::Created);
        store
            .modify::<Workload>("prod", "web", |w| w.status.ready_replicas = 1)
            .unwrap();

        let (_, outcome) = store.apply(workload("prod", "web", 1)).unwrap();
        assert_eq!(outcome, ApplyOutcome::Unchanged);

        let (applied, outcome) = store.apply(workload("prod", "web", 3)).unwrap();
        assert_eq!(outcome, ApplyOutcome::Configured);
        assert_eq!(applied.spec.replicas, 3);
        assert_eq!(applied.status.ready_replicas, 1);
        assert_eq!(applied.metadata.uid, created.metadata.uid);
        assert_eq!(applied.metadata.generation, 2);
    }

    #[test]
    fn state_survives_restart_and_torn_tail() {
        let dir = std::env::temp_dir().join(format!("orchestra-state-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("state.log");
        {
            let store = Store::open(&path).unwrap();
            store.create(workload("prod", "web", 2)).unwrap();
            store.create(workload("prod", "api", 1)).unwrap();
            store.delete::<Workload>("prod", "api").unwrap();
        }
        // Simulate a crash in the middle of an append.
        {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            file.write_all(b"{\"rev\":4,\"key\":{\"kind\":\"Wor").unwrap();
        }
        let store = Store::open(&path).unwrap();
        assert_eq!(store.revision(), 3);
        let all = store.list::<Workload>(None).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].metadata.name, "web");
        // New writes continue the revision sequence and survive another restart.
        let created = store.create(workload("prod", "db", 1)).unwrap();
        assert_eq!(created.metadata.resource_version, 4);
        drop(store);
        let store = Store::open(&path).unwrap();
        assert_eq!(store.revision(), 4);
        assert_eq!(store.list::<Workload>(None).unwrap().len(), 2);
        std::fs::remove_dir_all(&dir).unwrap();
    }
}

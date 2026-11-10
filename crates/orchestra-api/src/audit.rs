//! Append-only audit trail of security-relevant API activity.

use std::{
    fs::{File, OpenOptions},
    io::Write,
    path::Path,
    sync::Mutex,
};

use orchestra_domain::now_ms;
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditEntry {
    pub time_ms: u64,
    /// `admin:<name>`, `node:<name>` or `anonymous`.
    pub caller: String,
    pub method: String,
    pub path: String,
    pub status: u16,
    /// What the request was about, when the path does not say (for example
    /// the node being enrolled).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

/// Response extension through which a handler names the subject of its
/// audit entry.
#[derive(Clone)]
pub struct AuditSubject(pub String);

enum Sink {
    File(File),
    Memory(Vec<AuditEntry>),
}

/// One JSON object per line, only ever appended to.
pub struct AuditLog {
    sink: Mutex<Sink>,
}

impl AuditLog {
    pub fn open(path: &Path) -> std::io::Result<AuditLog> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(AuditLog {
            sink: Mutex::new(Sink::File(file)),
        })
    }

    /// Keeps entries in memory; for tests.
    pub fn in_memory() -> AuditLog {
        AuditLog {
            sink: Mutex::new(Sink::Memory(Vec::new())),
        }
    }

    pub fn record(&self, caller: String, method: &str, path: &str, status: u16, subject: Option<String>) {
        let entry = AuditEntry {
            time_ms: now_ms(),
            caller,
            method: method.to_string(),
            path: path.to_string(),
            status,
            subject,
        };
        let mut sink = self.sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        match &mut *sink {
            Sink::File(file) => {
                let mut line = serde_json::to_vec(&entry).unwrap_or_default();
                line.push(b'\n');
                if let Err(error) = file.write_all(&line) {
                    // The request has already been served; the best we can do
                    // is make the gap in the trail loud.
                    tracing::error!(%error, ?entry, "failed to write audit entry");
                }
            }
            Sink::Memory(entries) => entries.push(entry),
        }
    }

    /// Entries recorded so far by an in-memory log.
    pub fn entries(&self) -> Vec<AuditEntry> {
        match &*self.sink.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) {
            Sink::Memory(entries) => entries.clone(),
            Sink::File(_) => Vec::new(),
        }
    }
}

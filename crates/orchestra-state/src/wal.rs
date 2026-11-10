//! Write-ahead log: one JSON record per line, replayed and compacted on open.

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Key, Result, StoreError};

/// `key` + `value` is a put, `key` alone a delete, and neither a revision
/// marker (written by compaction so the revision counter never goes back).
#[derive(Serialize, Deserialize)]
struct Record {
    rev: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    key: Option<Key>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

pub(crate) struct Wal {
    file: File,
}

impl Wal {
    /// Replays the log at `path`, rewrites it as a compact snapshot and
    /// returns it ready for appending.
    pub(crate) fn open(path: &Path) -> Result<(Wal, u64, BTreeMap<Key, Value>)> {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(parent)?;
        }
        let mut revision = 0;
        let mut objects = BTreeMap::new();
        if path.exists() {
            let content = fs::read_to_string(path)?;
            let lines: Vec<&str> = content.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let record: Record = match serde_json::from_str(line) {
                    Ok(record) => record,
                    // A torn final record is an append interrupted by a crash:
                    // it was never acknowledged, so it is safe to drop.
                    Err(_) if index == lines.len() - 1 => {
                        tracing::warn!(line = index + 1, "dropping torn record at end of state log");
                        break;
                    }
                    Err(_) => return Err(StoreError::CorruptLog { line: index + 1 }),
                };
                revision = revision.max(record.rev);
                match (record.key, record.value) {
                    (Some(key), Some(value)) => {
                        objects.insert(key, value);
                    }
                    (Some(key), None) => {
                        objects.remove(&key);
                    }
                    (None, _) => {}
                }
            }
        }

        // Compact: write the live state to a temporary file and swap it in.
        let tmp = path.with_extension("compacting");
        let mut snapshot = File::create(&tmp)?;
        write_record(
            &mut snapshot,
            &Record {
                rev: revision,
                key: None,
                value: None,
            },
        )?;
        for (key, value) in &objects {
            let rev = value["metadata"]["resourceVersion"].as_u64().unwrap_or(revision);
            write_record(
                &mut snapshot,
                &Record {
                    rev,
                    key: Some(key.clone()),
                    value: Some(value.clone()),
                },
            )?;
        }
        snapshot.sync_all()?;
        fs::rename(&tmp, path)?;

        let file = OpenOptions::new().append(true).open(path)?;
        Ok((Wal { file }, revision, objects))
    }

    pub(crate) fn append(&mut self, rev: u64, key: &Key, value: Option<&Value>) -> Result<()> {
        write_record(
            &mut self.file,
            &Record {
                rev,
                key: Some(key.clone()),
                value: value.cloned(),
            },
        )?;
        // Durability against power loss: without this a committed write could
        // be acknowledged and then lost.
        self.file.sync_data()?;
        Ok(())
    }
}

fn write_record(file: &mut File, record: &Record) -> Result<()> {
    let mut line = serde_json::to_vec(record)?;
    line.push(b'\n');
    file.write_all(&line)?;
    Ok(())
}

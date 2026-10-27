//! Shared semantic importer for checkpoint and pending-write records.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::Value;
use tinyflows::graph::{Checkpoint, CheckpointConfig, Checkpointer, PendingWrite};
use tinyflows_bus::catalog::{
    ImportLegacyBatchResponse, LegacyImportKind, LegacyImportProblem, LegacyImportRecord,
    LegacyImportTally,
};
use tinyflows_drivers::DriverCheckpointer;

#[derive(Default)]
struct Counts {
    written: u64,
    existing: u64,
    invalid: u64,
}

/// Imports raw checkpoint/write DTOs through the shared checkpointer writer.
///
/// Like the original one-time importer, this requires exclusive migration
/// startup access to the target scope before live run writers start.
pub async fn apply_legacy_checkpoint_batch(
    records: &[LegacyImportRecord],
    checkpointer: &DriverCheckpointer<Value>,
) -> Result<ImportLegacyBatchResponse> {
    let mut counts = BTreeMap::<LegacyImportKind, Counts>::new();
    let mut problems = Vec::new();
    let mut checkpoints = Vec::new();
    let mut writes = Vec::new();
    for record in records {
        match record {
            LegacyImportRecord::Checkpoint { raw_json } => {
                match serde_json::from_str::<Checkpoint<Value>>(raw_json) {
                    Ok(checkpoint) => checkpoints.push(checkpoint),
                    Err(error) => invalid(
                        &mut counts,
                        &mut problems,
                        LegacyImportKind::Checkpoint,
                        None,
                        error.to_string(),
                    ),
                }
            }
            LegacyImportRecord::SqliteCheckpointWrite(row) => {
                let decoded = (|| -> Result<_> {
                    Ok((
                        CheckpointConfig {
                            thread_id: row.thread_id.clone(),
                            checkpoint_id: Some(row.checkpoint_id.clone()),
                            namespace: serde_json::from_str(&row.namespace_json)
                                .context("write namespace is corrupt")?,
                        },
                        PendingWrite {
                            node: tinyflows::graph::ids::NodeId::new(row.node.clone()),
                            task_id: row.task_id.clone(),
                            idx: row.idx,
                            channel: row.channel.clone(),
                            payload: serde_json::from_str(&row.payload_json)
                                .context("write payload is corrupt")?,
                        },
                    ))
                })();
                match decoded {
                    Ok(value) => writes.push(value),
                    Err(error) => invalid(
                        &mut counts,
                        &mut problems,
                        LegacyImportKind::Write,
                        Some(&row.task_id),
                        error.to_string(),
                    ),
                }
            }
            LegacyImportRecord::FileCheckpointWrite {
                thread_id,
                raw_json,
            } => match serde_json::from_str::<FileWrite>(raw_json) {
                Ok(row) => writes.push((
                    CheckpointConfig {
                        thread_id: thread_id.clone(),
                        checkpoint_id: Some(row.checkpoint_id),
                        namespace: row.namespace,
                    },
                    row.write,
                )),
                Err(error) => invalid(
                    &mut counts,
                    &mut problems,
                    LegacyImportKind::Write,
                    Some(thread_id),
                    error.to_string(),
                ),
            },
            LegacyImportRecord::Invalid {
                record_kind,
                identity,
                message,
            } if matches!(
                record_kind,
                LegacyImportKind::Checkpoint | LegacyImportKind::Write
            ) =>
            {
                invalid(
                    &mut counts,
                    &mut problems,
                    *record_kind,
                    identity.as_deref(),
                    message.clone(),
                );
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "checkpoint batch contains a non-checkpoint record"
                ));
            }
        }
    }
    replay(checkpointer, checkpoints, writes, &mut counts).await?;
    Ok(ImportLegacyBatchResponse {
        tallies: counts
            .into_iter()
            .map(|(kind, count)| LegacyImportTally {
                kind,
                written: count.written,
                existing: count.existing,
                invalid: count.invalid,
            })
            .collect(),
        problems,
    })
}

#[derive(Deserialize)]
struct FileWrite {
    #[serde(default)]
    namespace: Vec<String>,
    checkpoint_id: String,
    write: PendingWrite,
}

fn invalid(
    counts: &mut BTreeMap<LegacyImportKind, Counts>,
    problems: &mut Vec<LegacyImportProblem>,
    kind: LegacyImportKind,
    identity: Option<&str>,
    message: String,
) {
    counts.entry(kind).or_default().invalid += 1;
    problems.push(LegacyImportProblem {
        kind,
        identity: identity.map(|value| value.chars().take(128).collect()),
        message: message.chars().take(240).collect(),
    });
}

async fn replay(
    checkpointer: &DriverCheckpointer<Value>,
    checkpoints: Vec<Checkpoint<Value>>,
    writes: Vec<(CheckpointConfig, PendingWrite)>,
    counts: &mut BTreeMap<LegacyImportKind, Counts>,
) -> Result<()> {
    for checkpoint in checkpoints {
        let held = checkpointer
            .get_scoped(
                &checkpoint.thread_id,
                Some(&checkpoint.checkpoint_id),
                &checkpoint.namespace,
            )
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?
            .is_some();
        if held {
            counts
                .entry(LegacyImportKind::Checkpoint)
                .or_default()
                .existing += 1;
            continue;
        }
        checkpointer
            .put(checkpoint)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        counts
            .entry(LegacyImportKind::Checkpoint)
            .or_default()
            .written += 1;
    }
    let mut groups: Vec<(CheckpointConfig, Vec<PendingWrite>)> = Vec::new();
    for (config, write) in writes {
        match groups.iter_mut().find(|(known, _)| *known == config) {
            Some((_, group)) => group.push(write),
            None => groups.push((config, vec![write])),
        }
    }
    for (config, group) in groups {
        let (written, existing) = checkpointer
            .import_writes_if_absent(&config, &group)
            .await
            .map_err(|error| anyhow::anyhow!("{error}"))?;
        let count = counts.entry(LegacyImportKind::Write).or_default();
        count.written += written as u64;
        count.existing += existing as u64;
    }
    Ok(())
}

//! Shared semantic writer for legacy flow and checkpoint records.

use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result};
use serde_json::Value;
use tinyflows_bus::catalog::{
    ImportLegacyBatchResponse, LegacyImportKind, LegacyImportProblem, LegacyImportRecord,
    LegacyImportTally,
};
use tinyflows_catalog::{
    Flow, FlowDraft, FlowRevision, FlowRun, FlowRunStep, FlowSuggestion, SuggestionStatus,
};
use tinyflows_drivers::{DriverCheckpointer, catalog::FlowCatalogDocuments};

#[derive(Default)]
struct Counts {
    written: u64,
    existing: u64,
    invalid: u64,
}

/// Applies one already bounded batch through the caller's scoped catalog and checkpointer.
///
/// Source parsing stays in the host-only reader; this shared writer owns legacy
/// semantic decoding, migration, idempotent catalog writes and checkpoint replay.
/// The host must run all batches during exclusive migration startup, before
/// run execution or other writers can mutate the target scope. Checkpoints use
/// the append-only sequence protocol and are not atomically fenced from a live
/// checkpointer writer.
pub async fn apply_legacy_batch(
    records: &[LegacyImportRecord],
    catalog: &FlowCatalogDocuments,
    checkpointer: Option<&DriverCheckpointer<Value>>,
) -> Result<ImportLegacyBatchResponse> {
    let mut counts: BTreeMap<LegacyImportKind, Counts> = BTreeMap::new();
    let mut problems = Vec::new();
    let mut imported_flows = HashSet::new();
    let mut checkpoint_records = Vec::new();

    for record in records {
        match record {
            LegacyImportRecord::Flow(row) => match decode_flow(row) {
                Ok(flow) => match catalog.import_flow(&flow).await {
                    Ok(wrote) => {
                        tally(&mut counts, LegacyImportKind::Flow, wrote);
                        imported_flows.insert(flow.id);
                    }
                    Err(error) => return Err(error),
                },
                Err(error) => problem(
                    &mut counts,
                    &mut problems,
                    LegacyImportKind::Flow,
                    Some(&row.id),
                    error.to_string(),
                ),
            },
            LegacyImportRecord::Revision(row) => {
                let decoded = (|| -> Result<FlowRevision> {
                    let graph =
                        serde_json::from_str(&row.graph_json).context("graph_json is not JSON")?;
                    Ok(FlowRevision {
                        id: row.id.clone(),
                        flow_id: row.flow_id.clone(),
                        graph,
                        name: row.name.clone(),
                        require_approval: row.require_approval,
                        created_at: row.created_at.clone(),
                    })
                })();
                match decoded {
                    Ok(value) => {
                        if imported_flows.contains(&value.flow_id)
                            || catalog.get_flow(&value.flow_id).await?.is_some()
                        {
                            tally(
                                &mut counts,
                                LegacyImportKind::Revision,
                                catalog.import_revision(&value).await?,
                            );
                        } else {
                            problem(
                                &mut counts,
                                &mut problems,
                                LegacyImportKind::Revision,
                                Some(&value.id),
                                "owner flow was not imported".into(),
                            );
                        }
                    }
                    Err(error) => problem(
                        &mut counts,
                        &mut problems,
                        LegacyImportKind::Revision,
                        Some(&row.id),
                        error.to_string(),
                    ),
                }
            }
            LegacyImportRecord::Run(row) => {
                let decoded = (|| -> Result<FlowRun> {
                    Ok(FlowRun {
                        id: row.id.clone(),
                        flow_id: row.flow_id.clone(),
                        thread_id: row.thread_id.clone(),
                        status: row.status.clone(),
                        started_at: row.started_at.clone(),
                        finished_at: row.finished_at.clone(),
                        steps: serde_json::from_str::<Vec<FlowRunStep>>(&row.steps_json)
                            .context("run steps are corrupt")?,
                        pending_approvals: serde_json::from_str(&row.pending_approvals_json)
                            .context("pending approvals are corrupt")?,
                        error: row.error.clone(),
                        graph_hash: row.graph_hash.clone(),
                    })
                })();
                match decoded {
                    Ok(value) => {
                        if imported_flows.contains(&value.flow_id)
                            || catalog.get_flow(&value.flow_id).await?.is_some()
                        {
                            tally(
                                &mut counts,
                                LegacyImportKind::Run,
                                catalog.import_run(&value).await?,
                            );
                        } else {
                            problem(
                                &mut counts,
                                &mut problems,
                                LegacyImportKind::Run,
                                Some(&value.id),
                                "owner flow was not imported".into(),
                            );
                        }
                    }
                    Err(error) => problem(
                        &mut counts,
                        &mut problems,
                        LegacyImportKind::Run,
                        Some(&row.id),
                        error.to_string(),
                    ),
                }
            }
            LegacyImportRecord::State(row) => {
                match serde_json::from_str::<Value>(&row.value_json) {
                    Ok(value) => tally(
                        &mut counts,
                        LegacyImportKind::State,
                        catalog.import_kv(&row.namespace, &row.key, &value).await?,
                    ),
                    Err(error) => problem(
                        &mut counts,
                        &mut problems,
                        LegacyImportKind::State,
                        Some(&row.key),
                        error.to_string(),
                    ),
                }
            }
            LegacyImportRecord::Suggestion(row) => {
                let decoded = (|| -> Result<FlowSuggestion> {
                    Ok(FlowSuggestion {
                        id: row.id.clone(),
                        title: row.title.clone(),
                        one_liner: row.one_liner.clone(),
                        rationale: row.rationale.clone(),
                        trigger_hint: row.trigger_hint.clone(),
                        steps_outline: serde_json::from_str(&row.steps_json)
                            .context("suggestion steps are corrupt")?,
                        suggested_connections: serde_json::from_str(&row.connections_json)
                            .context("suggestion connections are corrupt")?,
                        suggested_slugs: serde_json::from_str(&row.slugs_json)
                            .context("suggestion slugs are corrupt")?,
                        build_prompt: row.build_prompt.clone(),
                        confidence: row.confidence,
                        status: SuggestionStatus::from_str_lossy(&row.status),
                        created_at: row.created_at.clone(),
                        source_run_id: row.source_run_id.clone(),
                    })
                })();
                match decoded {
                    Ok(value) => tally(
                        &mut counts,
                        LegacyImportKind::Suggestion,
                        catalog.import_suggestion(&value).await?,
                    ),
                    Err(error) => problem(
                        &mut counts,
                        &mut problems,
                        LegacyImportKind::Suggestion,
                        Some(&row.id),
                        error.to_string(),
                    ),
                }
            }
            LegacyImportRecord::Draft(row) => match serde_json::from_slice::<FlowDraft>(&row.bytes)
            {
                Ok(value) if safe_draft_id(&value.id) => tally(
                    &mut counts,
                    LegacyImportKind::Draft,
                    catalog.import_draft(&value).await?,
                ),
                Ok(value) => problem(
                    &mut counts,
                    &mut problems,
                    LegacyImportKind::Draft,
                    Some(&row.file_name),
                    format!("'{}' is not a usable draft id", value.id),
                ),
                Err(error) => problem(
                    &mut counts,
                    &mut problems,
                    LegacyImportKind::Draft,
                    Some(&row.file_name),
                    error.to_string(),
                ),
            },
            LegacyImportRecord::Checkpoint { .. }
            | LegacyImportRecord::SqliteCheckpointWrite(_)
            | LegacyImportRecord::FileCheckpointWrite { .. } => {
                checkpoint_records.push(record.clone());
            }
            LegacyImportRecord::Invalid {
                record_kind: LegacyImportKind::Checkpoint | LegacyImportKind::Write,
                ..
            } => {
                checkpoint_records.push(record.clone());
            }
            LegacyImportRecord::Invalid {
                record_kind,
                identity,
                message,
            } => problem(
                &mut counts,
                &mut problems,
                *record_kind,
                identity.as_deref(),
                message.clone(),
            ),
        }
    }
    if !checkpoint_records.is_empty() {
        let checkpointer =
            checkpointer.context("checkpoint import requires a scoped checkpointer")?;
        let response = crate::catalog_legacy_checkpoint::apply_legacy_checkpoint_batch(
            &checkpoint_records,
            checkpointer,
        )
        .await?;
        for tally in response.tallies {
            let count = counts.entry(tally.kind).or_default();
            count.written += tally.written;
            count.existing += tally.existing;
            count.invalid += tally.invalid;
        }
        problems.extend(response.problems);
    }
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

fn decode_flow(row: &tinyflows_bus::catalog::LegacyFlowRecord) -> Result<Flow> {
    let raw: Value = serde_json::from_str(&row.graph_json).context("graph_json is not JSON")?;
    let migrated = tinyflows::migrate::migrate(raw).map_err(|error| anyhow::anyhow!("{error}"))?;
    let graph = serde_json::from_value(migrated).context("graph_json does not decode")?;
    Ok(Flow {
        id: row.id.clone(),
        name: row.name.clone(),
        graph,
        enabled: row.enabled,
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        last_run_at: row.last_run_at.clone(),
        last_status: row.last_status.clone(),
        require_approval: row.require_approval,
    })
}

fn safe_draft_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '-' || character == '_'
        })
}

fn tally(counts: &mut BTreeMap<LegacyImportKind, Counts>, kind: LegacyImportKind, wrote: bool) {
    let count = counts.entry(kind).or_default();
    if wrote {
        count.written += 1
    } else {
        count.existing += 1
    }
}

fn problem(
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

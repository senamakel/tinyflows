//! Parked-run expiration, boot reconciliation, and resume claims.

use anyhow::{Context, Result};
use serde_json::json;
use tinyflows_storage::{DocumentStoreExt, ErrorKind, Filter, Query, Sort, Versioned};

use super::lineage::{belongs_in, live_flows, reclaim};
use super::runs::{parked_before, set_finished, status};
use super::{FlowCatalogDocuments, RUNS, compare_and_swap, instant_ms, int, set_optional, text};

impl FlowCatalogDocuments {
    /// Cancels every parked `pending_approval` run parked (its `finished_at`,
    /// else `started_at`) strictly before `cutoff`, stamping `now` and
    /// `error_msg`. Returns the `(run_id, flow_id)` this call **actually**
    /// flipped — a run resumed in between is left alone and not returned.
    pub async fn expire_parked_runs(
        &self,
        cutoff: &str,
        now: &str,
        error_msg: &str,
    ) -> Result<Vec<(String, String)>> {
        let cutoff = instant_ms(cutoff);
        let docs = self.docs().await?;
        let parked = docs
            .query_all(
                RUNS,
                &Query::filter(Filter::eq("status", "pending_approval")),
            )
            .await?;
        let flows = live_flows(docs).await?;
        let (parked, orphans): (Vec<_>, Vec<_>) = parked
            .into_iter()
            .partition(|run| belongs_in(&run.doc, &flows));
        reclaim(docs, RUNS, &orphans).await;
        let mut swept = Vec::new();
        for run in parked {
            if !parked_before(&run.doc, cutoff) {
                continue;
            }
            let flipped = compare_and_swap(docs, RUNS, &run.id, |doc| {
                // Re-checked on the current document: a run resumed and
                // parked again since the query has a newer parking time.
                if status(doc) != "pending_approval" || !parked_before(doc, cutoff) {
                    return None;
                }
                let mut next = doc.clone();
                next["status"] = json!("cancelled");
                set_finished(&mut next, Some(now));
                next["error"] = json!(error_msg);
                Some(next)
            })
            .await
            .context("Failed to expire parked flow run")?;
            match flipped {
                Some(stored) => swept.push((
                    stored.id.clone(),
                    text(&stored.doc, "flow_id").unwrap_or_default().to_string(),
                )),
                None => tracing::debug!(
                    target: "flows",
                    run_id = %run.id,
                    "[flows] TTL sweep: run left 'pending_approval' concurrently — not expiring it"
                ),
            }
        }
        if !swept.is_empty() {
            tracing::info!(target: "flows", swept = swept.len(), "[flows] expired parked pending_approval runs past TTL");
        }
        Ok(swept)
    }

    /// Lists owner-scoped parked runs older than `cutoff` in deterministic id
    /// order. Each row is transitioned separately with
    /// [`Self::expire_parked_run`] so one failing write cannot hide prior wins.
    pub async fn list_expirable_parked_run_ids(
        &self,
        cutoff: &str,
    ) -> Result<Vec<(String, String)>> {
        let cutoff = instant_ms(cutoff);
        let docs = self.docs().await?;
        let parked = docs
            .query_all(
                RUNS,
                &Query::filter(Filter::eq("status", "pending_approval")),
            )
            .await?;
        let flows = live_flows(docs).await?;
        let (parked, orphans): (Vec<_>, Vec<_>) = parked
            .into_iter()
            .partition(|run| belongs_in(&run.doc, &flows));
        reclaim(docs, RUNS, &orphans).await;
        let mut candidates: Vec<(String, String)> = parked
            .into_iter()
            .filter(|run| parked_before(&run.doc, cutoff))
            .map(|run| {
                (
                    run.id,
                    text(&run.doc, "flow_id").unwrap_or_default().to_string(),
                )
            })
            .collect();
        candidates.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(candidates)
    }

    /// Expires one parked row only if it still has the same parked status and
    /// age observed by [`Self::list_expirable_parked_run_ids`].
    pub async fn expire_parked_run(
        &self,
        id: &str,
        cutoff: &str,
        now: &str,
        error_msg: &str,
    ) -> Result<bool> {
        let cutoff = instant_ms(cutoff);
        let docs = self.docs().await?;
        let flipped = compare_and_swap(docs, RUNS, id, |doc| {
            if status(doc) != "pending_approval" || !parked_before(doc, cutoff) {
                return None;
            }
            let mut next = doc.clone();
            next["status"] = json!("cancelled");
            set_finished(&mut next, Some(now));
            next["error"] = json!(error_msg);
            Some(next)
        })
        .await
        .context("Failed to expire parked flow run")?;
        Ok(flipped.is_some())
    }

    /// The `(id, flow_id)` of every `running` run started strictly before
    /// `started_before`, for the boot orphan sweep (B42).
    pub async fn list_running_run_ids(
        &self,
        started_before: &str,
    ) -> Result<Vec<(String, String)>> {
        let started_before = instant_ms(started_before);
        let docs = self.docs().await?;
        let running = docs
            .query_all(
                RUNS,
                &Query::filter(Filter::eq("status", "running")).sort(Sort::asc("started_ms")),
            )
            .await?;
        let flows = live_flows(docs).await?;
        let (running, orphans): (Vec<_>, Vec<_>) = running
            .into_iter()
            .partition(|run| belongs_in(&run.doc, &flows));
        reclaim(docs, RUNS, &orphans).await;
        Ok(running
            .iter()
            .filter(|run| int(&run.doc, "started_ms") < started_before)
            .map(|run| {
                (
                    run.id.clone(),
                    text(&run.doc, "flow_id").unwrap_or_default().to_string(),
                )
            })
            .collect())
    }

    /// Test-only unconditional status write, bypassing the liveness guard, to
    /// stage a run at an arbitrary status.
    #[cfg(any(test, feature = "test-fixtures"))]
    pub async fn force_run_status_for_test(
        &self,
        id: &str,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let docs = self.docs().await?;
        compare_and_swap(docs, RUNS, id, |doc| {
            let mut next = doc.clone();
            next["status"] = json!(status);
            set_optional(&mut next, "error", error);
            Some(next)
        })
        .await
        .context("Failed to force flow run status (test fixture)")?;
        Ok(())
    }

    /// Flips a parked run back to `running` for the duration of a resume,
    /// only while it is still `pending_approval`; returns whether it flipped.
    pub async fn mark_run_resuming(&self, id: &str) -> Result<bool> {
        let docs = self.docs().await?;
        let flipped = compare_and_swap(docs, RUNS, id, |doc| {
            if status(doc) != "pending_approval" {
                return None;
            }
            let mut next = doc.clone();
            next["status"] = json!("running");
            set_finished(&mut next, None);
            set_optional(&mut next, "error", None);
            Some(next)
        })
        .await
        .context("Failed to mark parked flow run as resuming")?;
        if flipped.is_some() {
            tracing::debug!(target: "flows", run_id = id, "[flows] marked parked run 'running' for the duration of the resume");
        }
        Ok(flipped.is_some())
    }

    /// Atomically claims a parked run and binds the lease of the actor that
    /// will resume it. The lease update shares the same parked-status CAS, so
    /// boot reconciliation cannot mistake the claimed run for an orphan.
    pub async fn claim_parked_flow_run_for_resume(&self, id: &str, lease: &str) -> Result<bool> {
        let docs = self.docs().await?;
        let flipped = compare_and_swap(docs, RUNS, id, |doc| {
            if status(doc) != "pending_approval" || lease.trim().is_empty() {
                return None;
            }
            let mut next = doc.clone();
            let snapshot = next.get_mut("execution_snapshot")?.as_object_mut()?;
            snapshot.insert("lease".into(), json!(lease));
            next["status"] = json!("running");
            set_finished(&mut next, None);
            set_optional(&mut next, "error", None);
            Some(next)
        })
        .await
        .context("Failed to claim parked flow run for resume")?;
        Ok(flipped.is_some())
    }

    /// Atomically adopts a legacy parked row that has no execution snapshot.
    /// The exact version returned by [`Self::get_flow_run_versioned`] fences
    /// the claim against a concurrent replacement; the marker records only
    /// resume-time context and must not be treated as an original run
    /// snapshot. A previously adopted marker is accepted for a later repark.
    pub async fn claim_legacy_parked_flow_run_for_resume(
        &self,
        expected: &Versioned<serde_json::Value>,
        lease: &str,
        marker: serde_json::Value,
    ) -> Result<bool> {
        if expected.id.trim().is_empty() || lease.trim().is_empty() || !marker.is_object() {
            return Ok(false);
        }
        let existing_snapshot = expected.doc.get("execution_snapshot");
        if status(&expected.doc) != "pending_approval"
            || existing_snapshot.is_some_and(|existing| {
                existing.get("legacy_resume") != Some(&serde_json::Value::Bool(true))
            })
        {
            return Ok(false);
        }
        let mut next = expected.doc.clone();
        next["status"] = serde_json::json!("running");
        next["execution_snapshot"] = marker;
        next["execution_snapshot"]["legacy_resume"] = serde_json::json!(true);
        next["execution_snapshot"]["lease"] = serde_json::json!(lease);
        set_finished(&mut next, None);
        set_optional(&mut next, "error", None);
        let docs = self.docs().await?;
        match docs
            .put(RUNS, &expected.id, next, expected.unchanged())
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::Conflict => Ok(false),
            Err(error) => Err(error).context("Failed to claim legacy parked flow run for resume"),
        }
    }

    /// Settles a legacy parked row as failed only if it is still the exact
    /// version observed before legacy-resume preflight. This keeps a missing
    /// checkpoint from failing a replacement row that arrived during the
    /// owner-scoped checkpoint read.
    pub async fn fail_legacy_parked_flow_run_for_resume(
        &self,
        expected: &Versioned<serde_json::Value>,
        finished_at: &str,
        steps: &[tinyflows_catalog::FlowRunStep],
        error: &str,
    ) -> Result<bool> {
        if status(&expected.doc) != "pending_approval"
            || expected
                .doc
                .get("execution_snapshot")
                .is_some_and(|snapshot| {
                    snapshot.get("legacy_resume") != Some(&serde_json::Value::Bool(true))
                })
        {
            return Ok(false);
        }
        let mut next = expected.doc.clone();
        next["status"] = serde_json::json!("failed");
        next["steps"] =
            serde_json::to_value(steps).context("serializing legacy resume failure steps")?;
        next["pending_approvals"] = serde_json::json!([]);
        next["finished_ms"] = serde_json::json!(instant_ms(finished_at));
        next["error"] = serde_json::json!(error);
        next.as_object_mut().unwrap().remove("graph_hash");
        let docs = self.docs().await?;
        match docs
            .put(RUNS, &expected.id, next, expected.unchanged())
            .await
        {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == ErrorKind::Conflict => Ok(false),
            Err(error) => Err(error).context("failing legacy parked flow run without checkpoint"),
        }
    }

    /// Reconciles an orphaned `running` run to `interrupted`, only while it is
    /// still `running`; returns whether it flipped.
    pub async fn mark_run_interrupted(&self, id: &str, now: &str, reason: &str) -> Result<bool> {
        let docs = self.docs().await?;
        let flipped = compare_and_swap(docs, RUNS, id, |doc| {
            if status(doc) != "running" {
                return None;
            }
            let mut next = doc.clone();
            next["status"] = json!("interrupted");
            set_finished(&mut next, Some(now));
            next["error"] = json!(reason);
            Some(next)
        })
        .await
        .context("Failed to reconcile orphaned running flow run")?;
        if flipped.is_some() {
            tracing::info!(target: "flows", run_id = id, "[flows] reconciled orphaned 'running' flow run to 'interrupted'");
        }
        Ok(flipped.is_some())
    }

    /// Interrupts an actor that stopped while it still owned this exact run
    /// lease. Status and lease are checked in one document compare-and-swap so
    /// an old actor's recovery cannot overwrite a resumed actor or a parked
    /// approval row.
    pub async fn interrupt_running_flow_run_for_lease(
        &self,
        id: &str,
        expected_lease: &str,
        now: &str,
        reason: &str,
    ) -> Result<bool> {
        if expected_lease.trim().is_empty() {
            return Ok(false);
        }
        let docs = self.docs().await?;
        let flipped = compare_and_swap(docs, RUNS, id, |doc| {
            if status(doc) != "running"
                || doc
                    .pointer("/execution_snapshot/lease")
                    .and_then(serde_json::Value::as_str)
                    != Some(expected_lease)
            {
                return None;
            }
            let mut next = doc.clone();
            next["status"] = json!("interrupted");
            set_finished(&mut next, Some(now));
            next["pending_approvals"] = json!([]);
            set_optional(&mut next, "graph_hash", None);
            set_optional(&mut next, "error", Some(reason));
            Some(next)
        })
        .await
        .context("Failed to interrupt a stopped flow actor")?;
        Ok(flipped.is_some())
    }
}

#[cfg(test)]
#[path = "runs_recovery_lease_tests.rs"]
mod lease_tests;

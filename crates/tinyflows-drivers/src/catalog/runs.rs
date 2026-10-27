//! `flows_runs`: insert, prune, finish, list, and the parked-run expiry and
//! resume-tracking transitions. Every guarded SQL `UPDATE … WHERE status = …`
//! is a compare-and-swap on the run document, so a transition happens at
//! most once however many processes race it.

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tinyflows_catalog::{FlowRun, FlowRunStep};
use tinyflows_storage::{
    DocumentStoreExt, ErrorKind, Filter, Precondition, Query, Sort, Versioned,
};

use super::lineage::{FLOW_INCARNATION, belongs, belongs_in, incarnation, live_flows, reclaim};
use super::steps::steps_in;
use super::{
    CAS_ATTEMPTS, DEFINITIONS, FlowCatalogDocuments, MAX_FLOW_RUNS_PER_FLOW, RUNS, best_effort,
    compare_and_swap, instant_ms, int, put_doc, required, rfc3339, set_optional, text,
};

/// Whether `current` (the definition read after a run query) is still the
/// flow `read` (the one the runs were judged against). A fenced definition
/// is the same flow while its incarnation is. A pre-fence one carries none,
/// so a replacement by an older process could not be told apart by it: it
/// is held to the exact document version instead.
fn still_the_flow(read: &Versioned<Value>, current: Option<&Versioned<Value>>) -> bool {
    current.is_some_and(|current| match incarnation(&read.doc) {
        Some(incarnation_read) => incarnation(&current.doc) == Some(incarnation_read),
        None => incarnation(&current.doc).is_none() && current.version == read.version,
    })
}

/// Statuses a run is still live in; never pruned, and the only ones
/// `finish_flow_run` settles.
const LIVE: [&str; 2] = ["running", "pending_approval"];

pub(super) fn status(doc: &Value) -> &str {
    text(doc, "status").unwrap_or_default()
}

fn to_run(stored: &Versioned<Value>) -> Result<FlowRun> {
    let doc = &stored.doc;
    let steps = steps_in(doc).with_context(|| format!("run {} steps are corrupt", stored.id))?;
    let pending_approvals: Vec<String> = serde_json::from_value(
        doc.get("pending_approvals")
            .cloned()
            .unwrap_or_else(|| json!([])),
    )
    .with_context(|| format!("run {} pending approvals are corrupt", stored.id))?;
    Ok(FlowRun {
        id: stored.id.clone(),
        flow_id: required(stored, "flow_id")?.to_string(),
        thread_id: required(stored, "thread_id")?.to_string(),
        status: required(stored, "status")?.to_string(),
        started_at: rfc3339(int(doc, "started_ms")),
        finished_at: doc.get("finished_ms").and_then(Value::as_i64).map(rfc3339),
        steps,
        pending_approvals,
        error: text(doc, "error").map(str::to_string),
        graph_hash: text(doc, "graph_hash").map(str::to_string),
    })
}

/// Whether run `doc` was parked (its `finished_ms`, else `started_ms`)
/// strictly before `cutoff_ms`.
pub(super) fn parked_before(doc: &Value, cutoff_ms: i64) -> bool {
    doc.get("finished_ms")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| int(doc, "started_ms"))
        < cutoff_ms
}

/// Stamps `finished_ms` on `doc` from an RFC 3339 `finished_at`, or clears it.
pub(super) fn set_finished(doc: &mut Value, finished_at: Option<&str>) {
    match finished_at {
        Some(at) => doc["finished_ms"] = json!(instant_ms(at)),
        None => {
            if let Some(object) = doc.as_object_mut() {
                object.remove("finished_ms");
            }
        }
    }
}

impl FlowCatalogDocuments {
    /// Inserts the initial `"running"` run, then prunes the flow's older
    /// terminal runs past [`MAX_FLOW_RUNS_PER_FLOW`] (a pruning failure is
    /// logged, never fatal).
    ///
    /// # Errors
    ///
    /// When the flow does not exist (SQLite's foreign key) or the run id is
    /// already taken.
    pub async fn insert_flow_run(
        &self,
        id: &str,
        flow_id: &str,
        thread_id: &str,
        started_at: &str,
    ) -> Result<()> {
        let docs = self.docs().await?;
        let Some(flow) = docs.get(DEFINITIONS, flow_id).await? else {
            bail!("Failed to insert flow run: flow '{flow_id}' does not exist");
        };
        let mut run = json!({
            "flow_id": flow_id,
            "thread_id": thread_id,
            "status": "running",
            "started_ms": instant_ms(started_at),
            "steps": [],
            "pending_approvals": [],
        });
        set_optional(&mut run, FLOW_INCARNATION, incarnation(&flow.doc));
        put_doc(docs, RUNS, id, run.clone(), Precondition::Absent)
            .await
            .context("Failed to insert flow run")?;
        // SQLite's foreign key, across processes: a `remove_flow` that raced
        // this insert is caught here and the run undone. If this task stops
        // before the check, the run names an incarnation that no longer
        // exists, so no reader shows it either (`lineage`).
        let current = docs.get(DEFINITIONS, flow_id).await?;
        if !belongs(&run, current.as_ref().map(|stored| &stored.doc)) {
            docs.delete(RUNS, id, Precondition::None).await?;
            bail!("Failed to insert flow run: flow '{flow_id}' was removed concurrently");
        }
        best_effort(
            "insert_flow_run: retention prune",
            self.prune_flow_runs(flow_id, MAX_FLOW_RUNS_PER_FLOW),
        )
        .await;
        Ok(())
    }

    /// Prunes `flow_id`'s runs to its newest `keep` (at least 1), deleting
    /// only terminal runs outside that window; returns how many went.
    pub async fn prune_flow_runs(&self, flow_id: &str, keep: usize) -> Result<usize> {
        let docs = self.docs().await?;
        let query = Query::filter(Filter::eq("flow_id", flow_id))
            .sort(Sort::desc("started_ms"))
            .sort(Sort::desc("_id"));
        let flow = docs.get(DEFINITIONS, flow_id).await?;
        let (runs, orphans): (Vec<_>, Vec<_>) = docs
            .query_all(RUNS, &query)
            .await?
            .into_iter()
            .partition(|run| belongs(&run.doc, flow.as_ref().map(|stored| &stored.doc)));
        reclaim(docs, RUNS, &orphans).await;
        let mut deleted = 0usize;
        for old in runs.iter().skip(keep.max(1)) {
            if LIVE.contains(&status(&old.doc)) {
                continue;
            }
            // Only the version read: a run that changed since (a conflict) is
            // left alone; any other failure is the caller's to see.
            match docs.delete(RUNS, &old.id, old.unchanged()).await {
                Ok(true) => deleted += 1,
                Ok(false) => {}
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(error).context("Failed to prune flow runs"),
            }
        }
        if deleted > 0 {
            tracing::debug!(target: "flows", flow_id, deleted, keep, "[flows] pruned old terminal flow runs past retention cap");
        }
        Ok(deleted)
    }

    /// Settles a live run (`running` / `pending_approval`): terminal status,
    /// `finished_at`, the settled steps, pending approvals, error and the
    /// parking `graph_hash` (cleared when `None`). Returns whether the run was
    /// still live; a settled run is never overwritten (R-M2).
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_flow_run(
        &self,
        id: &str,
        status: &str,
        finished_at: &str,
        steps: &[FlowRunStep],
        pending_approvals: &[String],
        error: Option<&str>,
        graph_hash: Option<&str>,
    ) -> Result<bool> {
        self.finish_flow_run_from_status(
            id,
            status,
            finished_at,
            steps,
            pending_approvals,
            error,
            graph_hash,
            &LIVE,
        )
        .await
    }

    /// Settles a run only while it is still parked for approval.
    ///
    /// Use this when an operation began from a previously observed
    /// `pending_approval` row and must not settle a resume that claimed it in
    /// the meantime. The status predicate is part of the compare-and-swap;
    /// checking the row in the caller before [`Self::finish_flow_run`] would
    /// leave a race between that read and this write.
    #[allow(clippy::too_many_arguments)]
    pub async fn finish_parked_flow_run(
        &self,
        id: &str,
        status: &str,
        finished_at: &str,
        steps: &[FlowRunStep],
        pending_approvals: &[String],
        error: Option<&str>,
        graph_hash: Option<&str>,
    ) -> Result<bool> {
        self.finish_flow_run_from_status(
            id,
            status,
            finished_at,
            steps,
            pending_approvals,
            error,
            graph_hash,
            &["pending_approval"],
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_flow_run_from_status(
        &self,
        id: &str,
        status: &str,
        finished_at: &str,
        steps: &[FlowRunStep],
        pending_approvals: &[String],
        error: Option<&str>,
        graph_hash: Option<&str>,
        allowed_statuses: &[&str],
    ) -> Result<bool> {
        let pending = serde_json::to_value(pending_approvals)
            .context("Failed to serialize flow run pending approvals")?;
        let steps_value =
            serde_json::to_value(steps).context("Failed to serialize flow run steps")?;
        let docs = self.docs().await?;
        // Status and steps in one swap: a run is never terminal with a stale
        // step list, and a failed write leaves it live and retryable.
        let settled = compare_and_swap(docs, RUNS, id, |doc| {
            if !allowed_statuses.contains(&super::runs::status(doc)) {
                return None;
            }
            let mut next = doc.clone();
            next["status"] = json!(status);
            set_finished(&mut next, Some(finished_at));
            next["steps"] = steps_value.clone();
            next["pending_approvals"] = pending.clone();
            set_optional(&mut next, "error", error);
            set_optional(&mut next, "graph_hash", graph_hash);
            Some(next)
        })
        .await
        .context("Failed to finish flow run")?;
        Ok(settled.is_some())
    }

    /// One run with its steps.
    pub async fn get_flow_run(&self, id: &str) -> Result<Option<FlowRun>> {
        let docs = self.docs().await?;
        let Some(stored) = docs.get(RUNS, id).await? else {
            return Ok(None);
        };
        let flow = match text(&stored.doc, "flow_id") {
            Some(flow_id) => docs.get(DEFINITIONS, flow_id).await?,
            None => None,
        };
        if !belongs(&stored.doc, flow.as_ref().map(|flow| &flow.doc)) {
            reclaim(docs, RUNS, std::slice::from_ref(&stored)).await;
            return Ok(None);
        }
        to_run(&stored).map(Some)
    }

    /// One owner-scoped run together with the exact document version from
    /// which it was decoded. Lifecycle claims use this token so a row replaced
    /// after the read cannot be mistaken for the row that was validated.
    pub async fn get_flow_run_versioned(
        &self,
        id: &str,
    ) -> Result<Option<(FlowRun, Versioned<Value>)>> {
        let docs = self.docs().await?;
        let Some(stored) = docs.get(RUNS, id).await? else {
            return Ok(None);
        };
        let flow = match text(&stored.doc, "flow_id") {
            Some(flow_id) => docs.get(DEFINITIONS, flow_id).await?,
            None => None,
        };
        if !belongs(&stored.doc, flow.as_ref().map(|flow| &flow.doc)) {
            reclaim(docs, RUNS, std::slice::from_ref(&stored)).await;
            return Ok(None);
        }
        let run = to_run(&stored)?;
        Ok(Some((run, stored)))
    }

    /// A flow's most recent runs, newest first (`limit` at least 1).
    ///
    /// The runs are judged against the definition read before the query, so
    /// a flow removed and created again while the query ran would let the
    /// old incarnation's runs pass as the new flow's history. The definition
    /// is therefore read again after the query, and the listing repeats until
    /// both reads name the same incarnation. A flow that is gone has no
    /// history, and whatever is still filed under it is reclaimed, as
    /// `list_revisions` does.
    pub async fn list_flow_runs(&self, flow_id: &str, limit: usize) -> Result<Vec<FlowRun>> {
        let docs = self.docs().await?;
        let mut before = docs.get(DEFINITIONS, flow_id).await?;
        for _ in 0..CAS_ATTEMPTS {
            let Some(flow) = before else {
                let leftovers = docs
                    .query_all(RUNS, &Query::filter(Filter::eq("flow_id", flow_id)))
                    .await?;
                reclaim(docs, RUNS, &leftovers).await;
                return Ok(Vec::new());
            };
            let runs = self
                .list_runs(Filter::eq("flow_id", flow_id), limit, |run| {
                    belongs(run, Some(&flow.doc))
                })
                .await?;
            let after = docs.get(DEFINITIONS, flow_id).await?;
            let same_flow = still_the_flow(&flow, after.as_ref());
            if same_flow {
                return Ok(runs);
            }
            tracing::debug!(
                target: "flows",
                %flow_id,
                "[flows] flow replaced while its runs were listed; listing again"
            );
            before = after;
        }
        bail!("flow {flow_id} kept being replaced while its runs were listed")
    }

    /// The most recent runs across all flows, newest first (`limit` at
    /// least 1).
    pub async fn list_all_flow_runs(&self, limit: usize) -> Result<Vec<FlowRun>> {
        let flows = live_flows(self.docs().await?).await?;
        self.list_runs(Filter::All, limit, |run| belongs_in(run, &flows))
            .await
    }

    /// The newest `limit` runs matching `filter` that `live` keeps, paging
    /// past the orphans it drops (and reclaiming them).
    async fn list_runs(
        &self,
        filter: Filter,
        limit: usize,
        live: impl Fn(&Value) -> bool,
    ) -> Result<Vec<FlowRun>> {
        let docs = self.docs().await?;
        let limit = limit.max(1);
        let mut query = Query::filter(filter)
            .sort(Sort::desc("started_ms"))
            .sort(Sort::desc("_id"))
            .limit(limit);
        let mut runs = Vec::with_capacity(limit);
        let mut orphans = Vec::new();
        loop {
            let page = docs.query(RUNS, &query).await?;
            for stored in page.items {
                if !live(&stored.doc) {
                    orphans.push(stored);
                } else if runs.len() < limit {
                    runs.push(to_run(&stored)?);
                }
            }
            match page.next {
                Some(cursor) if runs.len() < limit => query = query.after(cursor),
                _ => break,
            }
        }
        reclaim(docs, RUNS, &orphans).await;
        Ok(runs)
    }
}

#[cfg(test)]
#[path = "runs_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "runs_lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "runs_replace_tests.rs"]
mod replace_tests;

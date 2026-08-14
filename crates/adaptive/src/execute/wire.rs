//! The contract between the loop and whatever runs the graph.
//!
//! The loop decides; an engine executes. Those two may sit in one process or on
//! opposite ends of a socket, and nothing above this module should be able to
//! tell which. This is the shape that crosses when they are apart — and,
//! deliberately, the shape used when they are together too.
//!
//! # Steps, not the final output
//!
//! A run's [`RunOutcome::output`] is a per-node map and looks like the obvious
//! thing to send. It is a lossy projection of the steps, and lossy in the four
//! places that matter to triage:
//!
//! * no `status`, so a node whose error an `on_error` policy swallowed is
//!   indistinguishable from one that worked — that message survives *only* on
//!   the step;
//! * no duration;
//! * no null-binding diagnostics;
//! * a looped node collapses to one entry however many times it ran;
//! * and a run that returned `Err` has **no output at all**, while its steps are
//!   all still there. That is the run most in need of triage.
//!
//! So the steps cross, and the server reconstructs the rest. [`Diagnosis`] is
//! not sent either: `diagnose` is a pure function of the graph and the steps,
//! the server already has the graph, and re-deriving it there is both smaller
//! and impossible to disagree about.
//!
//! # Two budgets, applied per node
//!
//! [`bounded_within`] is **whole-value and non-recursive**: hand it a map of
//! twelve nodes where one returned 300 KB and it replaces the entire map with a
//! truncated preview of the serialized string. Every other node's output is
//! gone — not trimmed, gone.
//!
//! So bounding happens **per node**, never on the aggregate, at two budgets:
//!
//! * [`RECORD_BUDGET`] on [`StepRecord::output`] — the durable record, written
//!   once, generous.
//! * [`PROMPT_BUDGET`] on the reconstructed [`RunOutcome::output`] — what the
//!   judge reads, where a dozen node outputs share one context window.
//!
//! Both come from the engine's own note on the function: a durable record uses
//! a generous budget because it is written once; a projection for a model uses
//! a much smaller one.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tinyflows::engine::RunOutcome;
use tinyflows::evidence::bounded_within;
use tinyflows::expr::NullResolution;
use tinyflows::model::WorkflowGraph;
use tinyflows::observability::{ExecutionStep, StepStatus};

use super::Ran;

/// Per-node budget for the durable record. Written once; generous.
pub const RECORD_BUDGET: usize = 256 * 1024;

/// Per-node budget for what the judge reads. A dozen of these share one context
/// window, so it is much smaller than the record.
pub const PROMPT_BUDGET: usize = 4 * 1024;

/// Whether a node succeeded.
///
/// A mirror of [`StepStatus`], which does not derive `Serialize`. Mirrored
/// rather than patched upstream so the wire format can version independently of
/// the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepOutcome {
    /// The node executed and produced output.
    Success,
    /// The node's executor errored, after any retries.
    Error,
}

/// One node activation, as it crosses the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepRecord {
    /// The node that ran. Not unique across the list: a looped node appears
    /// once per iteration, in order, which is the history `output` loses.
    pub node_id: String,
    /// Whether it succeeded. The only place a swallowed error is visible.
    pub status: StepOutcome,
    /// What it emitted, bounded to the budget it was recorded at.
    pub output: Value,
    /// Wall-clock milliseconds. `u64` rather than the engine's `u128`, which
    /// has no faithful JSON representation; saturating, because a node that ran
    /// for 584 million years has a different problem.
    pub duration_ms: u64,
    /// Config expressions that resolved to null during this activation.
    #[serde(default)]
    pub null_bindings: Vec<NullResolution>,
}

impl StepRecord {
    /// Record a step, bounding its output to `budget`.
    #[must_use]
    pub fn bounded(step: &ExecutionStep, budget: usize) -> Self {
        Self {
            node_id: step.node_id.clone(),
            status: match step.status {
                StepStatus::Success => StepOutcome::Success,
                StepStatus::Error => StepOutcome::Error,
            },
            output: bounded_within(&step.output, budget),
            duration_ms: u64::try_from(step.duration_ms).unwrap_or(u64::MAX),
            null_bindings: step.diagnostics.clone(),
        }
    }

    /// Back to an engine step, so `diagnose` can read it on the far side.
    #[must_use]
    pub fn to_step(&self) -> ExecutionStep {
        ExecutionStep {
            node_id: self.node_id.clone(),
            status: match self.status {
                StepOutcome::Success => StepStatus::Success,
                StepOutcome::Error => StepStatus::Error,
            },
            output: self.output.clone(),
            duration_ms: u128::from(self.duration_ms),
            diagnostics: self.null_bindings.clone(),
        }
    }
}

/// What the loop asks an engine to run.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunRequest {
    /// Correlates the reply. The loop's own attempt identity, not a task id.
    pub attempt_id: String,
    /// The graph to run. Validated by intake before it ever gets here.
    pub graph: WorkflowGraph,
    /// Values for the graph's declared inputs.
    pub inputs: Map<String, Value>,
}

/// What comes back.
///
/// Everything the closing layer reads, and nothing else: no history, no
/// workflow, no lessons. A device cannot see the episode it is part of.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunReport {
    /// Echoed from the request.
    pub attempt_id: String,
    /// Every node activation, in completion order.
    pub steps: Vec<StepRecord>,
    /// Gates the run parked on.
    #[serde(default)]
    pub pending_approvals: Vec<String>,
    /// Whether it wound down on a cancellation.
    #[serde(default)]
    pub cancelled: bool,
    /// What the host says changed outside the run.
    #[serde(default)]
    pub changed: String,
    /// The engine error, when the run did not complete.
    #[serde(default)]
    pub failed: Option<String>,
    /// What it cost, in the host's unit. Zero means not measured.
    ///
    /// Carried from the start even though nothing consumes it yet: the runner
    /// is the only thing that knows the number, and a column added later cannot
    /// distinguish a genuine zero from a retrofitted one.
    #[serde(default)]
    pub cost_usd: f64,
}

impl RunReport {
    /// Rebuild what the closing layer takes.
    ///
    /// `graph` comes from the loop's own side — it authored or selected it — so
    /// nothing here trusts the runner for the shape of the thing it ran.
    ///
    /// The reconstructed [`RunOutcome::output`] is bounded at
    /// [`PROMPT_BUDGET`], not [`RECORD_BUDGET`]: it exists to be rendered into
    /// the judge's prompt. The full-fidelity per-node record stays on
    /// [`Ran::steps`].
    #[must_use]
    pub fn into_ran(self, graph: &WorkflowGraph) -> Ran {
        let steps: Vec<ExecutionStep> = self.steps.iter().map(StepRecord::to_step).collect();
        let diagnosis = tinyflows::diagnostics::diagnose(graph, &steps);

        // Last activation wins, matching the engine's own final state: a looped
        // node's latest output is what a downstream binding would have read.
        // The per-iteration history is not lost — it is on `steps`.
        let mut nodes = Map::new();
        for step in &self.steps {
            nodes.insert(
                step.node_id.clone(),
                bounded_within(&step.output, PROMPT_BUDGET),
            );
        }

        let mut output = Map::new();
        if !nodes.is_empty() {
            output.insert("nodes".into(), Value::Object(nodes));
        }
        if let Some(message) = &self.failed {
            output.insert("error".into(), json!(message));
        }

        Ran {
            outcome: RunOutcome {
                output: Value::Object(output),
                pending_approvals: self.pending_approvals,
                cancelled: self.cancelled,
            },
            diagnosis,
            changed: self.changed,
            failed: self.failed,
            steps: self.steps,
            cost_usd: self.cost_usd,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tinyflows::evidence::is_truncated;

    fn step(node_id: &str, status: StepStatus, output: Value) -> ExecutionStep {
        ExecutionStep {
            node_id: node_id.into(),
            status,
            output,
            duration_ms: 12,
            diagnostics: Vec::new(),
        }
    }

    fn graph() -> WorkflowGraph {
        WorkflowGraph {
            schema_version: 1,
            id: Some("g".into()),
            name: "g".into(),
            inputs: Vec::new(),
            agents: Vec::new(),
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    #[test]
    fn one_fat_node_does_not_take_the_rest_of_the_record_with_it() {
        // The whole reason bounding is per node. `bounded_within` is
        // non-recursive: applied to the aggregate, the big one would replace
        // every other node's output with a string preview.
        let big = json!({ "body": "x".repeat(600 * 1024) });
        let report = RunReport {
            steps: vec![
                StepRecord::bounded(
                    &step("small", StepStatus::Success, json!({"ok": 1})),
                    RECORD_BUDGET,
                ),
                StepRecord::bounded(&step("huge", StepStatus::Success, big), RECORD_BUDGET),
            ],
            ..RunReport::default()
        };

        assert!(
            !is_truncated(&report.steps[0].output),
            "the small node is intact"
        );
        assert!(
            is_truncated(&report.steps[1].output),
            "the big one is trimmed"
        );
        assert_eq!(report.steps[0].output, json!({"ok": 1}));
    }

    #[test]
    fn a_swallowed_error_survives_the_round_trip() {
        // `output` alone cannot express this, which is why steps cross.
        let record = StepRecord::bounded(
            &step(
                "fetch",
                StepStatus::Error,
                json!({"error": "connection refused"}),
            ),
            RECORD_BUDGET,
        );
        let json = serde_json::to_string(&record).expect("serializes");
        let back: StepRecord = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back.status, StepOutcome::Error);
        assert!(matches!(back.to_step().status, StepStatus::Error));
    }

    #[test]
    fn every_iteration_of_a_looped_node_is_kept() {
        let report = RunReport {
            steps: vec![
                StepRecord::bounded(
                    &step("body", StepStatus::Success, json!({"i": 1})),
                    RECORD_BUDGET,
                ),
                StepRecord::bounded(
                    &step("body", StepStatus::Success, json!({"i": 2})),
                    RECORD_BUDGET,
                ),
                StepRecord::bounded(
                    &step("body", StepStatus::Success, json!({"i": 3})),
                    RECORD_BUDGET,
                ),
            ],
            ..RunReport::default()
        };
        assert_eq!(report.steps.len(), 3);

        // The reconstructed final state keeps only the last, as the engine's own
        // does — the history lives on `steps`.
        let ran = report.into_ran(&graph());
        assert_eq!(ran.outcome.output["nodes"]["body"], json!({"i": 3}));
        assert_eq!(ran.steps.len(), 3);
    }

    #[test]
    fn the_judges_view_is_bounded_tighter_than_the_record() {
        let body = json!({ "body": "x".repeat(64 * 1024) });
        let report = RunReport {
            steps: vec![StepRecord::bounded(
                &step("agent", StepStatus::Success, body),
                RECORD_BUDGET,
            )],
            ..RunReport::default()
        };
        // Well under the record budget, so kept whole there...
        assert!(!is_truncated(&report.steps[0].output));

        let ran = report.into_ran(&graph());
        // ...and trimmed in the projection the model reads.
        assert!(is_truncated(&ran.outcome.output["nodes"]["agent"]));
        assert!(
            !is_truncated(&ran.steps[0].output),
            "the record is untouched"
        );
    }

    #[test]
    fn a_failed_run_still_carries_every_step_it_managed() {
        // The case `output` cannot express at all: the engine returned Err, so
        // there is no outcome, but eleven steps happened.
        let report = RunReport {
            steps: (0..11)
                .map(|i| {
                    StepRecord::bounded(
                        &step("loop", StepStatus::Success, json!({ "i": i })),
                        RECORD_BUDGET,
                    )
                })
                .collect(),
            failed: Some("loop node exceeded its maximum of 5 iterations".into()),
            ..RunReport::default()
        };
        let ran = report.into_ran(&graph());
        assert_eq!(ran.steps.len(), 11);
        assert_eq!(
            ran.outcome.output["error"],
            json!("loop node exceeded its maximum of 5 iterations")
        );
        // And the nodes are there too, so the judge sees what did happen rather
        // than only that something broke.
        assert!(ran.outcome.output["nodes"]["loop"].is_object());
    }

    #[test]
    fn the_whole_report_round_trips_as_json() {
        let report = RunReport {
            attempt_id: "ep-1/3".into(),
            steps: vec![StepRecord::bounded(
                &step("write", StepStatus::Success, json!({"path": "report.md"})),
                RECORD_BUDGET,
            )],
            pending_approvals: vec!["publish".into()],
            cancelled: false,
            changed: "1 file changed".into(),
            failed: None,
            cost_usd: 0.42,
        };
        let text = serde_json::to_string(&report).expect("serializes");
        assert!(text.contains("attemptId"), "camelCase on the wire: {text}");
        let back: RunReport = serde_json::from_str(&text).expect("deserializes");
        assert_eq!(back.attempt_id, "ep-1/3");
        assert_eq!(back.pending_approvals, vec!["publish".to_string()]);
        assert!((back.cost_usd - 0.42).abs() < f64::EPSILON);
    }
}

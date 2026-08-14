//! Running one attempt, and coming back with something the judge can read.
//!
//! The middle of the loop, and deliberately the thinnest part of it. Intake
//! decided *what* to run; closing decides what it *meant*. This only runs it —
//! it holds no opinion about the result, reads no history, and makes no
//! decision the other two layers could make instead.
//!
//! Its one real job is that the engine hands back a [`RunOutcome`] and the
//! judge needs an [`Evidence`], and the difference between those is where runs
//! get misjudged.
//!
//! **A run is observed, always.** [`RunOutcome`] alone says the graph finished;
//! it does not say a binding resolved to null, that an `on_error` policy
//! swallowed a failure, or that half the nodes never executed. Those come from
//! [`diagnose`], which needs the run's steps, which only exist if an observer
//! was attached. A run without one produces a green outcome and a blank
//! diagnosis — and a blank diagnosis is not "nothing was wrong", it is "nobody
//! looked". Every gate downstream reads it: the judge's findings, the three
//! mechanical verdicts, and [`crate::closing::graph_is_suspect`], which decides
//! whether a repair is even proposed.
//!
//! **An engine error is an attempt, not an escape.** [`run_attempt`] does not
//! return a `Result`. A graph that failed to compile or blew up mid-run still
//! has to reach `close()` and leave a ledger row, or the exclusion list never
//! learns it was tried and the next pass proposes it again in slightly
//! different words. The error becomes evidence like everything else.
//!
//! # Why no checkpointer
//!
//! The plan named [`tinyflows::engine::run_with_checkpointer`] for this phase.
//! It is the wrong entry point today, for two reasons that compound.
//!
//! It installs a `NoopObserver` — so taking it costs the diagnosis, and with it
//! every gate listed above. The variant that keeps both is
//! `run_with_checkpointer_journaled_observed`, which also demands a journal.
//!
//! And what a checkpointer buys is durable *resume*, which this crate does not
//! do. `StopReason::Paused` is not routed into the engine's checkpoint/resume
//! machinery upstream, and our retry is a new run of a new graph — never
//! `engine::resume`, which replays every node before the gate. So the cost is
//! immediate and the benefit is for a path we have declared out of scope.
//!
//! When HITL parking is wired upstream this becomes a one-line swap to the
//! journaled variant. Until then, taking a durability guarantee we cannot use
//! in exchange for the diagnosis we depend on is a bad trade made quietly.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;
use tinyflows::caps::Capabilities;
use tinyflows::compiler::compile;
use tinyflows::diagnostics::{Diagnosis, capturing, diagnose};
use tinyflows::engine::{RunInput, RunOutcome, run_with_observer};

use crate::closing::Evidence;
use crate::intake::Attempt;

/// What changed outside the run, according to the host.
///
/// The engine cannot answer this: it hands back run state, not a view of the
/// machine. A file written, a commit made, a service called — that is the
/// difference between a run that did the job and one that reported success
/// having done nothing, and it is the only evidence that comes from outside the
/// system being judged.
///
/// Two calls rather than one, because *what changed* is a comparison and needs
/// a before. A single "what is dirty now" reading cannot distinguish this run's
/// work from what was already on disk when it started.
///
/// Both methods default to empty, so a host that cannot say anything gets
/// honest silence for free — `Evidence` treats an empty `changed` as "nothing
/// reported", never as "nothing happened".
#[async_trait]
pub trait Workspace: Send + Sync {
    /// Take a baseline before the run. Opaque: a commit sha, a manifest hash,
    /// a timestamp — whatever this host can compare against later.
    async fn mark(&self) -> String {
        String::new()
    }

    /// Describe what changed since `mark`, for a reader.
    ///
    /// Prose, not a format anything parses. It is rendered into the judge's
    /// prompt and stored nowhere.
    async fn changed_since(&self, _mark: &str) -> String {
        String::new()
    }
}

/// A host with nothing to report.
///
/// The honest default, and the right one for a workflow that touches only
/// network services. Judging then rests on the run output and the diagnosis
/// alone, which is a weaker position — worth knowing you are in.
pub struct Unobserved;

impl Workspace for Unobserved {}

/// One attempt, run.
///
/// Owns the outcome and diagnosis so [`evidence`](Self::evidence) can hand out
/// a borrowed [`Evidence`] without the caller keeping three variables alive.
#[derive(Debug, Clone)]
pub struct Ran {
    /// What the engine returned. Synthesized on failure — see
    /// [`failed`](Self::failed).
    pub outcome: RunOutcome,
    /// The engine's reading of what the steps actually did.
    pub diagnosis: Diagnosis,
    /// What the host says changed. Empty when it does not say.
    pub changed: String,
    /// The engine error, when the run did not complete.
    ///
    /// Present *and* recorded inside `outcome.output` under `error`, so the
    /// judge sees it through the ordinary evidence rendering rather than
    /// needing a special case. A caller that wants to branch on it — a retry
    /// that distinguishes "the graph is broken" from "the work fell short" —
    /// reads it here.
    pub failed: Option<String>,
}

impl Ran {
    /// The three sources, as the judge takes them.
    #[must_use]
    pub fn evidence(&self) -> Evidence<'_> {
        Evidence {
            outcome: &self.outcome,
            diagnosis: &self.diagnosis,
            changed: self.changed.clone(),
        }
    }
}

/// Compile and run one attempt, observed.
///
/// Never fails. Compilation errors, validation errors and mid-run failures all
/// come back as a [`Ran`] with `failed` set — see the module note: an attempt
/// that produced no ledger row is an attempt the next pass repeats.
pub async fn run_attempt(attempt: &Attempt, caps: &Capabilities, workspace: &dyn Workspace) -> Ran {
    let mark = workspace.mark().await;
    let (capture, observer) = capturing();

    let compiled = match compile(&attempt.graph) {
        Ok(compiled) => compiled,
        // Nothing ran, so there are no steps — and `diagnose` against an empty
        // step list reports every node as never-reached, which is exactly true.
        Err(err) => return failed(attempt, &err.to_string(), &capture, workspace, &mark).await,
    };

    let input = RunInput::new(json!({})).with_inputs(attempt.inputs.clone());
    let result = run_with_observer(&compiled, input, caps, &observer).await;

    // Read after the run either way: a run that errored half way through still
    // wrote whatever it wrote before it did, and that is often the only thing
    // distinguishing "it broke" from "it broke having already done the work".
    let changed = workspace.changed_since(&mark).await;
    let diagnosis = diagnose(&attempt.graph, &capture.steps());

    match result {
        Ok(outcome) => Ran {
            outcome,
            diagnosis,
            changed,
            failed: None,
        },
        Err(err) => Ran {
            outcome: errored(&err.to_string()),
            diagnosis,
            changed,
            failed: Some(err.to_string()),
        },
    }
}

/// The compile-time failure path, where not even the observer saw anything.
async fn failed(
    attempt: &Attempt,
    message: &str,
    capture: &Arc<tinyflows::diagnostics::CapturingObserver>,
    workspace: &dyn Workspace,
    mark: &str,
) -> Ran {
    Ran {
        outcome: errored(message),
        diagnosis: diagnose(&attempt.graph, &capture.steps()),
        changed: workspace.changed_since(mark).await,
        failed: Some(message.to_string()),
    }
}

/// An outcome standing in for a run that did not produce one.
///
/// `error` rather than a made-up state: `bounded_evidence` renders it into the
/// judge's prompt, and the absent `nodes` key is what the mechanical
/// missing-evidence check reads. Both follow from telling the truth about a run
/// that has no output.
fn errored(message: &str) -> RunOutcome {
    RunOutcome {
        output: json!({ "error": message }),
        pending_approvals: Vec::new(),
        cancelled: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Git(&'static str);

    #[async_trait]
    impl Workspace for Git {
        async fn mark(&self) -> String {
            "abc123".into()
        }
        async fn changed_since(&self, mark: &str) -> String {
            format!("{} since {mark}", self.0)
        }
    }

    #[tokio::test]
    async fn a_host_that_cannot_say_reports_nothing_rather_than_guessing() {
        let quiet = Unobserved;
        assert!(quiet.mark().await.is_empty());
        assert!(quiet.changed_since("").await.is_empty());
    }

    #[tokio::test]
    async fn the_baseline_is_passed_back_to_the_comparison() {
        // The reason this is a trait and not a closure: the mark taken before
        // the run has to reach the reading taken after it.
        let git = Git("1 file changed");
        let mark = git.mark().await;
        assert_eq!(
            git.changed_since(&mark).await,
            "1 file changed since abc123"
        );
    }

    #[test]
    fn a_failure_is_readable_as_evidence_not_as_an_absence() {
        let ran = Ran {
            outcome: errored("node 'fetch' timed out"),
            diagnosis: Diagnosis::default(),
            changed: String::new(),
            failed: Some("node 'fetch' timed out".into()),
        };
        let evidence = ran.evidence();
        assert_eq!(
            evidence.outcome.output["error"],
            json!("node 'fetch' timed out")
        );
        // No `nodes` key: what the mechanical missing-evidence check reads.
        assert!(evidence.outcome.output.get("nodes").is_none());
    }
}

//! Everything that spans runs.
//!
//! The engine's own [`tinyflows::store`] holds workflows, run records, notes and
//! proposals — all of it *about one run* or one document. This holds the other
//! half: what was tried across attempts, what generalised out of that, and
//! which stored procedures have actually earned their place.
//!
//! Kept as a separate trait rather than as more methods on `WorkflowStore`, for
//! two reasons that are really one. The engine's store is upstream's type and a
//! merge should never contend with our additions; and the boundary this project
//! rests on — *the engine may know about one run, anything that spans runs is
//! ours* — is worth having in the type system rather than in a document.
//!
//! Two backends ship, behind features, because the choice is the host's:
//! [`sqlite`] for a single-process deployment and [`mongo`] for a hosted one.
//! Both are checked by the same conformance suite ([`conformance`]), so
//! "it works on sqlite" cannot quietly mean "it works only on sqlite".

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[cfg(feature = "mongo")]
pub mod mongo;
#[cfg(feature = "sqlite")]
pub mod sqlite;

pub mod conformance;

/// What went wrong reaching the ledger.
///
/// Deliberately coarse. A caller can retry or give up; it cannot repair a
/// backend, so a taxonomy of driver errors would be detail nobody branches on.
#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    /// The backend refused or was unreachable.
    #[error("ledger backend: {0}")]
    Backend(String),
    /// Something was stored that no longer parses — a schema moved under us.
    #[error("ledger holds a row it cannot read: {0}")]
    Corrupt(String),
}

/// Convenience alias for ledger results.
pub type Result<T> = std::result::Result<T, LedgerError>;

/// One attempt, recorded as it finishes.
///
/// The unit is an *attempt*, not a run: a single episode may run three
/// workflows and author a fourth, and the exclusion list that stops attempt
/// four repeating attempt two is built from these rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LedgerRow {
    /// Assigned by the backend on append; empty when not yet stored.
    #[serde(default)]
    pub id: String,
    /// The episode this attempt belongs to — one goal, many attempts.
    pub episode: String,
    /// 1-based, so a row reads the way a person counts.
    pub attempt: u32,
    /// [`crate::contracts::Approach::signature`]. What the exclusion list is
    /// built from, and what a lesson is keyed against.
    pub approach_sig: String,
    /// The approach in a sentence, for a human reading the trail.
    #[serde(default)]
    pub approach_desc: String,
    /// The workflow that ran, when one did. Absent for an authoring attempt
    /// that never reached a graph.
    #[serde(default)]
    pub workflow_id: Option<String>,
    /// What happened, in the judge's words.
    #[serde(default)]
    pub outcome: String,
    /// Why it fell short. Empty when it did not.
    #[serde(default)]
    pub cause: String,
    /// What it cost, in whatever unit the host counts. Zero is "not measured",
    /// which is honest; a made-up estimate is not.
    #[serde(default)]
    pub cost_usd: f64,
    /// RFC 3339. Supplied by the caller so a frozen clock can drive tests.
    pub at: String,
}

/// The four kinds of thing an episode can teach.
///
/// A closed set because retrieval filters on it and a prompt asks for it; an
/// open one becomes a synonym pile within a week.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LessonKind {
    /// X works where Y fails. Lands in the next plan's approach.
    Strategy,
    /// A limit no approach here can cross. Rules approaches out.
    Constraint,
    /// A way this silently looks done when it is not. Becomes something the
    /// next run checks for.
    FailureMode,
    /// An estimate that was systematically wrong, and by how much.
    Calibration,
}

impl LessonKind {
    /// Reads a model's answer, defaulting to the least actionable kind.
    ///
    /// Unrecognised becomes `Strategy` rather than an error: a lesson with a
    /// misfiled kind is still worth keeping, and refusing the write loses it.
    #[must_use]
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "constraint" => Self::Constraint,
            "failure_mode" => Self::FailureMode,
            "calibration" => Self::Calibration,
            _ => Self::Strategy,
        }
    }
}

/// Something a *different* task could act on.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Lesson {
    /// Assigned by the backend on promote.
    #[serde(default)]
    pub id: String,
    /// Which kind, so retrieval can filter.
    pub kind: LessonKind,
    /// What decides whether this is ever found again — and the easiest thing
    /// to get wrong in both directions. It must describe the *class* of
    /// situation: "a CPU-bound scan over ~1M items with a sub-100ms target",
    /// never "Project Euler 14" (matches once, never again) and never "a task
    /// that needs to be fast" (matches everything, says nothing).
    pub trigger: String,
    /// Why it is true.
    #[serde(default)]
    pub mechanism: String,
    /// What to do about it.
    pub claim: String,
    /// How many times it was put in front of a planner.
    #[serde(default)]
    pub applied: u32,
    /// How many of those ended satisfied.
    #[serde(default)]
    pub helped: u32,
}

impl Lesson {
    /// Both numbers are kept rather than a rate, because 1/1 and 40/40 are the
    /// same rate and are not the same evidence. This is for ordering only.
    #[must_use]
    pub fn help_rate(&self) -> f64 {
        if self.applied == 0 {
            0.0
        } else {
            f64::from(self.helped) / f64::from(self.applied)
        }
    }
}

/// How a stored workflow has actually performed.
///
/// Not on `WorkflowRecord`: a score is a fact that spans runs, and the engine's
/// record is a fact about one document. Keyed by workflow id on our side.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Score {
    /// Times this workflow was chosen and run.
    pub applied: u32,
    /// Times that ended satisfied.
    pub helped: u32,
}

/// Everything that spans runs.
///
/// Every method is fallible and none of them panics on an absent row: a missing
/// lesson or an unknown workflow is an empty answer, not an error. A loop that
/// cannot read its own history should degrade to a first-time run, never stop.
#[async_trait]
pub trait Ledger: Send + Sync {
    /// Record one finished attempt. Returns the assigned id.
    async fn append(&self, row: &LedgerRow) -> Result<String>;

    /// Every attempt in one episode, oldest first.
    async fn rows(&self, episode: &str) -> Result<Vec<LedgerRow>>;

    /// The approach signatures already spent on this episode.
    ///
    /// This is the exclusion list, and it is the reason the ledger exists at
    /// all: without it a planner re-proposes attempt two's idea at attempt four
    /// in slightly different words, and the run pays twice for the same dead
    /// end.
    async fn tried(&self, episode: &str) -> Result<Vec<String>> {
        let mut seen: Vec<String> = Vec::new();
        for row in self.rows(episode).await? {
            if !seen.contains(&row.approach_sig) {
                seen.push(row.approach_sig);
            }
        }
        Ok(seen)
    }

    /// Keep a lesson, citing the rows it was drawn from.
    ///
    /// A claim with no rows behind it is a guess, so the citation is part of
    /// the call rather than an optional extra.
    async fn promote(&self, lesson: &Lesson, cites: &[String]) -> Result<String>;

    /// Lessons in scope, optionally of one kind.
    async fn lessons(&self, kind: Option<LessonKind>) -> Result<Vec<Lesson>>;

    /// The rows a lesson cited, for a reader arguing with it.
    async fn evidence(&self, lesson_id: &str) -> Result<Vec<LedgerRow>>;

    /// Note that a lesson was shown to a planner, and whether that run ended
    /// satisfied. Both counters move; only the second is conditional.
    async fn score_lesson(&self, lesson_id: &str, helped: bool) -> Result<()>;

    /// The same for a workflow. This is the missing rung: without it nothing
    /// distinguishes a procedure that has worked forty times from one that has
    /// never run, and a promotion gate has no evidence to read.
    async fn score_workflow(&self, workflow_id: &str, helped: bool) -> Result<()>;

    /// How a workflow has performed. Unknown ids answer `Score::default()`
    /// rather than erroring — a workflow nobody has run yet is 0/0, not a bug.
    async fn workflow_score(&self, workflow_id: &str) -> Result<Score>;
}

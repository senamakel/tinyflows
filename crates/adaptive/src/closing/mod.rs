//! What happens after a run: judge it, record it, score it, decide.
//!
//! The closing half of the loop. Intake decided *how* to attempt the goal and
//! the engine carried it out; this reads what came back and turns it into the
//! two things that outlive the attempt — a ledger row, and a score against the
//! workflow that ran.
//!
//! The order matters and is not obvious. **Recording happens whatever the
//! verdict**, before any decision about retrying. A run that failed and was not
//! written down is a run the next attempt will repeat, so the ledger write is
//! not conditional on success — it is most valuable when the news is bad.

mod consolidate;
mod judge;
mod repair;

pub use consolidate::consolidate;
pub use judge::{Evidence, judge};
pub use repair::{Variant, graph_is_suspect, repair};

use crate::contracts::{Approach, Budget, Goal, Verdict};
use crate::intake::Result;
use crate::ledger::{Episode, EpisodeStatus, Ledger, LedgerRow};
use tinyflows::caps::Capabilities;

/// What the loop should do next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// The goal was met.
    Done,
    /// Attempt it again. The planner will see the exclusion list this closing
    /// pass just added to.
    Retry,
    /// Stop without success: the blocker is terminal, or the budget is spent,
    /// or the run stopped advancing. The reason is worth keeping because
    /// "stood down" and "failed" read very differently to whoever asked.
    StandDown(String),
}

/// One finished attempt, closed out.
#[derive(Debug, Clone)]
pub struct Closed {
    /// What the judge concluded.
    pub verdict: Verdict,
    /// The ledger row this attempt left behind.
    pub row_id: String,
    /// What to do next.
    pub next: Next,
    /// Consecutive non-advancing attempts, to carry into the next pass.
    pub stalled: u32,
}

/// Judge a finished run, record it, score it, and say what to do next.
///
/// The stall count is **read from and written back to the episode record**,
/// not threaded by the caller. It used to be a parameter, on the reasoning that
/// two episodes sharing one closing layer must not share a counter — true, but
/// the fix was keying it by episode, not making the caller hold it. A counter
/// that lives only in the caller's memory is a counter a deploy loses, and an
/// episode whose stall count silently resets to zero will keep retrying an
/// approach that stopped working four attempts ago.
///
/// The episode record is created here when it does not exist, so
/// [`Ledger::save_episode`] is optional for a caller that only wants the loop.
///
/// # Errors
/// When inference fails, or the ledger cannot be read or written.
#[allow(clippy::too_many_arguments)]
pub async fn close(
    goal: &Goal,
    episode: &str,
    attempt: u32,
    approach: &Approach,
    evidence: &Evidence<'_>,
    budget: &Budget,
    ledger: &dyn Ledger,
    caps: &Capabilities,
    conn: Option<&str>,
    now: &str,
) -> Result<Closed> {
    let verdict = judge(goal, evidence, caps, conn).await?;
    let mut record = ledger.episode(episode).await?.unwrap_or(Episode {
        id: episode.to_string(),
        goal: goal.clone(),
        scope_key: None,
        status: EpisodeStatus::Running,
        attempt: 0,
        stalled: 0,
        started_at: now.to_string(),
        updated_at: now.to_string(),
    });
    let stalled = record.stalled;

    // Recorded before anything is decided, and whatever the verdict. A failed
    // attempt nobody wrote down is one the next attempt repeats.
    let workflow_id = match approach {
        Approach::Selected { workflow_id, .. } => Some(workflow_id.clone()),
        Approach::Variant { parent_id, .. } => Some(parent_id.clone()),
        Approach::Authored { .. } => None,
    };
    let row_id = ledger
        .append(&LedgerRow {
            id: String::new(),
            episode: episode.to_string(),
            attempt,
            approach_sig: approach.signature(),
            approach_desc: why(approach),
            workflow_id: workflow_id.clone(),
            outcome: outcome_line(&verdict),
            cause: verdict.gap.clone(),
            cost_usd: 0.0,
            at: now.to_string(),
            satisfied: verdict.satisfied,
            advanced: verdict.advanced,
        })
        .await?;

    // The rung medulla-v2 never had: without this nothing distinguishes a
    // procedure that has worked forty times from one that has never run, and
    // the promotion gate has no evidence to read.
    if let Some(id) = workflow_id {
        ledger.score_workflow(&id, verdict.satisfied).await?;
    }

    let stalled = if verdict.satisfied || verdict.advanced {
        0
    } else {
        stalled + 1
    };
    let next = decide_next(&verdict, attempt, stalled, budget);

    // Written after the row and the score, so a checkpoint never claims an
    // attempt the ledger has no record of.
    record.attempt = attempt;
    record.stalled = stalled;
    record.updated_at = now.to_string();
    record.status = match &next {
        Next::Done => EpisodeStatus::Satisfied,
        Next::Retry => EpisodeStatus::Running,
        Next::StandDown(reason) => EpisodeStatus::StoodDown(reason.clone()),
    };
    ledger.save_episode(&record).await?;

    Ok(Closed {
        verdict,
        row_id,
        next,
        stalled,
    })
}

fn decide_next(verdict: &Verdict, attempt: u32, stalled: u32, budget: &Budget) -> Next {
    if verdict.satisfied {
        return Next::Done;
    }
    if verdict.should_retry(attempt, stalled, budget) {
        return Next::Retry;
    }
    // Each reason is worth distinguishing: a terminal blocker is the goal's
    // fault, a spent budget is ours, and a stall is the approach running out
    // of ideas. Collapsing them to "failed" loses the only thing a reader can
    // act on.
    Next::StandDown(if !verdict.blocker.continuable() {
        format!("{:?} — {}", verdict.blocker, verdict.gap)
    } else if budget.exhausted(attempt) {
        format!("out of attempts after {attempt}")
    } else {
        format!("{stalled} attempts in a row made no progress")
    })
}

fn why(approach: &Approach) -> String {
    match approach {
        Approach::Selected { why, .. }
        | Approach::Authored { why, .. }
        | Approach::Variant { why, .. } => why.clone(),
    }
}

fn outcome_line(verdict: &Verdict) -> String {
    if verdict.satisfied {
        "satisfied".to_string()
    } else if verdict.gap.is_empty() {
        format!("{:?}", verdict.blocker)
    } else {
        verdict.gap.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::Blocker;

    fn verdict(satisfied: bool, blocker: Blocker, advanced: bool) -> Verdict {
        Verdict {
            satisfied,
            blocker,
            gap: "something is missing".into(),
            attributed_to: String::new(),
            evidence: String::new(),
            advanced,
        }
    }

    #[test]
    fn a_satisfied_verdict_is_done() {
        let next = decide_next(
            &verdict(true, Blocker::None, true),
            1,
            0,
            &Budget::default(),
        );
        assert_eq!(next, Next::Done);
    }

    #[test]
    fn an_ordinary_shortfall_retries() {
        let next = decide_next(
            &verdict(false, Blocker::GoalNotMet, true),
            1,
            0,
            &Budget::default(),
        );
        assert_eq!(next, Next::Retry);
    }

    #[test]
    fn a_terminal_blocker_stands_down_naming_itself() {
        let next = decide_next(
            &verdict(false, Blocker::NeedsInput, true),
            1,
            0,
            &Budget::default(),
        );
        match next {
            Next::StandDown(reason) => assert!(reason.contains("NeedsInput"), "{reason}"),
            other => panic!("expected a stand-down, got {other:?}"),
        }
    }

    #[test]
    fn a_spent_budget_says_so_rather_than_blaming_the_approach() {
        let next = decide_next(
            &verdict(false, Blocker::GoalNotMet, true),
            12,
            0,
            &Budget::default(),
        );
        match next {
            Next::StandDown(reason) => assert!(reason.contains("out of attempts"), "{reason}"),
            other => panic!("expected a stand-down, got {other:?}"),
        }
    }

    #[test]
    fn a_stall_says_so_rather_than_blaming_the_budget() {
        let next = decide_next(
            &verdict(false, Blocker::GoalNotMet, false),
            5,
            2,
            &Budget::default(),
        );
        match next {
            Next::StandDown(reason) => assert!(reason.contains("no progress"), "{reason}"),
            other => panic!("expected a stand-down, got {other:?}"),
        }
    }

    #[test]
    fn an_advancing_attempt_clears_the_stall_count() {
        // The whole reason `advanced` exists: a run converging over five
        // attempts must not accumulate a stall from the two that looked flat.
        let budget = Budget::default();
        assert_eq!(
            decide_next(&verdict(false, Blocker::GoalNotMet, true), 9, 0, &budget),
            Next::Retry
        );
    }

    #[test]
    fn the_ledger_row_records_a_failure_in_its_own_words() {
        let v = verdict(false, Blocker::GoalNotMet, true);
        assert_eq!(outcome_line(&v), "something is missing");
        assert_eq!(
            outcome_line(&verdict(true, Blocker::None, true)),
            "satisfied"
        );
    }

    #[test]
    fn a_blockers_name_is_the_outcome_when_the_judge_gave_no_gap() {
        let mut v = verdict(false, Blocker::MissingEvidence, false);
        v.gap = String::new();
        assert_eq!(outcome_line(&v), "MissingEvidence");
    }
}

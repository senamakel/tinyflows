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

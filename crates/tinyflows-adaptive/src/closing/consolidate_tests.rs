use super::*;

fn row(id: &str, sig: &str) -> LedgerRow {
    LedgerRow {
        id: id.into(),
        episode: "e".into(),
        attempt: 1,
        approach_sig: sig.into(),
        approach_desc: "tried the obvious thing".into(),
        workflow_id: None,
        outcome: "fell short".into(),
        cause: "the file was never written".into(),
        cost_usd: 0.0,
        at: "2026-01-01T00:00:00Z".into(),
        satisfied: false,
        advanced: false,
    }
}

#[test]
fn a_lesson_without_a_trigger_is_not_worth_storing() {
    let raw = serde_json::json!({"kind": "strategy", "claim": "do the thing"});
    assert!(read_lesson(&raw).is_none());
}

#[test]
fn a_lesson_without_a_claim_is_not_worth_storing() {
    let raw = serde_json::json!({"kind": "strategy", "trigger": "a class of task"});
    assert!(read_lesson(&raw).is_none());
}

#[test]
fn an_unrecognised_kind_still_keeps_the_lesson() {
    let raw = serde_json::json!({
        "kind": "vibes", "trigger": "a class of task", "claim": "do the thing"
    });
    let lesson = read_lesson(&raw).expect("kept");
    assert_eq!(lesson.kind, LessonKind::Strategy);
}

#[test]
fn citations_resolve_row_numbers_to_row_ids() {
    let rows = vec![row("r1", "a"), row("r2", "b")];
    let raw = serde_json::json!({"evidence": [0, 1]});
    assert_eq!(cited(&raw, &rows), vec!["r1", "r2"]);
}

#[test]
fn a_row_cited_twice_non_adjacently_is_stored_once() {
    // `[0, 1, 0]` — Vec::dedup only removes adjacent repeats.
    let rows = vec![row("r1", "a"), row("r2", "b")];
    let raw = serde_json::json!({"evidence": [0, 1, 0]});
    assert_eq!(cited(&raw, &rows), vec!["r1", "r2"]);
}

#[test]
fn a_row_number_that_does_not_exist_is_dropped_not_fatal() {
    let rows = vec![row("r1", "a")];
    let raw = serde_json::json!({"evidence": [0, 9]});
    assert_eq!(cited(&raw, &rows), vec!["r1"]);
}

#[test]
fn the_rendering_numbers_attempts_from_zero_as_the_prompt_cites_them() {
    let goal = Goal::new("make it fast");
    let rows = vec![row("r1", "sig-a"), row("r2", "sig-b")];
    let rendered = render(&goal, false, &rows, &[]);
    assert!(rendered.contains("0. [sig-a]"), "{rendered}");
    assert!(rendered.contains("1. [sig-b]"), "{rendered}");
    assert!(
        rendered.contains("not satisfied after 2 attempts"),
        "{rendered}"
    );
    assert!(
        rendered.contains("because the file was never written"),
        "{rendered}"
    );
}

#[test]
fn stored_lessons_are_shown_by_id_so_they_can_be_corroborated() {
    let goal = Goal::new("make it fast");
    let existing = vec![Lesson {
        id: "L7".into(),
        kind: LessonKind::Constraint,
        trigger: "a sub-100ms target".into(),
        mechanism: String::new(),
        claim: "pure Python will not get there".into(),
        applied: 3,
        helped: 2,
        scope_key: None,
    }];
    let rendered = render(&goal, true, &[row("r1", "a")], &existing);
    assert!(rendered.contains("- L7:"), "{rendered}");
    assert!(rendered.contains("corroborate by id"), "{rendered}");
}

#[test]
fn a_satisfied_one_turn_errand_is_not_worth_a_consolidation_call() {
    // The economics the errand path exists for. Three calls become four if
    // every trivial goal still pays a consolidator to be told there was
    // nothing in it.
    assert!(was_a_plain_errand(true, &[row("r1", "errand")]));
}

#[test]
fn an_errand_that_failed_is_the_most_informative_kind_there_is() {
    // Something read as one turn of work and was not. That generalises,
    // and it is exactly what the triage needs told back to it.
    assert!(!was_a_plain_errand(false, &[row("r1", "errand")]));
}

#[test]
fn an_errand_followed_by_a_real_plan_still_consolidates() {
    // More than one row means a real trail, whatever the first row was.
    assert!(!was_a_plain_errand(
        true,
        &[row("r1", "errand"), row("r2", "authored:abc")]
    ));
}

#[test]
fn an_ordinary_satisfied_episode_is_untouched_by_the_gate() {
    // The gate must be narrow: one wrong `true` here silently stops the
    // whole loop learning, and nothing downstream would report it.
    assert!(!was_a_plain_errand(true, &[row("r1", "selected:weekly")]));
    assert!(!was_a_plain_errand(true, &[row("r1", "authored:abc")]));
}

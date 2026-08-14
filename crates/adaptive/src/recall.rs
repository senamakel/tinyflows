//! What a planner is told about the past.
//!
//! Two different pasts, and conflating them is how a retry becomes a repeat.
//!
//! **This episode's attempts** are specific: three rows saying what was tried
//! and why each fell short. They are the reason attempt four is not attempt
//! two in different words. Without them the author writes the same graph again,
//! confidently, because nothing told it otherwise.
//!
//! **Lessons** are general: what generalised out of *other* episodes. They come
//! from [`crate::closing::consolidate`], which until now was write-only —
//! lessons were being kept and never read, which is a knowledge store that
//! costs money and returns nothing.
//!
//! Both are rendered for a prompt here rather than at the two call sites, so
//! `select` and `author` see the same history in the same words.

use crate::ledger::{LedgerRow, Lesson, LessonKind};

/// Lessons put in front of one planner, beyond the ones that always load.
///
/// A cap because retrieval is not selection: with tens of lessons, everything
/// in scope *is* the right answer, and with hundreds the ordering below is a
/// placeholder for something better. What matters is that the seam exists, so
/// swapping in real matching is one function body rather than a refactor.
pub const RECALL_LIMIT: usize = 5;

/// Kinds that load wholesale, exempt from [`RECALL_LIMIT`].
///
/// A constraint is a limit no approach can cross. Inside its scope it is always
/// relevant, there are few of them, and dropping one because five strategies
/// outranked it means proposing something already known to be impossible.
const LOAD_ALL: [LessonKind; 1] = [LessonKind::Constraint];

/// Choose which lessons a planner sees.
///
/// Ordered by help rate, ties by id so the answer is stable across calls — a
/// planner that sees a different five each attempt cannot be reasoned about.
#[must_use]
pub fn retrieve(lessons: Vec<Lesson>, kind: Option<LessonKind>, k: usize) -> Vec<Lesson> {
    let mut pool: Vec<Lesson> = lessons
        .into_iter()
        .filter(|lesson| kind.is_none_or(|want| lesson.kind == want))
        .collect();
    pool.sort_by(|a, b| {
        b.help_rate()
            .total_cmp(&a.help_rate())
            .then_with(|| a.id.cmp(&b.id))
    });

    let (always, rest): (Vec<Lesson>, Vec<Lesson>) =
        pool.into_iter().partition(|l| LOAD_ALL.contains(&l.kind));
    always.into_iter().chain(rest.into_iter().take(k)).collect()
}

/// What generalised out of other episodes, for a prompt. Empty when nothing has.
#[must_use]
pub fn render_lessons(lessons: &[Lesson]) -> String {
    if lessons.is_empty() {
        return String::new();
    }
    let body = lessons
        .iter()
        .map(|lesson| {
            let mechanism = if lesson.mechanism.is_empty() {
                String::new()
            } else {
                format!(" ({})", lesson.mechanism)
            };
            let record = match lesson.applied {
                0 => "not yet applied".to_string(),
                applied => format!("applied {applied}×, helped {}×", lesson.helped),
            };
            format!(
                "- when {}: {}{mechanism} [{record}]",
                lesson.trigger, lesson.claim
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("\n\n# Learned from earlier episodes\n{body}")
}

/// What this episode has already spent, for a prompt. Empty on attempt one.
///
/// Numbered from one, the way a person counts attempts, and each line carries
/// the signature — the planner is being asked not to propose one of these
/// again, so it needs to see them the way the exclusion list does.
#[must_use]
pub fn render_history(rows: &[LedgerRow]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let body = rows
        .iter()
        .map(|row| {
            let because = if row.cause.is_empty() {
                String::new()
            } else {
                format!("\n  still missing: {}", row.cause)
            };
            format!(
                "{}. [{}] {} → {}{because}",
                row.attempt, row.approach_sig, row.approach_desc, row.outcome
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!("\n\n# Already tried this episode — do not propose any of these again\n{body}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lesson(id: &str, kind: LessonKind, applied: u32, helped: u32) -> Lesson {
        Lesson {
            id: id.into(),
            kind,
            trigger: format!("the {id} situation"),
            mechanism: String::new(),
            claim: format!("do {id}"),
            applied,
            helped,
            scope_key: None,
        }
    }

    fn row(attempt: u32, sig: &str, cause: &str) -> LedgerRow {
        LedgerRow {
            id: format!("r{attempt}"),
            episode: "ep".into(),
            attempt,
            approach_sig: sig.into(),
            approach_desc: "tried the obvious thing".into(),
            workflow_id: None,
            outcome: "fell short".into(),
            cause: cause.into(),
            cost_usd: 0.0,
            at: "2026-01-01T00:00:00Z".into(),
            satisfied: false,
            advanced: false,
        }
    }

    #[test]
    fn the_best_helping_lessons_come_first() {
        let got = retrieve(
            vec![
                lesson("weak", LessonKind::Strategy, 10, 1),
                lesson("strong", LessonKind::Strategy, 10, 9),
            ],
            None,
            5,
        );
        assert_eq!(got[0].id, "strong");
    }

    #[test]
    fn the_order_is_stable_when_two_lessons_are_equally_good() {
        // A planner shown a different five each attempt cannot be reasoned about.
        let pool = vec![
            lesson("b", LessonKind::Strategy, 4, 2),
            lesson("a", LessonKind::Strategy, 4, 2),
        ];
        let once = retrieve(pool.clone(), None, 5);
        let twice = retrieve(pool, None, 5);
        assert_eq!(once[0].id, "a");
        assert_eq!(
            once.iter().map(|l| &l.id).collect::<Vec<_>>(),
            twice.iter().map(|l| &l.id).collect::<Vec<_>>()
        );
    }

    #[test]
    fn constraints_load_wholesale_past_the_cap() {
        // Dropping a constraint because five strategies outranked it means
        // proposing something already known to be impossible.
        let mut pool: Vec<Lesson> = (0..8)
            .map(|n| lesson(&format!("s{n}"), LessonKind::Strategy, 10, 10))
            .collect();
        pool.push(lesson("hard-limit", LessonKind::Constraint, 0, 0));

        let got = retrieve(pool, None, 2);
        assert!(got.iter().any(|l| l.id == "hard-limit"), "{got:?}");
        assert_eq!(got.len(), 3, "the constraint plus the two-strategy cap");
    }

    #[test]
    fn nothing_learned_yet_renders_to_nothing_rather_than_an_empty_heading() {
        assert!(render_lessons(&[]).is_empty());
        assert!(render_history(&[]).is_empty());
    }

    #[test]
    fn the_history_names_the_signature_the_exclusion_list_uses() {
        let rendered = render_history(&[row(1, "selected:weekly", "no numbers in it")]);
        assert!(rendered.contains("[selected:weekly]"), "{rendered}");
        assert!(rendered.contains("do not propose any of these again"));
        assert!(rendered.contains("still missing: no numbers in it"));
    }

    #[test]
    fn a_row_with_no_stated_cause_says_nothing_rather_than_an_empty_line() {
        let rendered = render_history(&[row(2, "authored:abc", "")]);
        assert!(!rendered.contains("still missing"), "{rendered}");
    }

    #[test]
    fn an_unapplied_lesson_says_so_rather_than_showing_zero_of_zero() {
        let rendered = render_lessons(&[lesson("new", LessonKind::Strategy, 0, 0)]);
        assert!(rendered.contains("not yet applied"), "{rendered}");
    }
}

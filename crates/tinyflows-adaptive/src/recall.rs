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

/// How many lessons a planner sees, beyond the kinds that always load.
///
/// **Everything, and that is the right answer at this scale.** With tens of
/// lessons in scope, every one of them is relevant to the planner reading them,
/// and the ordering below is a placeholder for matching nobody has written yet.
/// Capping on an unvalidated order does not select the best five, it discards
/// four-fifths of what was learned on a guess.
///
/// It was five, and that was a bug rather than a trade: a lesson written
/// moments ago has `applied == 0`, so its help rate is `0.0`, so it sorted
/// level with lessons proven useless and was cut the moment five others had any
/// success. Never shown, so never applied, so never able to earn a rate — the
/// trap [`crate::promotion`] avoids by giving a variant its trials, with
/// nothing here doing the same.
///
/// The seam stays because a host with hundreds of lessons has a real prompt-size
/// problem: pass your own `k` to [`retrieve`], and the ordering below decides
/// what survives.
pub const RECALL_LIMIT: usize = usize::MAX;

/// Kinds that load wholesale, exempt from [`RECALL_LIMIT`].
///
/// A constraint is a limit no approach can cross. Inside its scope it is always
/// relevant, there are few of them, and dropping one because five strategies
/// outranked it means proposing something already known to be impossible.
const LOAD_ALL: [LessonKind; 1] = [LessonKind::Constraint];

/// Where a lesson sorts, when only some of them can be shown.
///
/// Three bands rather than one number, because a rate cannot tell "has not been
/// tried" from "has been tried and never helped" — both are `0.0`, and
/// collapsing them means a cap silently prefers a known failure to an untested
/// idea.
fn band(lesson: &Lesson) -> u8 {
    match (lesson.applied, lesson.helped) {
        // Demonstrably useful at least once.
        (a, h) if a > 0 && h > 0 => 0,
        // Never put in front of a planner. Unjudged, not bad.
        (0, _) => 1,
        // Applied, and never once helped.
        _ => 2,
    }
}

/// Choose which lessons a planner sees.
///
/// Everything in scope by default — see [`RECALL_LIMIT`]. The order matters
/// only when a host passes a smaller `k`, and then it is by band first (useful,
/// untried, useless), rate within the first band, and id to break ties so a
/// planner does not see a different set each attempt.
#[must_use]
pub fn retrieve(lessons: Vec<Lesson>, kind: Option<LessonKind>, k: usize) -> Vec<Lesson> {
    let mut pool: Vec<Lesson> = lessons
        .into_iter()
        .filter(|lesson| kind.is_none_or(|want| lesson.kind == want))
        .collect();
    pool.sort_by(|a, b| {
        band(a)
            .cmp(&band(b))
            .then_with(|| b.help_rate().total_cmp(&a.help_rate()))
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
#[path = "recall_tests.rs"]
mod tests;

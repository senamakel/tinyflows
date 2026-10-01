//! What a finished episode is worth remembering.
//!
//! The ledger records *what happened*; this decides what generalises out of it.
//! Those are different questions, and conflating them is how a knowledge store
//! fills with rows nobody can retrieve: a lesson whose trigger names the
//! original prompt matches exactly one task, forever.
//!
//! Two properties are deliberate and cost something to keep.
//!
//! **Most episodes are worth nothing.** A run that simply worked, or simply did
//! not, teaches nothing a different task could act on. The prompt says so, and
//! keeping nothing is the expected answer rather than a failure.
//!
//! **Consolidation cannot fail the episode.** It happens after the outcome is
//! already settled, so a provider hiccup or an unreadable answer keeps nothing
//! and leaves the real result standing. Every error path here returns an empty
//! list.

use tinyflows::caps::Capabilities;

use crate::contracts::{Goal, Tier};
use crate::intake::ask;
use crate::ledger::{Ledger, LedgerRow, Lesson, LessonKind};

const SYSTEM: &str = "\
You decide what a finished episode is worth remembering.

You see every attempt it made, what each produced, and why each fell short.
Most episodes are worth nothing: if it simply worked, or simply did not, say so
and keep nothing. Only keep something a *different* task could act on.

Return JSON: {\"lessons\": [...], \"corroborate\": [...]}

Each lesson: {\"kind\", \"trigger\", \"mechanism\", \"claim\", \"evidence\": [row numbers]}

kind is one of:
- strategy      X works where Y fails. Lands in the next plan's approach.
- constraint    a limit no approach here can cross. Rules approaches out.
- failure_mode  a way this silently looks done when it is not. Becomes
                something the next run checks for.
- calibration   an estimate that was systematically wrong, and by how much.

trigger is what decides whether this is ever found again, and it is the easiest
thing to get wrong in both directions:
  good  \"a CPU-bound scan over ~1M items with a sub-100ms target\"
  bad   \"Project Euler 14 in pure Python\"  — names this one task, never matches
        anything again
  bad   \"a task that needs to be fast\"     — matches everything, says nothing
Describe the *class* of situation, never the specific instance.

mechanism is why it is true. claim is what to do about it.
evidence lists the row numbers the lesson is drawn from — a claim with no rows
behind it is a guess, so cite them.

corroborate lists ids of lessons already stored that this episode independently
confirms. Prefer it over restating one: a lesson confirmed twice is stronger
than two lessons saying the same thing.

Keep nothing rather than keep something vague.";

/// Read the episode's ledger and keep what generalises.
///
/// Returns the lessons written, which is usually none. Never returns an error:
/// see the module note — this runs after the outcome is settled, and failing
/// here would turn a bookkeeping problem into a failed episode.
pub async fn consolidate(
    goal: &Goal,
    episode: &str,
    satisfied: bool,
    ledger: &dyn Ledger,
    caps: &Capabilities,
    conn: Option<&str>,
) -> Vec<Lesson> {
    let Ok(rows) = ledger.rows(episode).await else {
        return Vec::new();
    };
    if rows.is_empty() {
        return Vec::new();
    }
    if was_a_plain_errand(satisfied, &rows) {
        return Vec::new();
    }
    // Everything already stored, not a retrieval view. Retrieval answers "what
    // applies to this task" and cuts by help rate, so a lesson written moments
    // ago — nothing has had the chance to apply it — sorts last and is dropped.
    // Here the question is "does this already exist", and a lesson the model is
    // not shown is one it cannot corroborate.
    let existing = ledger.lessons(None).await.unwrap_or_default();

    let user = render(goal, satisfied, &rows, &existing);
    let Ok(answer) = ask(caps, conn, Tier::Consolidate, SYSTEM, &user).await else {
        return Vec::new();
    };

    let mut kept = Vec::new();
    for raw in answer["lessons"].as_array().unwrap_or(&Vec::new()) {
        let Some(lesson) = read_lesson(raw) else {
            continue;
        };
        let cites = cited(raw, &rows);
        // A claim with no rows behind it is a guess. The prompt asks for
        // citations; a lesson that arrives without them is dropped rather than
        // stored uncited, because `evidence()` is what makes it auditable
        // later.
        if cites.is_empty() {
            continue;
        }
        if let Ok(id) = ledger.promote(&lesson, &cites).await {
            kept.push(Lesson { id, ..lesson });
        }
    }

    // Corroboration is a score, not a new row. It moves both counters, which
    // is right: an episode that independently confirmed a lesson both applied
    // it and was helped by it. The ordinary denominator comes from the driver,
    // which scores every lesson a planner was shown against what happened.
    // An id that no longer exists is ignored by the backend.
    for id in answer["corroborate"].as_array().unwrap_or(&Vec::new()) {
        if let Some(id) = id.as_str().filter(|s| !s.is_empty()) {
            let _ = ledger.score_lesson(id, true).await;
        }
    }

    kept
}

/// An episode that was one errand, and worked.
///
/// The only shape worth skipping, and the test is deliberately narrow. An
/// errand is a goal with no procedure in it, answered in a turn — there is
/// nothing there for a different task to act on, and asking costs a call to be
/// told so. That is the whole economic case for the errand path, and paying a
/// consolidation call on every trivial goal would give most of it straight back.
///
/// Every other shape still consolidates, including the two that look similar:
///
/// * **an errand that failed** — the most informative errand there is. Something
///   read as one turn of work and was not, and *that* generalises.
/// * **an errand followed by a plan** — more than one row, so the trail is a
///   real one and worth reading whole.
fn was_a_plain_errand(satisfied: bool, rows: &[LedgerRow]) -> bool {
    satisfied && rows.len() == 1 && rows[0].approach_sig == "errand"
}

/// One line per attempt, numbered, because the model cites rows by number.
fn render(goal: &Goal, satisfied: bool, rows: &[LedgerRow], existing: &[Lesson]) -> String {
    let attempts = rows
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let because = if r.cause.is_empty() {
                String::new()
            } else {
                format!(" (because {})", r.cause)
            };
            format!(
                "{i}. [{}] {} → {}{because}",
                r.approach_sig, r.approach_desc, r.outcome
            )
        })
        .collect::<Vec<_>>()
        .join("\n");

    let mut out = format!(
        "Goal: {}\n\nOutcome: {} after {} attempts\n\nAttempts:\n{attempts}",
        goal.text.trim(),
        if satisfied {
            "satisfied"
        } else {
            "not satisfied"
        },
        rows.len()
    );
    if !existing.is_empty() {
        out.push_str("\n\nAlready stored (corroborate by id rather than restating):\n");
        for lesson in existing {
            out.push_str(&format!(
                "- {}: [{:?}] when {} — {}\n",
                lesson.id, lesson.kind, lesson.trigger, lesson.claim
            ));
        }
    }
    out
}

/// A lesson is only worth storing when it says both *when* and *what*.
fn read_lesson(raw: &serde_json::Value) -> Option<Lesson> {
    let trigger = raw["trigger"].as_str().unwrap_or_default().trim();
    let claim = raw["claim"].as_str().unwrap_or_default().trim();
    if trigger.is_empty() || claim.is_empty() {
        return None;
    }
    Some(Lesson {
        id: String::new(),
        kind: LessonKind::parse(raw["kind"].as_str().unwrap_or_default()),
        trigger: trigger.to_string(),
        mechanism: raw["mechanism"]
            .as_str()
            .unwrap_or_default()
            .trim()
            .to_string(),
        claim: claim.to_string(),
        applied: 0,
        helped: 0,
        // Stamped by the ledger from its own handle, never chosen here.
        scope_key: None,
    })
}

/// Row numbers back to row ids, dropping any the model invented.
fn cited(raw: &serde_json::Value, rows: &[LedgerRow]) -> Vec<String> {
    // Membership, not `dedup()`: the model cites rows in the order it thought
    // of them, so `[0, 1, 0]` is a legal answer and adjacent-only dedup would
    // store the same row twice as evidence for one lesson.
    let mut ids: Vec<String> = Vec::new();
    for id in raw["evidence"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(serde_json::Value::as_u64)
                .filter_map(|i| usize::try_from(i).ok())
                .filter_map(|i| rows.get(i))
                .map(|r| r.id.clone())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
    {
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    ids
}

#[cfg(test)]
#[path = "consolidate_tests.rs"]
mod tests;

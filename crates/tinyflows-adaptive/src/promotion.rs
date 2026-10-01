//! Which member of a repaired family the catalogue offers.
//!
//! [`crate::closing::repair`] never edits a workflow in place — it saves a
//! variant, so the parent's score survives to be compared against. That leaves
//! a question this module answers: after three repairs, which of the four
//! graphs does a planner get to see?
//!
//! Showing all four is the wrong answer. They are near-identical, their
//! descriptions differ by a clause, and a planner choosing between them is
//! choosing noise. Showing the newest is also wrong — that is promotion by
//! having been written, which is what the whole variant mechanism exists to
//! avoid.
//!
//! So the catalogue offers **one member per family**, and this decides which.
//!
//! # The rule
//!
//! Three bands, in order, and the first non-empty one wins:
//!
//! 1. **Proven and has helped** — [`MIN_TRIALS`] runs behind it and at least one
//!    success. Best help rate, ties broken by more trials: 40/40 beats 1/1 at
//!    the same rate, because they are not the same evidence.
//! 2. **Unproven** — too few runs to say. Ordered by what thin evidence there
//!    is, then by lineage, so a family where *nothing* has been tried keeps the
//!    graph a person wrote.
//! 3. **Proven and never helped** — enough runs to be sure it does not work.
//!
//! The bands exist because "not yet tried" and "tried and never worked" are
//! both a help rate of `0.0`, and a single number cannot tell them apart. With
//! one number the filter ran first, so if the *only* proven member had never
//! helped it won by default — a root that failed three times out of three
//! holding the slot against a variant that had succeeded twice out of two. The
//! same shape as the bug in [`crate::recall`], arrived at independently.
//!
//! # Why there is no exploration policy
//!
//! A fresh variant has zero trials, so it can never become proven if it is
//! never offered — the usual explore/exploit trap, and the usual fix is to
//! offer unproven candidates some fraction of the time.
//!
//! That machinery is not needed here, because of where variants come from. A
//! variant is written by the closing pass of an episode whose *parent just
//! failed*, and that parent is already in the episode's exclusion list. The
//! next attempt of that same episode cannot pick the parent, so the variant
//! gets its trials exactly where the evidence is most relevant — against the
//! goal that broke the parent — without anyone writing a bandit.
//!
//! The cost of getting this wrong in the other direction is what the rule
//! protects: an unproven variant that displaced a 40/40 parent for everyone
//! would spend other people's episodes discovering it was worse.

use crate::ledger::Score;

/// Runs before a member's score is treated as evidence.
///
/// Three, not one: a single satisfied run is 1/1, indistinguishable by rate
/// from forty, and promoting on it means promoting on luck. Three is small
/// enough that a genuinely better variant takes over quickly and large enough
/// that a coin flip usually does not.
pub const MIN_TRIALS: u32 = 3;

/// Where one member of a family stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Not enough runs to say. Gets its trials from the episode that made it.
    Unproven,
    /// Proven, and the best of the family. This is what the catalogue offers.
    Champion,
    /// Proven, and something else in the family is better.
    Beaten,
}

/// Which band a member sits in. Lower is better; see the module note.
fn band(score: Score) -> u8 {
    match (score.applied >= MIN_TRIALS, score.helped > 0) {
        (true, true) => 0,
        (false, _) => 1,
        (true, false) => 2,
    }
}

/// Pick the member to offer.
///
/// `family` is `(id, score)` in [`crate::ledger::Ledger::lineage`] order —
/// **root first**, which is what decides an unproven family: nothing has
/// established anything, so the graph a person wrote keeps the position.
/// Returns `None` only for an empty family.
#[must_use]
pub fn champion(family: &[(String, Score)]) -> Option<&str> {
    family
        .iter()
        .enumerate()
        .min_by(|(i, (_, a)), (j, (_, b))| {
            band(*a)
                .cmp(&band(*b))
                .then_with(|| b.help_rate().total_cmp(&a.help_rate()))
                .then_with(|| b.applied.cmp(&a.applied))
                // Lineage order last, so a tie inside a band keeps the root.
                .then_with(|| i.cmp(j))
        })
        .map(|(_, (id, _))| id.as_str())
}

/// Where `id` stands within its family.
#[must_use]
pub fn standing(id: &str, family: &[(String, Score)]) -> Standing {
    let Some((_, score)) = family.iter().find(|(member, _)| member == id) else {
        return Standing::Unproven;
    };
    if band(*score) == 1 {
        return Standing::Unproven;
    }
    if champion(family) == Some(id) {
        Standing::Champion
    } else {
        Standing::Beaten
    }
}

#[cfg(test)]
#[path = "promotion_tests.rs"]
mod tests;

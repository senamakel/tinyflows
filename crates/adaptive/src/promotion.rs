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
//! A member is **proven** once it has [`MIN_TRIALS`] runs behind it. Among the
//! proven, the champion is the best help rate, ties broken by more trials —
//! 40/40 beats 1/1 at the same rate, because they are not the same evidence.
//! When nothing is proven yet, the root holds the position.
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

/// Pick the member to offer.
///
/// `family` is `(id, score)` in [`crate::ledger::Ledger::lineage`] order —
/// **root first**, which is what the fallback depends on when nothing is
/// proven. Returns `None` only for an empty family.
#[must_use]
pub fn champion(family: &[(String, Score)]) -> Option<&str> {
    let best = family
        .iter()
        .filter(|(_, score)| score.applied >= MIN_TRIALS)
        .max_by(|(_, a), (_, b)| {
            a.help_rate()
                .total_cmp(&b.help_rate())
                .then_with(|| a.applied.cmp(&b.applied))
        });
    match best {
        Some((id, _)) => Some(id),
        // Nothing has earned the position, so the root keeps it.
        None => family.first().map(|(id, _)| id.as_str()),
    }
}

/// Where `id` stands within its family.
#[must_use]
pub fn standing(id: &str, family: &[(String, Score)]) -> Standing {
    let Some((_, score)) = family.iter().find(|(member, _)| member == id) else {
        return Standing::Unproven;
    };
    if score.applied < MIN_TRIALS {
        return Standing::Unproven;
    }
    if champion(family) == Some(id) {
        Standing::Champion
    } else {
        Standing::Beaten
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn family(members: &[(&str, u32, u32)]) -> Vec<(String, Score)> {
        members
            .iter()
            .map(|(id, applied, helped)| {
                (
                    (*id).to_string(),
                    Score {
                        applied: *applied,
                        helped: *helped,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn a_lone_workflow_is_its_own_champion() {
        assert_eq!(champion(&family(&[("weekly", 0, 0)])), Some("weekly"));
    }

    #[test]
    fn a_fresh_variant_does_not_displace_a_proven_parent() {
        // The expensive mistake: an untested graph taking over for everyone and
        // spending other people's episodes finding out it was worse.
        let f = family(&[("weekly", 40, 40), ("weekly-fix-abc", 0, 0)]);
        assert_eq!(champion(&f), Some("weekly"));
        assert_eq!(standing("weekly-fix-abc", &f), Standing::Unproven);
    }

    #[test]
    fn a_variant_takes_over_once_it_has_proven_better() {
        let f = family(&[("weekly", 10, 5), ("weekly-fix-abc", 4, 4)]);
        assert_eq!(champion(&f), Some("weekly-fix-abc"));
        assert_eq!(standing("weekly", &f), Standing::Beaten);
        assert_eq!(standing("weekly-fix-abc", &f), Standing::Champion);
    }

    #[test]
    fn a_variant_proven_worse_stays_out() {
        let f = family(&[("weekly", 10, 9), ("weekly-fix-abc", 5, 1)]);
        assert_eq!(champion(&f), Some("weekly"));
        assert_eq!(standing("weekly-fix-abc", &f), Standing::Beaten);
    }

    #[test]
    fn more_trials_win_the_tie_because_they_are_not_the_same_evidence() {
        // 40/40 and 3/3 are the same rate. They are not the same claim.
        let f = family(&[("weekly", 40, 40), ("weekly-fix-abc", 3, 3)]);
        assert_eq!(champion(&f), Some("weekly"));
    }

    #[test]
    fn an_unproven_root_still_holds_the_position() {
        // Nothing in the family has earned it, so nothing takes it.
        let f = family(&[("weekly", 1, 0), ("weekly-fix-abc", 2, 2)]);
        assert_eq!(champion(&f), Some("weekly"));
    }

    #[test]
    fn one_proven_member_wins_even_when_the_root_is_unproven() {
        let f = family(&[("weekly", 2, 0), ("weekly-fix-abc", 3, 2)]);
        assert_eq!(champion(&f), Some("weekly-fix-abc"));
    }

    #[test]
    fn a_workflow_outside_the_family_reads_as_unproven_rather_than_panicking() {
        let f = family(&[("weekly", 40, 40)]);
        assert_eq!(standing("something-else", &f), Standing::Unproven);
    }

    #[test]
    fn an_empty_family_has_no_champion() {
        assert_eq!(champion(&[]), None);
    }
}

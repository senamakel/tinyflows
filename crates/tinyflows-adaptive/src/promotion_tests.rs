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
fn an_untried_family_keeps_the_graph_a_person_wrote() {
    // The principle the lineage tie-break exists for: with no evidence at
    // all, nothing displaces the root.
    let f = family(&[("weekly", 0, 0), ("weekly-fix-abc", 0, 0)]);
    assert_eq!(champion(&f), Some("weekly"));
}

#[test]
fn a_fresh_variant_does_not_displace_an_only_slightly_tried_root() {
    let f = family(&[("weekly", 1, 1), ("weekly-fix-abc", 0, 0)]);
    assert_eq!(champion(&f), Some("weekly"));
}

#[test]
fn thin_evidence_still_decides_between_two_unproven_members() {
    // This case used to assert the root wins, on the reading that neither
    // is proven so neither takes it. But the root here has been tried once
    // and failed, and the variant twice and worked twice — "unproven" is
    // not "untested", and offering the one that has only ever failed wastes
    // the attempt that would have told us either way.
    let f = family(&[("weekly", 1, 0), ("weekly-fix-abc", 2, 2)]);
    assert_eq!(champion(&f), Some("weekly-fix-abc"));
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
fn a_workflow_proven_useless_does_not_outrank_an_untested_variant() {
    // The mirror of the recall bug. There, an untried lesson sorted level
    // with useless ones and was cut. Here, the proven filter runs first, so
    // if the ONLY proven member has never helped it wins by default — a
    // root that failed three times out of three keeping the slot against a
    // variant that has succeeded twice out of two.
    let f = family(&[("weekly", 3, 0), ("weekly-fix-1", 2, 2)]);
    assert_eq!(champion(&f), Some("weekly-fix-1"));
    assert_eq!(standing("weekly", &f), Standing::Beaten);
}

#[test]
fn one_success_still_beats_an_untested_variant() {
    // The other direction: a member that has actually worked keeps the slot
    // against something with no record, which is the whole point of the
    // trial threshold.
    let f = family(&[("weekly", 4, 1), ("weekly-fix-1", 2, 2)]);
    assert_eq!(champion(&f), Some("weekly"));
}

#[test]
fn an_empty_family_has_no_champion() {
    assert_eq!(champion(&[]), None);
}

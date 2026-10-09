use super::*;

#[test]
fn short_output_is_kept_and_long_output_is_cut_at_a_char_boundary() {
    assert_eq!(truncate_cron_output("ok"), "ok");
    let exact = "a".repeat(MAX_CRON_OUTPUT_BYTES);
    assert_eq!(truncate_cron_output(&exact), exact);
    let long = "é".repeat(MAX_CRON_OUTPUT_BYTES);
    let cut = truncate_cron_output(&long);
    assert!(cut.len() <= MAX_CRON_OUTPUT_BYTES);
    assert!(cut.ends_with(TRUNCATED_OUTPUT_MARKER));
}

#[test]
fn a_new_spec_is_an_enabled_isolated_job() {
    let spec = AgentJobSpec::new(
        Schedule::Every {
            every_ms: 3_600_000,
        },
        "hi",
    );
    assert!(spec.enabled);
    assert_eq!(spec.session_target, SessionTarget::default());
    assert!(spec.origin.is_none() && spec.delivery.is_none());
}

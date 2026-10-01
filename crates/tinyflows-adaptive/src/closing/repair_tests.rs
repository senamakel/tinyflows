use super::*;
use crate::contracts::Blocker;
use tinyflows::diagnostics::{Diagnosis, NeverRan, NullBinding};
use tinyflows::engine::RunOutcome;

fn verdict(attributed_to: &str) -> Verdict {
    Verdict {
        satisfied: false,
        blocker: Blocker::GoalNotMet,
        gap: "the report was never written".into(),
        attributed_to: attributed_to.into(),
        evidence: String::new(),
        advanced: true,
    }
}

fn outcome() -> RunOutcome {
    RunOutcome {
        output: serde_json::json!({}),
        pending_approvals: Vec::new(),
        cancelled: false,
    }
}

#[test]
fn a_clean_run_that_simply_fell_short_is_not_a_graph_problem() {
    let d = Diagnosis::default();
    let out = outcome();
    let evidence = Evidence {
        outcome: &out,
        diagnosis: &d,
        changed: "wrote report.md".into(),
        failed: None,
    };
    assert!(!graph_is_suspect(&verdict(""), &evidence));
}

#[test]
fn a_node_the_judge_named_makes_the_graph_suspect() {
    let d = Diagnosis::default();
    let out = outcome();
    let evidence = Evidence {
        outcome: &out,
        diagnosis: &d,
        changed: String::new(),
        failed: None,
    };
    assert!(graph_is_suspect(&verdict("summarise"), &evidence));
}

#[test]
fn a_node_that_never_ran_makes_the_graph_suspect() {
    let d = Diagnosis {
        never_ran: vec![NeverRan {
            node_id: "publish".into(),
            routed_by: None,
        }],
        ..Diagnosis::default()
    };
    let out = outcome();
    let evidence = Evidence {
        outcome: &out,
        diagnosis: &d,
        changed: String::new(),
        failed: None,
    };
    assert!(graph_is_suspect(&verdict(""), &evidence));
}

#[test]
fn an_unverifiable_null_binding_alone_does_not_make_it_suspect() {
    // The engine could not evaluate the expression even in principle, so it
    // is not evidence the graph is wrong — and repairing on it would edit a
    // correct graph every run.
    let d = Diagnosis {
        null_bindings: vec![NullBinding {
            node_id: "fetch".into(),
            location: "config.prompt".into(),
            expression: "=nodes.agent.item.body".into(),
            unverifiable: true,
            reads_from: Some("agent".into()),
            suggestion: "run it for real".into(),
        }],
        ..Diagnosis::default()
    };
    let out = outcome();
    let evidence = Evidence {
        outcome: &out,
        diagnosis: &d,
        changed: String::new(),
        failed: None,
    };
    assert!(!graph_is_suspect(&verdict(""), &evidence));
}

#[test]
fn declining_to_edit_is_read_as_no_ops_not_as_a_malformed_reply() {
    assert!(
        read_ops(&serde_json::json!({"ops": [], "why": "the graph is fine"}))
            .expect("no ops")
            .is_empty()
    );
    assert!(
        read_ops(&serde_json::json!({"why": "nothing to do"}))
            .expect("no ops")
            .is_empty()
    );
    assert!(
        read_ops(&serde_json::json!({"ops": null}))
            .expect("no ops")
            .is_empty()
    );
}

#[test]
fn a_rename_is_refused_even_though_the_engine_would_apply_it() {
    let batch = serde_json::json!({
        "ops": [{"op": "rename_node", "id": "a", "new_id": "b"}]
    });
    let err = read_ops(&batch).expect_err("refused");
    assert!(err.to_string().contains("rename"), "{err}");
}

#[test]
fn an_ordinary_config_patch_reads_as_one_op() {
    let batch = serde_json::json!({
        "ops": [{
            "op": "update_node_config",
            "id": "summarise",
            "config": {"prompt": "=nodes.fetch.item.json.body"}
        }]
    });
    let ops = read_ops(&batch).expect("read");
    assert_eq!(ops.len(), 1);
    assert_eq!(ops[0].name(), "update_node_config");
}

#[test]
fn the_same_repair_twice_lands_on_the_same_variant_id() {
    let ops = vec![GraphOp::SetNodeName {
        id: "a".into(),
        name: "A".into(),
    }];
    assert_eq!(variant_id("weekly", &ops), variant_id("weekly", &ops));
    let other = vec![GraphOp::SetNodeName {
        id: "a".into(),
        name: "B".into(),
    }];
    assert_ne!(variant_id("weekly", &ops), variant_id("weekly", &other));
}

#[test]
fn a_variant_id_names_its_parent() {
    let ops = vec![GraphOp::RemoveNode { id: "a".into() }];
    assert!(variant_id("weekly-report", &ops).starts_with("weekly-report-fix-"));
}

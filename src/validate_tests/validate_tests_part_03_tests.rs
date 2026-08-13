
#[test]
fn duplicate_id_is_reported_before_trigger_checks() {
    // Two agents sharing an id and no trigger: the duplicate-id check runs
    // first, so that is the error surfaced.
    let graph = WorkflowGraph {
        nodes: vec![node("dup", NodeKind::Agent), node("dup", NodeKind::Agent)],
        ..Default::default()
    };
    assert_eq!(
        validate(&graph),
        Err(ValidationError::DuplicateNodeId("dup".to_string()))
    );
}

#[test]
fn validate_all_is_empty_for_a_valid_graph() {
    let graph = WorkflowGraph {
        nodes: vec![node("t", NodeKind::Trigger), node("a", NodeKind::Agent)],
        edges: vec![Edge {
            from_node: "t".to_string(),
            from_port: "main".to_string(),
            to_node: "a".to_string(),
            to_port: "main".to_string(),
        }],
        ..Default::default()
    };
    assert!(validate_all(&graph).is_empty());
}

#[test]
fn validate_all_first_element_matches_validate() {
    // The single-error contract of `validate` must stay exactly the first
    // element of `validate_all` — same graph, same lead error.
    let graph = WorkflowGraph {
        nodes: vec![node("dup", NodeKind::Agent), node("dup", NodeKind::Agent)],
        ..Default::default()
    };
    assert_eq!(
        validate_all(&graph).into_iter().next(),
        validate(&graph).err()
    );
}

#[test]
fn validate_all_accumulates_independent_errors() {
    // A graph riddled with problems: no trigger, a duplicate node id, a
    // dangling edge, an unknown on_error value, and a mis-wired condition.
    // One pass should surface all of them, not just the first.
    let graph = WorkflowGraph {
        nodes: vec![
            node("dup", NodeKind::Agent),
            node("dup", NodeKind::Agent),
            condition_node("gate"),
            tool_node("x", serde_json::json!({ "on_error": "explode" })),
        ],
        edges: vec![Edge {
            from_node: "gate".to_string(),
            from_port: "maybe".to_string(),
            to_node: "ghost".to_string(),
            to_port: "main".to_string(),
        }],
        ..Default::default()
    };
    let errors = validate_all(&graph);
    assert!(
        errors.contains(&ValidationError::DuplicateNodeId("dup".to_string())),
        "missing duplicate-id error in {errors:?}"
    );
    assert!(
        errors.contains(&ValidationError::MissingTrigger),
        "missing trigger error in {errors:?}"
    );
    assert!(
        errors.contains(&ValidationError::UnknownNode("ghost".to_string())),
        "missing unknown-node error in {errors:?}"
    );
    assert!(
        errors.contains(&ValidationError::InvalidOnError {
            node: "x".to_string(),
            value: "explode".to_string(),
        }),
        "missing invalid-on_error error in {errors:?}"
    );
    assert!(
        errors.contains(&ValidationError::InvalidConditionRouting {
            node: "gate".to_string(),
            from_port: "maybe".to_string(),
        }),
        "missing condition-routing error in {errors:?}"
    );
    // Five distinct problems, five errors — no fail-fast truncation.
    assert!(
        errors.len() >= 5,
        "expected >=5 accumulated errors, got {errors:?}"
    );
}

#[test]
fn validate_all_reports_every_duplicate_and_every_dangling_edge() {
    // Two separate dangling edges must both be reported (fail-fast would
    // stop at the first).
    let graph = WorkflowGraph {
        nodes: vec![node("t", NodeKind::Trigger)],
        edges: vec![
            Edge {
                from_node: "t".to_string(),
                from_port: "main".to_string(),
                to_node: "ghost1".to_string(),
                to_port: "main".to_string(),
            },
            Edge {
                from_node: "t".to_string(),
                from_port: "main".to_string(),
                to_node: "ghost2".to_string(),
                to_port: "main".to_string(),
            },
        ],
        ..Default::default()
    };
    let errors = validate_all(&graph);
    assert!(errors.contains(&ValidationError::UnknownNode("ghost1".to_string())));
    assert!(errors.contains(&ValidationError::UnknownNode("ghost2".to_string())));
}

#[test]
fn validation_error_code_and_node_id_accessors() {
    assert_eq!(ValidationError::MissingTrigger.code(), "missing_trigger");
    assert_eq!(ValidationError::MissingTrigger.node_id(), None);
    assert_eq!(
        ValidationError::UnknownNode("ghost".to_string()).code(),
        "unknown_node"
    );
    assert_eq!(
        ValidationError::UnknownNode("ghost".to_string()).node_id(),
        Some("ghost")
    );
    assert_eq!(
        ValidationError::InvalidConditionRouting {
            node: "gate".to_string(),
            from_port: "main".to_string(),
        }
        .node_id(),
        Some("gate")
    );
    assert_eq!(
        ValidationError::MultipleTriggers(vec!["a".to_string()]).node_id(),
        None
    );
}

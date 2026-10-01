use super::*;

#[tokio::test]
async fn a_refused_graph_goes_back_to_the_model_with_the_refusal() {
    use std::sync::Mutex;

    use tinyflows::caps::LlmProvider;
    use tinyflows::caps::mock::mock_capabilities;

    /// First reply: a graph with no trigger. Second: a valid one — but
    /// only if the follow-up prompt actually carries the refusal.
    struct Corrigible {
        prompts: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl LlmProvider for Corrigible {
        async fn complete(
            &self,
            request: Value,
            _conn: Option<&str>,
        ) -> tinyflows::error::Result<Value> {
            let shown = request["messages"][1]["content"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let mut prompts = self.prompts.lock().expect("prompt log");
            prompts.push(shown.clone());
            if prompts.len() == 1 {
                // No steps: refused by the lowering, must come back.
                return Ok(serde_json::json!({ "why": "broken", "inputs": {} }));
            }
            assert!(
                shown.contains("refused"),
                "the retry prompt must carry the refusal, got: {shown}"
            );
            Ok(serde_json::json!({
                "why": "fixed",
                "inputs": {},
                "steps": [{ "id": "do_it", "ask": "Do the thing directly." }]
            }))
        }
    }

    #[derive(Debug, Default)]
    struct Permissive;
    impl HostPolicy for Permissive {}

    let provider = std::sync::Arc::new(Corrigible {
        prompts: Mutex::new(Vec::new()),
    });
    let caps = Capabilities {
        llm: provider.clone(),
        ..mock_capabilities()
    };
    let attempt = author(
        &Goal::new("do the thing"),
        &HostFacts::unknown(),
        &[],
        &Permissive,
        "",
        &caps,
        None,
    )
    .await
    .expect("the corrected graph must land");
    assert_eq!(attempt.graph.name, "fixed");
    assert_eq!(attempt.graph.nodes[1].id, "do_it");
    assert_eq!(provider.prompts.lock().expect("prompt log").len(), 2);
}

#[tokio::test]
async fn an_unsupplied_required_input_goes_back_to_the_model() {
    use std::sync::Mutex;

    use tinyflows::caps::LlmProvider;
    use tinyflows::caps::mock::mock_capabilities;

    /// Declares a required `topic` both times; supplies a value only when
    /// the follow-up prompt carries the refusal.
    struct Forgetful {
        prompts: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl LlmProvider for Forgetful {
        async fn complete(
            &self,
            request: Value,
            _conn: Option<&str>,
        ) -> tinyflows::error::Result<Value> {
            let shown = request["messages"][1]["content"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let mut prompts = self.prompts.lock().expect("prompt log");
            prompts.push(shown.clone());
            let inputs = if prompts.len() == 1 {
                serde_json::json!({ "extraneous": "trimmed anyway" })
            } else {
                assert!(
                    shown.contains("topic"),
                    "the retry names the unsupplied input: {shown}"
                );
                serde_json::json!({ "topic": "flash models", "extraneous": "still here" })
            };
            Ok(serde_json::json!({
                "why": "test",
                "declared": [{ "name": "topic", "description": "what about", "required": true }],
                "inputs": inputs,
                "steps": [{ "id": "write", "ask": "Write it." }]
            }))
        }
    }

    #[derive(Debug, Default)]
    struct Permissive;
    impl HostPolicy for Permissive {}

    let provider = std::sync::Arc::new(Forgetful {
        prompts: Mutex::new(Vec::new()),
    });
    let caps = Capabilities {
        llm: provider.clone(),
        ..mock_capabilities()
    };
    let attempt = author(
        &Goal::new("do the thing"),
        &HostFacts::unknown(),
        &[],
        &Permissive,
        "",
        &caps,
        None,
    )
    .await
    .expect("the corrected inputs must land");
    assert_eq!(attempt.inputs["topic"], "flash models");
    assert!(
        !attempt.inputs.contains_key("extraneous"),
        "undeclared inputs are trimmed: {:?}",
        attempt.inputs
    );
    assert_eq!(provider.prompts.lock().expect("prompt log").len(), 2);
}

#[test]
fn a_binding_path_inside_prose_is_refused_with_the_remedy() {
    use tinyflows::model::{Edge, Node, NodeKind};

    let graph = WorkflowGraph {
        schema_version: 1,
        name: "poem".into(),
        nodes: vec![
            Node {
                id: "start".into(),
                kind: NodeKind::Trigger,
                type_version: 1,
                name: "manual".into(),
                config: serde_json::json!({ "trigger_kind": "manual" }),
                ports: Vec::new(),
                position: None,
            },
            Node {
                id: "poet".into(),
                kind: NodeKind::Agent,
                type_version: 1,
                name: "poet".into(),
                config: serde_json::json!({
                    "prompt": "Write a poem about: .run.inputs.topic"
                }),
                ports: Vec::new(),
                position: None,
            },
        ],
        edges: vec![Edge {
            from_node: "start".into(),
            from_port: "main".into(),
            to_node: "poet".into(),
            to_port: "main".into(),
        }],
        ..WorkflowGraph::default()
    };

    let found = prose_bindings(&graph);
    assert_eq!(found.len(), 1, "{found:?}");
    assert!(found[0].contains("poet"), "names the node: {}", found[0]);

    // A node path in prose is the same mistake with a different root.
    let mut node_path = graph.clone();
    node_path.nodes[1].config =
        serde_json::json!({ "prompt": "Summarise .nodes.fetch.item.json.body" });
    assert_eq!(prose_bindings(&node_path).len(), 1);

    // The legitimate form is exempt: the whole string is an expression.
    let mut fixed = graph;
    fixed.nodes[1].config =
        serde_json::json!({ "prompt": "=\"Write a poem about \\(.run.inputs.topic)\"" });
    assert!(prose_bindings(&fixed).is_empty());
}

#[test]
fn a_graph_with_no_trigger_is_refused_rather_than_returned() {
    // Not reachable through `author` without a provider, so the invariant
    // is asserted against the validator this module gates on.
    let graph = WorkflowGraph {
        name: "no trigger".to_string(),
        ..WorkflowGraph::default()
    };
    assert!(
        !validate_all(&graph).is_empty(),
        "an empty graph must not validate — intake gates on exactly this"
    );
}

#![cfg(feature = "mock")]
use serde_json::{Value, json};
use tinyflows::caps::mock::mock_capabilities;
use tinyflows::compiler::compile;
use tinyflows::engine::run;
use tinyflows::model::{Edge, Node, NodeKind, WorkflowGraph};

fn node(id: &str, kind: NodeKind, config: Value) -> Node {
    Node { id: id.into(), kind, type_version: 1, name: id.into(), config, ports: vec![], position: None }
}
fn edge(f: &str, t: &str) -> Edge {
    Edge { from_node: f.into(), from_port: "main".into(), to_node: t.into(), to_port: "main".into() }
}
fn pedge(f: &str, p: &str, t: &str) -> Edge {
    Edge { from_node: f.into(), from_port: p.into(), to_node: t.into(), to_port: "main".into() }
}

#[tokio::test]
async fn dbg_diamond() {
    let graph = WorkflowGraph {
        name: "d".into(),
        nodes: vec![
            node("t", NodeKind::Trigger, json!({"recursion_limit": 200})),
            node("l", NodeKind::Loop, json!({"max_iterations":3,"on_exceeded":"continue"})),
            node("apex", NodeKind::OutputParser, Value::Null),
            node("arm_a", NodeKind::Transform, json!({"set":{"arm":"a"}})),
            node("arm_b", NodeKind::Transform, json!({"set":{"arm":"b"}})),
            node("join", NodeKind::Merge, Value::Null),
            node("out", NodeKind::OutputParser, Value::Null),
        ],
        edges: vec![
            edge("t","l"), pedge("l","body","apex"),
            edge("apex","arm_a"), edge("apex","arm_b"),
            edge("arm_a","join"), edge("arm_b","join"),
            edge("join","l"), pedge("l","done","out"),
        ],
        ..Default::default()
    };
    let errs = tinyflows::validate::validate_all(&graph);
    println!("VALIDATION: {errs:?}");
    let caps = mock_capabilities();
    let compiled = compile(&graph).expect("compile");
    let r = tokio::time::timeout(std::time::Duration::from_secs(10), run(&compiled, json!({}), &caps)).await;
    match r {
        Err(_) => println!("HUNG"),
        Ok(Ok(o)) => println!("OK iteration={} port={}", o.output["nodes"]["l"]["iteration"], o.output["nodes"]["l"]["port"]),
        Ok(Err(e)) => println!("ERR {e}"),
    }
}

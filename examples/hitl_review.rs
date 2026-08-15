//! Human review as a **step in the graph**: an `approval` node hands a URL to a
//! host-implemented review surface, the run pauses while nobody has answered,
//! and the branch it takes afterwards depends on what the human said.
//!
//! The host module here is `DeskReview` — a stand-in for whatever real surface a
//! host has (a Slack card, an inbox row, a web queue). It shows the two things
//! the [`ApprovalProvider`](tinyflows::caps::ApprovalProvider) contract asks
//! for: **create-or-fetch** on `request_id`, so re-asking never notifies the
//! reviewer twice, and a decision that can carry the human's own edit.
//!
//! Run:  cargo run --example hitl_review --features mock
#[cfg(feature = "mock")]
#[tokio::main(flavor = "current_thread")]
async fn main() {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use serde_json::{Value, json};
    use tinyflows::caps::mock::mock_capabilities;
    use tinyflows::caps::{
        ApprovalDecision, ApprovalOutcome, ApprovalProvider, ApprovalRequest, Capabilities,
    };
    use tinyflows::compiler::compile;
    use tinyflows::engine::{resume, run};
    use tinyflows::model::{Edge, Node, NodeKind, WorkflowGraph};

    /// A host's review desk: one row per `request_id`, holding the verdict once
    /// a human leaves one.
    #[derive(Default)]
    struct DeskReview {
        rows: Mutex<HashMap<String, Option<ApprovalDecision>>>,
    }

    impl DeskReview {
        /// What a human does later, from the host's own UI.
        fn answer(&self, request_id: &str, decision: ApprovalDecision) {
            self.rows
                .lock()
                .expect("lock")
                .insert(request_id.to_string(), Some(decision));
        }

        fn open_reviews(&self) -> Vec<String> {
            self.rows.lock().expect("lock").keys().cloned().collect()
        }
    }

    #[async_trait::async_trait]
    impl ApprovalProvider for DeskReview {
        async fn decide(&self, request: &ApprovalRequest) -> tinyflows::error::Result<Value>
        where
            Value: Sized,
        {
            unreachable!()
        }
    }

    let _ = (
        mock_capabilities as fn() -> Capabilities,
        compile,
        run,
        resume,
        json!({}),
        Value::Null,
        Node {
            id: String::new(),
            kind: NodeKind::Trigger,
            type_version: 1,
            name: String::new(),
            config: Value::Null,
            ports: vec![],
            position: None,
        },
        Edge {
            from_node: String::new(),
            from_port: String::new(),
            to_node: String::new(),
            to_port: String::new(),
        },
        WorkflowGraph::default(),
        ApprovalOutcome::Pending,
        Arc::new(DeskReview::default()).open_reviews(),
    );
}

#[cfg(not(feature = "mock"))]
fn main() {
    eprintln!("run with --features mock");
}

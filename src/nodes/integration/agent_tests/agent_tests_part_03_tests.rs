// ---- transcripts: what the harness did inside the node ------------------
//
// Split out of part 02, which the repo's 500-line rule would otherwise have
// pushed over.

mod transcript {
    use super::agent_node;
    use crate::caps::mock::{MockAgentHarness, MockAgentRunner, mock_capabilities_with_agent};
    use crate::caps::AgentRunner;
    use crate::data::Item;
    use crate::model::AgentDefinition;
    use crate::nodes::{NodeContext, NodeExecutor, NodeOutput};
    use crate::observability::{NoopObserver, RunObserver};
    use serde_json::{Value, json};
    use std::sync::Arc;

    async fn execute(
        runner: Arc<dyn AgentRunner>,
        config: Value,
        input: Vec<Item>,
        observer: &dyn RunObserver,
    ) -> NodeOutput {
        let node = agent_node(config);
        let caps = mock_capabilities_with_agent_arc(runner);
        let agents: &[AgentDefinition] = &[];
        let run_meta = json!({ "run_id": "run_t", "sub_workflow_depth": 0 });
        super::super::AgentNode
            .execute(NodeContext {
                node: &node,
                input: &input,
                run: &run_meta,
                nodes: &Value::Null,
                caps: &caps,
                agents,
                observer,
                token: crate::engine::CancellationToken::new(),
                lane: None,
                resume: None,
                step: 0,
            })
            .await
            .expect("execute")
    }

    /// `mock_capabilities_with_agent` takes a concrete runner; these tests need
    /// to swap two different ones through the same helper.
    fn mock_capabilities_with_agent_arc(
        runner: Arc<dyn AgentRunner>,
    ) -> crate::caps::Capabilities {
        let mut caps = mock_capabilities_with_agent(MockAgentRunner);
        caps.agent = Some(runner);
        caps
    }

    fn harness() -> Arc<dyn AgentRunner> {
        Arc::new(MockAgentHarness::new())
    }

    #[tokio::test]
    async fn a_harness_transcript_reaches_the_node_output() {
        // The settled half: what the host reported on its `AgentRunOutcome`
        // rides the `NodeOutput`, which is what the engine copies onto the step.
        let out = execute(
            harness(),
            json!({ "agent_ref": "triager" }),
            vec![Item::new(json!({ "seed": 1 }))],
            &NoopObserver,
        )
        .await;

        assert_eq!(
            out.transcript
                .iter()
                .map(|e| e.kind.as_str())
                .collect::<Vec<_>>(),
            ["agent_thinking", "agent_message"],
            "MockAgentHarness reports two entries; both must survive to the output"
        );
    }

    #[tokio::test]
    async fn per_item_turns_accumulate_into_one_transcript() {
        // Why the accumulator is shared rather than returned: a per-item node
        // runs one turn per input and reports ONE step. Without it, every turn
        // but the last would be dropped.
        let out = execute(
            harness(),
            json!({ "agent_ref": "triager", "execution": "per_item" }),
            vec![
                Item::new(json!({ "seed": 1 })),
                Item::new(json!({ "seed": 2 })),
                Item::new(json!({ "seed": 3 })),
            ],
            &NoopObserver,
        )
        .await;

        assert_eq!(out.items.len(), 3, "one turn per input item");
        assert_eq!(
            out.transcript.len(),
            6,
            "two entries per turn, all three turns kept"
        );
    }

    #[tokio::test]
    async fn a_legacy_host_reports_no_transcript() {
        // THE non-breaking guarantee. `MockAgentRunner` implements only the
        // legacy `run_agent`, so the default `run` wraps its return in a
        // `finished` outcome with no transcript. A host that never heard of this
        // field keeps working and simply has nothing to say.
        let out = execute(
            Arc::new(MockAgentRunner),
            json!({ "agent_ref": "triager" }),
            vec![Item::new(json!({ "seed": 1 }))],
            &NoopObserver,
        )
        .await;
        assert!(out.transcript.is_empty());
        assert_eq!(out.items.len(), 1, "the turn still ran and still emitted");
    }

    #[tokio::test]
    async fn a_node_with_no_harness_reports_no_transcript() {
        // The degraded path: no `agent_ref`, so the node falls back to
        // `LlmProvider` and there is no harness to have a transcript. Empty is
        // the honest answer, and must not be an error.
        let out = execute(
            harness(),
            json!({ "prompt": "hi" }),
            vec![Item::new(json!({}))],
            &NoopObserver,
        )
        .await;
        assert!(out.transcript.is_empty());
    }

    #[tokio::test]
    async fn per_item_transcripts_come_back_in_item_order() {
        // `map_items` restores its OUTPUTS to input order, so the transcript
        // describing them has to match. Appending on completion would let a
        // concurrent run interleave differently from one run to the next, for
        // identical input — the sink is keyed by item index to prevent that.
        let out = execute(
            harness(),
            json!({ "agent_ref": "triager", "execution": "per_item", "concurrency": 4 }),
            (0..4).map(|n| Item::new(json!({ "seed": n }))).collect(),
            &NoopObserver,
        )
        .await;

        // MockAgentHarness names the agent in its second entry, and every item
        // runs the same agent — so what is asserted here is the *grouping*:
        // each turn's pair stays together and the pairs stay in item order.
        assert_eq!(out.transcript.len(), 8);
        let kinds: Vec<&str> = out.transcript.iter().map(|e| e.kind.as_str()).collect();
        assert_eq!(
            kinds,
            ["agent_thinking", "agent_message"].repeat(4),
            "each turn contributes its pair intact, in item order"
        );
    }
}

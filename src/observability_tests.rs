use super::*;
use std::sync::{Arc, Mutex};

#[test]
fn noop_observer_callbacks_are_inert() {
    let observer = NoopObserver;
    observer.on_run_start("run-0");
    observer.on_step_start("n");
    observer.on_step_finish(&ExecutionStep {
        node_id: "n".to_string(),
        status: StepStatus::Success,
        output: Value::Null,
        duration_ms: 0,
        diagnostics: Vec::new(),
        transcript: vec![],
    });
    observer.on_item_start("n", 0, 1);
    observer.on_item_finish("n", 0, 1, true);
    observer.on_run_finish(&Run {
        id: "run-0".to_string(),
        status: RunStatus::Completed,
        steps: Vec::new(),
    });
}

#[test]
fn run_status_equality() {
    assert_eq!(RunStatus::Completed, RunStatus::Completed);
    assert_ne!(RunStatus::Completed, RunStatus::Failed);
}

#[test]
fn run_status_variants_are_distinct() {
    assert_ne!(RunStatus::Running, RunStatus::Completed);
    assert_ne!(RunStatus::Running, RunStatus::Failed);
    assert_ne!(RunStatus::Completed, RunStatus::Failed);
}

#[test]
fn constructs_execution_step_with_each_status() {
    // `StepStatus` does not derive `PartialEq`, so match to inspect it.
    let ok = ExecutionStep {
        node_id: "parse".to_string(),
        status: StepStatus::Success,
        output: serde_json::json!([{ "json": { "x": 1 } }]),
        duration_ms: 12,
        diagnostics: Vec::new(),
        transcript: vec![],
    };
    assert_eq!(ok.node_id, "parse");
    assert_eq!(ok.duration_ms, 12);
    assert!(matches!(ok.status, StepStatus::Success));
    assert_eq!(ok.output, serde_json::json!([{ "json": { "x": 1 } }]));

    let err = ExecutionStep {
        node_id: "http".to_string(),
        status: StepStatus::Error,
        output: Value::Null,
        duration_ms: 0,
        diagnostics: Vec::new(),
        transcript: vec![],
    };
    assert!(matches!(err.status, StepStatus::Error));
    assert_eq!(err.output, Value::Null);
}

#[test]
fn constructs_run_with_steps() {
    let run = Run {
        id: "run-7".to_string(),
        status: RunStatus::Completed,
        steps: vec![ExecutionStep {
            node_id: "a".to_string(),
            status: StepStatus::Success,
            output: serde_json::json!([]),
            duration_ms: 3,
            diagnostics: Vec::new(),
            transcript: vec![],
        }],
    };
    assert_eq!(run.id, "run-7");
    assert_eq!(run.status, RunStatus::Completed);
    assert_eq!(run.steps.len(), 1);
    assert_eq!(run.steps[0].node_id, "a");
}

#[test]
fn cloned_run_is_independent() {
    let run = Run {
        id: "run-1".to_string(),
        status: RunStatus::Running,
        steps: Vec::new(),
    };
    let clone = run.clone();
    assert_eq!(clone.id, run.id);
    assert_eq!(clone.status, run.status);
}

/// A capturing observer used to assert the callbacks fire with the right
/// records.
#[derive(Default)]
struct Capture {
    started: Mutex<Vec<String>>,
    steps: Mutex<Vec<String>>,
    finished: Mutex<Vec<(String, RunStatus)>>,
}

impl RunObserver for Capture {
    fn on_run_start(&self, run_id: &str) {
        self.started.lock().unwrap().push(run_id.to_string());
    }

    fn on_step_finish(&self, step: &ExecutionStep) {
        self.steps.lock().unwrap().push(step.node_id.clone());
    }

    fn on_run_finish(&self, run: &Run) {
        self.finished
            .lock()
            .unwrap()
            .push((run.id.clone(), run.status.clone()));
    }
}

#[test]
fn custom_observer_receives_all_callbacks() {
    let observer = Capture::default();

    observer.on_run_start("run-9");
    observer.on_step_finish(&ExecutionStep {
        node_id: "first".to_string(),
        status: StepStatus::Success,
        output: serde_json::json!([]),
        duration_ms: 1,
        diagnostics: Vec::new(),
        transcript: vec![],
    });
    observer.on_step_finish(&ExecutionStep {
        node_id: "second".to_string(),
        status: StepStatus::Error,
        output: Value::Null,
        duration_ms: 2,
        diagnostics: Vec::new(),
        transcript: vec![],
    });
    observer.on_run_finish(&Run {
        id: "run-9".to_string(),
        status: RunStatus::Failed,
        steps: Vec::new(),
    });

    assert_eq!(observer.started.lock().unwrap().as_slice(), ["run-9"]);
    assert_eq!(
        observer.steps.lock().unwrap().as_slice(),
        ["first", "second"]
    );
    assert_eq!(
        observer.finished.lock().unwrap().as_slice(),
        [("run-9".to_string(), RunStatus::Failed)]
    );
}

#[test]
fn observer_is_usable_as_trait_object() {
    // The engine holds the observer as `Arc<dyn RunObserver>`; confirm a
    // custom impl coerces and dispatches dynamically.
    let observer: Arc<dyn RunObserver> = Arc::new(Capture::default());
    observer.on_run_start("run-dyn");
    observer.on_step_finish(&ExecutionStep {
        node_id: "n".to_string(),
        status: StepStatus::Success,
        output: serde_json::json!([]),
        duration_ms: 0,
        diagnostics: Vec::new(),
        transcript: vec![],
    });
}

/// A `RunObserver` that records the live agent-event stream alongside steps,
/// so a test can tell the two delivery paths apart.
#[derive(Default)]
struct AgentCapture {
    live: Mutex<Vec<(String, String, String)>>,
    settled: Mutex<Vec<(String, usize)>>,
}

impl RunObserver for AgentCapture {
    fn on_agent_event(&self, node_id: &str, entry: &crate::transcript::TranscriptEntry) {
        self.live.lock().unwrap().push((
            node_id.to_string(),
            entry.kind.clone(),
            entry.text.clone(),
        ));
    }

    fn on_step_finish(&self, step: &ExecutionStep) {
        self.settled
            .lock()
            .unwrap()
            .push((step.node_id.clone(), step.transcript.len()));
    }
}

#[test]
fn on_agent_event_defaults_to_inert() {
    // The whole point of the default body: a host that predates this hook keeps
    // compiling and observes nothing new.
    let observer = NoopObserver;
    observer.on_agent_event(
        "solve",
        &crate::transcript::TranscriptEntry::bounded(1, "agent_thinking", "…"),
    );
}

#[test]
fn agent_events_arrive_live_and_in_order() {
    let observer = AgentCapture::default();
    for (at, kind, text) in [
        (1, "agent_thinking", "Collatz — memoise"),
        (2, "tool_call", "shell: python3 solve.py"),
        (3, "tool_result", "837799"),
    ] {
        observer.on_agent_event(
            "solve",
            &crate::transcript::TranscriptEntry::bounded(at, kind, text),
        );
    }

    let live = observer.live.lock().unwrap();
    // Order is the contract: a transcript read out of order is not a transcript.
    assert_eq!(
        live.iter()
            .map(|(_, kind, _)| kind.as_str())
            .collect::<Vec<_>>(),
        ["agent_thinking", "tool_call", "tool_result"]
    );
    assert!(live.iter().all(|(node, _, _)| node == "solve"));
    assert_eq!(live[2].2, "837799");
}

#[test]
fn a_step_carries_the_settled_transcript() {
    let observer = AgentCapture::default();
    observer.on_step_finish(&ExecutionStep {
        node_id: "solve".to_string(),
        status: StepStatus::Success,
        output: serde_json::json!([]),
        duration_ms: 5,
        diagnostics: Vec::new(),
        transcript: vec![
            crate::transcript::TranscriptEntry::bounded(1, "agent_thinking", "a"),
            crate::transcript::TranscriptEntry::bounded(2, "agent_message", "b"),
        ],
    });
    assert_eq!(
        observer.settled.lock().unwrap().as_slice(),
        [("solve".to_string(), 2)]
    );
}

#[test]
fn a_non_agent_step_carries_no_transcript() {
    // Empty is the normal case, and it must stay cheap: every non-agent node
    // reports one of these on every activation.
    let step = ExecutionStep {
        node_id: "cost".to_string(),
        status: StepStatus::Success,
        output: serde_json::json!([]),
        duration_ms: 0,
        diagnostics: Vec::new(),
        transcript: vec![],
    };
    assert!(step.transcript.is_empty());
}

//! The lowering is where authoring mistakes used to live — every test here
//! is a defect class a real episode paid for.

use serde_json::json;
use tinyflows::model::NodeKind;
use tinyflows::validate::validate_all;

use super::lower;

fn review_recipe() -> serde_json::Value {
    json!({
        "why": "fetch then review",
        "declared": [
            { "name": "repo", "description": "owner/name to review", "required": true }
        ],
        "inputs": { "repo": "acme/thing" },
        "steps": [
            { "id": "fetch", "run": "gh issue list --json number,title" },
            { "id": "review", "ask": "Write the verdict report.", "reads": ["fetch"] }
        ]
    })
}

#[test]
fn a_recipe_lowers_to_a_graph_that_validates() {
    let (graph, inputs, why) = lower(&review_recipe()).expect("lowers");
    assert!(
        validate_all(&graph).is_empty(),
        "a lowered graph must always validate: {:?}",
        validate_all(&graph)
    );
    assert_eq!(graph.nodes.len(), 3, "trigger + two steps");
    assert_eq!(graph.nodes[1].kind, NodeKind::Shell);
    assert_eq!(graph.nodes[2].kind, NodeKind::Agent);
    assert_eq!(inputs["repo"], "acme/thing");
    assert_eq!(why, "fetch then review");
}

#[test]
fn the_generated_prompt_is_one_expression_with_the_right_envelope_paths() {
    let (graph, _, _) = lower(&review_recipe()).expect("lowers");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");
    // The whole string is an expression — the prose-binding failure class
    // cannot occur by construction.
    assert!(prompt.starts_with('='), "{prompt}");
    // A shell upstream is read through `stdout`, the field its kind emits —
    // the exact path a blind author guessed wrong three runs straight.
    assert!(prompt.contains(".nodes.fetch.item.json.stdout"), "{prompt}");
    // Declared inputs are attached without the model writing any binding.
    assert!(prompt.contains(".run.inputs.repo"), "{prompt}");
    // Absent values surface as markers, not silent nothing.
    assert!(prompt.contains("(no output)"), "{prompt}");
}

#[test]
fn an_agent_upstream_is_read_through_text_not_stdout() {
    let recipe = json!({
        "why": "chain of agents",
        "steps": [
            { "id": "draft", "ask": "Draft it." },
            { "id": "polish", "ask": "Polish the draft.", "reads": ["draft"] }
        ]
    });
    let (graph, _, _) = lower(&recipe).expect("lowers");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");
    assert!(prompt.contains(".nodes.draft.item.json.text"), "{prompt}");
}

#[test]
fn quotes_and_newlines_in_an_ask_survive_as_a_valid_jq_literal() {
    let recipe = json!({
        "why": "quoting",
        "steps": [
            { "id": "speak", "ask": "Say \"hello\",\nthen stop." }
        ]
    });
    let (graph, _, _) = lower(&recipe).expect("lowers");
    let prompt = graph.nodes[1].config["prompt"].as_str().expect("prompt");
    assert!(prompt.contains("\\\"hello\\\""), "{prompt}");
    assert!(prompt.contains("\\n"), "{prompt}");
}

#[test]
fn a_worker_on_an_ask_step_becomes_agent_ref() {
    let recipe = json!({
        "why": "placed work",
        "steps": [
            { "id": "build", "ask": "Build it.", "worker": "ci-box" }
        ]
    });
    let (graph, _, _) = lower(&recipe).expect("lowers");
    assert_eq!(graph.nodes[1].config["agent_ref"], "ci-box");
}

#[test]
fn every_structural_problem_is_reported_at_once_with_the_fix() {
    let recipe = json!({
        "why": "broken",
        "steps": [
            { "id": "fetch", "run": "true", "reads": ["later"] },
            { "id": "fetch", "ask": "duplicate id" },
            { "id": "confused", "run": "true", "ask": "both" },
            { "id": "empty" }
        ]
    });
    let err = lower(&recipe).expect_err("refused").to_string();
    for fragment in ["EARLIER", "unique", "split it into two", "neither"] {
        assert!(err.contains(fragment), "missing `{fragment}` in: {err}");
    }
}

#[test]
fn a_declared_value_pasted_into_an_ask_is_refused_with_the_remedy() {
    // Observed on a live host: the author declared `topic` AND wrote
    // "about the topic 'warm caches'" in the ask. The lowering attaches the
    // value anyway, so the paste is redundant now — and poisonous later:
    // selected for a different topic, the prompt carries both, and the keep
    // gate rightly refuses to file the plan. Caught here, the feedback
    // round fixes it before anything runs.
    let recipe = json!({
        "why": "poem",
        "declared": [{ "name": "topic", "description": "", "required": true }],
        "inputs": { "topic": "warm caches" },
        "steps": [
            { "id": "write", "ask": "Write a two-line poem about the topic 'warm caches'." }
        ]
    });
    let err = lower(&recipe).expect_err("refused").to_string();
    assert!(err.contains("pastes the value"), "{err}");
    assert!(err.contains("attached automatically"), "{err}");

    // The same plan without the paste is exactly what should be written.
    let clean = json!({
        "why": "poem",
        "declared": [{ "name": "topic", "description": "", "required": true }],
        "inputs": { "topic": "warm caches" },
        "steps": [
            { "id": "write", "ask": "Write a two-line poem about the given topic." }
        ]
    });
    lower(&clean).expect("keepable");
}

#[test]
fn an_indistinct_input_value_in_an_ask_is_not_a_paste() {
    // "on" appears in half of all prose; refusing on it would block
    // perfectly reusable plans. Only distinctive values count.
    let recipe = json!({
        "why": "toggle",
        "declared": [{ "name": "mode", "description": "", "required": true }],
        "inputs": { "mode": "on" },
        "steps": [
            { "id": "flip", "ask": "Turn the feature on if the mode input says so." }
        ]
    });
    lower(&recipe).expect("not a paste");
}

#[test]
fn a_reply_with_no_steps_says_what_to_return() {
    let err = lower(&json!({ "why": "empty" }))
        .expect_err("refused")
        .to_string();
    assert!(err.contains("at least one step"), "{err}");
}

#[test]
fn ids_are_sanitized_into_engine_and_jq_safe_names() {
    let recipe = json!({
        "why": "messy ids",
        "steps": [
            { "id": "  Fetch-Issues! ", "run": "true" },
            { "id": "review", "ask": "Review.", "reads": ["Fetch-Issues!"] }
        ]
    });
    let (graph, _, _) = lower(&recipe).expect("lowers");
    assert_eq!(graph.nodes[1].id, "fetch_issues");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");
    assert!(prompt.contains(".nodes.fetch_issues.item"), "{prompt}");
}

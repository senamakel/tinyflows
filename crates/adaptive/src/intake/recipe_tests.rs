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
    let (graph, inputs, why) = lower(&review_recipe(), &[]).expect("lowers");
    assert!(
        validate_all(&graph).is_empty(),
        "a lowered graph must always validate: {:?}",
        validate_all(&graph)
    );
    assert_eq!(graph.nodes.len(), 3, "trigger + two steps");
    assert_eq!(graph.nodes[1].kind, NodeKind::Shell);
    assert_eq!(
        graph.nodes[1].config["source"], "gh issue list --json number,title",
        "the engine's shell node reads config.source — a lowered run step \
         under any other key is born broken, and the model cannot fix it"
    );
    assert_eq!(graph.nodes[2].kind, NodeKind::Agent);
    assert_eq!(inputs["repo"], "acme/thing");
    assert_eq!(why, "fetch then review");
}

#[test]
fn the_generated_prompt_is_one_expression_with_the_right_envelope_paths() {
    let (graph, _, _) = lower(&review_recipe(), &[]).expect("lowers");
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
    let (graph, _, _) = lower(&recipe, &[]).expect("lowers");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");
    assert!(prompt.contains(".nodes.draft.item.text"), "{prompt}");
}

#[test]
fn quotes_and_newlines_in_an_ask_survive_as_a_valid_jq_literal() {
    let recipe = json!({
        "why": "quoting",
        "steps": [
            { "id": "speak", "ask": "Say \"hello\",\nthen stop." }
        ]
    });
    let (graph, _, _) = lower(&recipe, &[]).expect("lowers");
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
    let (graph, _, _) = lower(&recipe, &[]).expect("lowers");
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
    let err = lower(&recipe, &[]).expect_err("refused").to_string();
    for fragment in ["EARLIER", "unique", "so split it", "has none of"] {
        assert!(err.contains(fragment), "missing `{fragment}` in: {err}");
    }
}

#[test]
fn a_reply_with_no_steps_says_what_to_return() {
    let err = lower(&json!({ "why": "empty" }), &[])
        .expect_err("refused")
        .to_string();
    assert!(err.contains("at least one step"), "{err}");
}

#[test]
fn the_lowered_shell_config_satisfies_the_engines_own_contract() {
    // Field observation, three attempts of one episode: every run step
    // failed with "shell node missing inline script or script_path" while
    // the author rationally iterated on the only thing the feedback named —
    // a config key it does not write. The lowering emitted `script`; the
    // engine reads `source`. This test asks the ENGINE which required
    // fields its shell contract has and asserts the lowering fills one, so
    // the two cannot drift apart silently again.
    let recipe = json!({
        "why": "fetch",
        "steps": [{ "id": "fetch", "run": "echo hi" }]
    });
    let (graph, _, _) = lower(&recipe, &[]).expect("lowers");
    let shell = tinyflows::catalog::all_contracts()
        .iter()
        .find(|contract| contract.kind == "shell")
        .expect("the engine has a shell contract")
        .clone();
    let required: Vec<&str> = shell
        .config_fields
        .iter()
        .filter(|field| field.required)
        .map(|field| field.name.as_str())
        .collect();
    let config = &graph.nodes[1].config;
    // The shell contract's requirement is one-of (source | script_path), so
    // required may be empty — assert on the actual read keys instead when so.
    if required.is_empty() {
        assert!(
            config.get("source").is_some() || config.get("script_path").is_some(),
            "a lowered run step must fill config.source or config.script_path: {config}"
        );
    } else {
        assert!(
            required.iter().any(|name| config.get(*name).is_some()),
            "the lowering fills none of the engine's required shell fields \
             {required:?}: {config}"
        );
    }
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
    let (graph, _, _) = lower(&recipe, &[]).expect("lowers");
    assert_eq!(graph.nodes[1].id, "fetch_issues");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");
    assert!(prompt.contains(".nodes.fetch_issues.item"), "{prompt}");
}

// ---------------------------------------------------------------------------
// `use` steps: calling a saved workflow as one step of a plan.
// ---------------------------------------------------------------------------

use super::Callable;

fn audit() -> Callable {
    Callable {
        id: "pr-audit-review".to_string(),
        name: "PR audit review".to_string(),
        description: "reviews a pull request and posts the verdict".to_string(),
        inputs: vec![("repo".to_string(), true), ("depth".to_string(), false)],
    }
}

fn compose_recipe() -> serde_json::Value {
    json!({
        "why": "audit the PR, then summarise what it found",
        "declared": [
            { "name": "repo", "description": "owner/name", "required": true }
        ],
        "inputs": { "repo": "acme/thing" },
        "steps": [
            { "id": "audit", "use": "pr-audit-review", "with": { "repo": "@input.repo" } },
            { "id": "summary", "ask": "Summarise the audit in three bullets.",
              "reads": ["audit"] }
        ]
    })
}

#[test]
fn a_use_step_lowers_to_a_sub_workflow_node_that_references_the_callee() {
    let (graph, _, _) = lower(&compose_recipe(), &[audit()]).expect("lowers");
    assert!(
        validate_all(&graph).is_empty(),
        "{:?}",
        validate_all(&graph)
    );
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == "audit")
        .expect("the use step became a node");
    assert_eq!(node.kind, NodeKind::SubWorkflow);
    // By reference, never inlined: the callee keeps its own identity, its own
    // scores, and whatever it becomes next.
    assert_eq!(node.config["workflow_id"], json!("pr-audit-review"));
    assert!(
        node.config.get("workflow").is_none(),
        "an inlined child would fork the callee at authoring time"
    );
    // `@input.repo` became the expression that reads the parent's run input.
    assert_eq!(node.config["inputs"]["repo"], json!("=.run.inputs.repo"));
}

#[test]
fn a_step_reference_in_with_reads_the_earlier_step_the_way_its_kind_produces() {
    let recipe = json!({
        "why": "fetch the diff, then hand it to a saved reviewer",
        "declared": [],
        "steps": [
            { "id": "diff", "run": "git diff" },
            { "id": "review", "use": "reviewer", "with": { "patch": "@step.diff" } }
        ]
    });
    let callable = Callable {
        id: "reviewer".to_string(),
        name: String::new(),
        description: "reviews a patch".to_string(),
        inputs: vec![("patch".to_string(), true)],
    };
    let (graph, _, _) = lower(&recipe, &[callable]).expect("lowers");
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == "review")
        .expect("the use step became a node");
    // A run step's output is its stdout, and the reference knows that without
    // the model having to.
    assert_eq!(
        node.config["inputs"]["patch"],
        json!("=.nodes.diff.item.json.stdout")
    );
}

#[test]
fn a_use_step_naming_a_workflow_nobody_offered_is_refused_at_intake() {
    // Not deferred to the resolver: a hallucinated id would surface as a
    // capability error mid-run, after the earlier steps had already been paid
    // for, and be attributed to the work rather than to the plan.
    let err = lower(&compose_recipe(), &[])
        .expect_err("refused")
        .to_string();
    assert!(err.contains("no saved workflows to call"), "{err}");

    let other = Callable {
        id: "something-else".to_string(),
        ..audit()
    };
    let err = lower(&compose_recipe(), &[other])
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("something-else"),
        "names what IS callable: {err}"
    );
}

#[test]
fn a_use_step_that_omits_a_required_input_is_refused_with_the_name() {
    let recipe = json!({
        "why": "call it with nothing",
        "steps": [{ "id": "audit", "use": "pr-audit-review", "with": {} }]
    });
    let err = lower(&recipe, &[audit()]).expect_err("refused").to_string();
    assert!(err.contains("requires the input `repo`"), "{err}");
}

#[test]
fn a_with_key_the_callee_never_declared_is_refused_rather_than_dropped() {
    // Silently dropping it would leave the model believing it passed a value
    // the child will never see — the worst kind of pass, because the run
    // completes.
    let recipe = json!({
        "why": "wrong input name",
        "declared": [{ "name": "repo", "description": "owner/name", "required": true }],
        "steps": [{
            "id": "audit", "use": "pr-audit-review",
            "with": { "repo": "@input.repo", "reponame": "acme/thing" }
        }]
    });
    let err = lower(&recipe, &[audit()]).expect_err("refused").to_string();
    assert!(err.contains("declares no input `reponame`"), "{err}");
    assert!(err.contains("repo, depth"), "says what it does take: {err}");
}

#[test]
fn an_input_reference_to_something_undeclared_is_refused() {
    let recipe = json!({
        "why": "reference an input that does not exist",
        "declared": [],
        "steps": [{
            "id": "audit", "use": "pr-audit-review", "with": { "repo": "@input.repo" }
        }]
    });
    let err = lower(&recipe, &[audit()]).expect_err("refused").to_string();
    assert!(err.contains("did not declare"), "{err}");
}

#[test]
fn reads_on_a_use_step_points_at_with_instead() {
    let recipe = json!({
        "why": "reads does not apply",
        "declared": [{ "name": "repo", "description": "owner/name", "required": true }],
        "steps": [
            { "id": "diff", "run": "git diff" },
            { "id": "audit", "use": "pr-audit-review", "reads": ["diff"],
              "with": { "repo": "@input.repo" } }
        ]
    });
    let err = lower(&recipe, &[audit()]).expect_err("refused").to_string();
    assert!(err.contains("`with`"), "{err}");
}

#[test]
fn a_step_reading_a_use_step_gets_the_childs_answer_not_its_run_state() {
    // The defect this projection exists for: a `sub_workflow` node emits the
    // child's ENTIRE final run state, so a naive read hands the next agent the
    // child's bookkeeping with the deliverable buried in it.
    let (graph, _, _) = lower(&compose_recipe(), &[audit()]).expect("lowers");
    let summary = graph
        .nodes
        .iter()
        .find(|node| node.id == "summary")
        .expect("the ask step");
    let prompt = summary.config["prompt"]
        .as_str()
        .expect("a generated prompt expression");

    // Run the generated expression against a real child run state, through the
    // engine's own evaluator — the only thing that proves the projection is
    // valid jq and picks the right leaves.
    let state = json!({
        "run": { "inputs": { "repo": "acme/thing" } },
        "inputs": { "repo": "acme/thing" },
        "nodes": { "audit": { "item": child_run_state(), "items": [child_run_state()] } }
    });
    let rendered = tinyflows::expr::resolve(&json!(prompt), &state);
    let rendered = rendered.as_str().expect("resolves to a string");

    assert!(
        rendered.contains("Requesting changes."),
        "the child's deliverable must reach the reader: {rendered}"
    );
    assert!(
        rendered.contains("3 files changed"),
        "and so must every other leaf it produced: {rendered}"
    );
    assert!(
        rendered.contains("## verdict"),
        "each labelled with the child step it came from: {rendered}"
    );
    assert!(
        !rendered.contains("trigger"),
        "but not the child's own bookkeeping: {rendered}"
    );
}

#[test]
fn a_child_that_produced_nothing_readable_says_so_rather_than_erroring() {
    // The projection walks a state this graph did not choose the shape of, so
    // every hop is written defensively; a jq error here would fail the parent
    // node instead of reporting the step that produced nothing.
    let (graph, _, _) = lower(&compose_recipe(), &[audit()]).expect("lowers");
    let prompt = graph
        .nodes
        .iter()
        .find(|node| node.id == "summary")
        .expect("the ask step")
        .config["prompt"]
        .as_str()
        .expect("a generated prompt expression")
        .to_string();

    for state in [
        json!({ "run": {}, "nodes": { "audit": { "item": { "nodes": {} }, "items": [] } } }),
        json!({ "run": {}, "nodes": { "audit": { "item": null, "items": [] } } }),
        json!({ "run": {}, "nodes": {} }),
    ] {
        let rendered = tinyflows::expr::resolve(&json!(prompt), &state);
        let rendered = rendered.as_str().unwrap_or_default();
        assert!(
            rendered.contains("(no output)"),
            "empty child state {state} rendered: {rendered}"
        );
    }
}

#[test]
fn the_callable_listing_names_the_inputs_a_call_must_fill() {
    // A model asked to supply `with` for inputs it was never shown is a model
    // guessing — the same defect the chooser had.
    let rendered = super::render_callables(&[audit()]);
    assert!(rendered.contains("pr-audit-review"), "{rendered}");
    assert!(
        rendered.contains("with: repo, depth (optional)"),
        "{rendered}"
    );
    assert!(
        super::render_callables(&[]).is_empty(),
        "a cold store offers no `use` list at all, rather than an empty one"
    );
}

/// A child workflow's final run state, in the shape the engine records it.
///
/// Raw run-state slots, so items are serialized (`{"json": …}`) — one shape in
/// from the parent's scope projection, which exposes bare payloads. Getting
/// that boundary wrong is precisely what `child_answer` has to survive.
fn child_run_state() -> serde_json::Value {
    json!({
        "run": { "trigger": [], "inputs": { "repo": "acme/thing" } },
        "nodes": {
            // The trigger slot, verbatim from a real run: its payload is the
            // seeded item ARRAY, not an object. Every child has one, and it is
            // what made the first spelling of the projection fail — a
            // fixture whose slots were all objects passed while the real
            // thing resolved the whole prompt to null.
            "start": { "items": [{ "json": [{ "json": {} }] }] },
            "fetch_pr": { "items": [{ "json": {
                "json": { "exit_code": 0, "stdout": "3 files changed" },
                "text": null, "raw": {}
            } }] },
            "verdict": { "items": [{ "json": {
                "json": { "text": "Requesting changes.", "worker": "local" },
                "text": "Requesting changes.",
                "raw": { "text": "Requesting changes." }
            } }] }
        }
    })
}

#[test]
fn an_agents_prose_is_read_from_the_envelopes_text_not_from_inside_its_json() {
    // The regression this file exists to prevent, found in this file's own
    // output: `item.json.text` reads a `text` field inside the STRUCTURED
    // value, which a prose reply has not got, so every `reads` of an agent
    // step rendered "(no output)". Evaluated rather than string-matched —
    // asserting the path spelling is what let the wrong spelling ship.
    let recipe = json!({
        "why": "chain of agents",
        "steps": [
            { "id": "draft", "ask": "Draft it." },
            { "id": "polish", "ask": "Polish the draft.", "reads": ["draft"] }
        ]
    });
    let (graph, _, _) = lower(&recipe, &[]).expect("lowers");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");

    let scope = json!({
        "run": {}, "inputs": null, "item": null, "items": [],
        "nodes": { "draft": { "item": {
            "json": "The draft, in prose.",
            "text": "The draft, in prose.",
            "raw": "The draft, in prose."
        } } }
    });
    let rendered = tinyflows::expr::resolve(&json!(prompt), &scope);
    let rendered = rendered.as_str().expect("resolves to a string");
    assert!(
        rendered.contains("The draft, in prose."),
        "an upstream agent's reply must reach the next step: {rendered}"
    );
    assert!(
        !rendered.contains("(no output)"),
        "it rendered the missing-value marker instead: {rendered}"
    );
}

#[test]
fn a_scripts_stdout_is_read_from_inside_its_json_because_that_is_where_it_is() {
    // The asymmetry that made the agent path look right: a shell node's
    // structured value genuinely holds `{exit_code, stdout}`, so this one IS
    // nested. Pinned by evaluation so the two never get "harmonised".
    let (graph, _, _) = lower(&review_recipe(), &[]).expect("lowers");
    let prompt = graph.nodes[2].config["prompt"].as_str().expect("prompt");
    let scope = json!({
        "run": { "inputs": { "repo": "acme/thing" } },
        "inputs": { "repo": "acme/thing" }, "item": null, "items": [],
        "nodes": { "fetch": { "item": {
            "json": { "exit_code": 0, "stdout": "#41 flaky test" },
            "text": null, "raw": {}
        } } }
    });
    let rendered = tinyflows::expr::resolve(&json!(prompt), &scope);
    let rendered = rendered.as_str().expect("resolves to a string");
    assert!(rendered.contains("#41 flaky test"), "{rendered}");
    assert!(
        rendered.contains("acme/thing"),
        "declared inputs too: {rendered}"
    );
}

//! Writing a graph when nothing stored fits.
//!
//! Two things make the difference between a graph that runs and one that
//! validates and then does nothing, and both are here rather than in the
//! prompt's good intentions:
//!
//! * the node catalogue is **generated from the engine**, not described from
//!   memory, so a config field cannot be invented; and
//! * the result is **validated before it is returned**, so an authoring mistake
//!   is an error from intake rather than a run-time failure that reads like the
//!   work failing.

use serde_json::Value;
use tinyflows::caps::Capabilities;
use tinyflows::catalog::{NodeKindContract, all_contracts};
use tinyflows::model::WorkflowGraph;
use tinyflows::store::HostPolicy;
use tinyflows::validate::validate_all;

use super::{Attempt, IntakeError, Result, ask};
use crate::contracts::{Approach, Goal, Tier};
use crate::host::HostFacts;

const SYSTEM: &str = "\
You write a workflow graph that achieves a goal.

Return JSON: {\"graph\": <WorkflowGraph>, \"why\": str, \"inputs\": {name: value}}

The graph is the engine's own format:
{\"schema_version\": 1, \"name\": str, \"inputs\": [{\"name\", \"required\", \"description\"}],
 \"nodes\": [{\"id\", \"kind\", \"name\", \"config\": {...}}],
 \"edges\": [{\"from_node\", \"from_port\", \"to_node\", \"to_port\"}]}

Rules that are checked, not requested:

- Exactly one `trigger` node, and it is where the graph starts.
- Every node kind and every config field must come from the catalogue below.
  It is generated from the engine, so it is the truth; a field you remember
  from elsewhere is a field that resolves to null at run time.
- Ports default to `main` on both ends. Name one only where the catalogue says
  a node has others (a `condition` emits `true`/`false`; a `loop` emits `body`
  and `done`).
- Declare in `inputs` anything the goal supplies as data — a repository, a path,
  an id — and read it in config rather than pasting the literal. A graph with
  the value baked in is a graph that works once.

How one node reads another. A config string starting with `=` is an expression;
everything else is a literal.

    =item.name                     a field of the direct predecessor's output
    =nodes.fetch.item.json.body    a field of any completed node, by node id
    =run.trigger.payload           what the trigger carried
    =run.inputs.topic              a declared workflow input, by its name
    =.items | length               a leading dot makes the rest a jq program

There are no braces. `={{ ... }}` is not a binding — it is a jq program that
fails to compile, and a failed program is null, so the step runs with an empty
value and reports success.

An `=` anywhere but the FIRST character is literal text, not a binding:
`\"about: =run.inputs.topic\"` sends those exact characters to the model. To
put a value inside prose — an agent prompt, a message — the whole string is
one expression, jq with explicit dots:

    =\"Write a poem about \\(.run.inputs.topic)\"
    =\"Summarise \" + .run.inputs.repo

`agent`, `tool_call` and `http_request` wrap their output in
`{json, text, raw}`. Their fields are under `.json`: write
`=nodes.fetch.item.json.body`, never `=nodes.fetch.item.body`. The second form
validates, dry-runs green, and resolves to null every time.

Design guidance, which is judgement rather than a check:

- Fewer nodes is better. An `agent` node is a whole coding-agent session on some
  hosts — minutes, not seconds — so a graph of eight is usually a worse answer
  than a graph of three.
- Use `agent` for work that cannot be specified, and the determined kinds for
  everything else. Fetching, reshaping and branching are not agent work.
- Say what a step is for, concretely. The agent running it sees the goal and
  that instruction and nothing else — not the other nodes, not what they found.

Where a section below states what this host permits, it is the machine's own
configuration and is enforced when the graph runs. A graph that ignores it saves
cleanly, validates cleanly, and fails the first time it matters.

Where a section lists what this episode already tried, write something
DIFFERENT. Not the same graph with a reworded prompt — a different shape: other
nodes, another order, a step that checks what the last attempt assumed. If every
approach you can think of is already on that list, say so in `why` and write the
smallest graph that would establish which assumption is wrong.";

/// Write a graph for `goal`, grounded on the engine's own node catalogue.
///
/// # Errors
/// When inference fails, the reply holds no graph, or the graph does not
/// validate. An invalid graph is never returned: the caller would hand it
/// straight to `compile`, and the resulting failure would be attributed to the
/// work rather than to the authoring.
pub async fn author(
    goal: &Goal,
    facts: &HostFacts,
    policy: &dyn HostPolicy,
    past: &str,
    caps: &Capabilities,
    conn: Option<&str>,
) -> Result<Attempt> {
    let permitted = facts.render();
    let user = format!(
        "# Goal\n{}\n\n# Node catalogue — the only kinds and fields that exist\n{}{}{past}",
        goal.text.trim(),
        catalogue(),
        if permitted.is_empty() {
            String::new()
        } else {
            format!("\n\n{permitted}")
        }
    );

    // The gates below produce readable refusals on purpose, and this loop is
    // where they earn it: a refused graph goes back to the model with the
    // refusal, once per round, rather than costing the whole episode. Bounded,
    // because a model that cannot fix its graph in two more tries is telling
    // us the answer.
    //
    // A reply that never became an answer — no JSON object, a transport
    // failure — is retried too, but with the prompt unchanged: there is no
    // graph to give feedback on, and a resample is the whole remedy.
    let mut prompt = user;
    let mut last: Option<IntakeError> = None;
    for _ in 0..ROUNDS {
        let answer = match ask(caps, conn, Tier::Author, SYSTEM, &prompt).await {
            Ok(answer) => answer,
            Err(err) => {
                last = Some(err);
                continue;
            }
        };
        match gated(&answer, facts, policy) {
            Ok(attempt) => return Ok(attempt),
            Err(err) => {
                prompt = format!(
                    "{prompt}\n\n# Your previous graph was refused — fix exactly this\n\
                     {err}\n\nReturn the corrected, complete JSON reply."
                );
                last = Some(err);
            }
        }
    }
    Err(last.unwrap_or_else(|| IntakeError::Inference("the author was never asked".to_string())))
}

/// How many replies the author gets before the failure is the answer.
const ROUNDS: usize = 3;

/// One reply through every gate, or why it was refused.
fn gated(answer: &Value, facts: &HostFacts, policy: &dyn HostPolicy) -> Result<Attempt> {
    let raw = answer
        .get("graph")
        .cloned()
        .ok_or_else(|| IntakeError::Inference("the reply has no `graph` key".to_string()))?;

    let graph: WorkflowGraph = serde_json::from_value(raw)
        .map_err(|e| IntakeError::Invalid(format!("not a workflow graph: {e}")))?;

    // Every failure at once, not the first. A model handed one error fixes it
    // and returns with the next; handed all four it fixes all four.
    let problems = validate_all(&graph);
    if !problems.is_empty() {
        return Err(IntakeError::Invalid(
            problems
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        ));
    }

    // The single most common authoring mistake, across every model tried: a
    // binding path inside prose — `"about: .run.inputs.topic"` — which the
    // engine reads as those literal characters, so the step runs on garbage
    // and reports success. Mechanically detectable, so it is refused here and
    // fixed through the feedback loop rather than found by a judge two
    // minutes and one model call later.
    let prose = prose_bindings(&graph);
    if !prose.is_empty() {
        return Err(IntakeError::Invalid(prose.join("; ")));
    }

    // Three gates, and the order is cost. `validate_all` is structural and
    // free. `HostFacts::check` is our own reading of the machine's config.
    // `check_graph` is the host's, which may know things we were not told —
    // it runs last because it is the one that can reach outside this process.
    let refused = facts.check(&graph);
    if !refused.is_empty() {
        return Err(IntakeError::Unsupported(refused.join("; ")));
    }
    if let Err(err) = policy.check_graph(graph.id.as_deref().unwrap_or("authored"), &graph) {
        return Err(IntakeError::Unsupported(err.to_string()));
    }

    Ok(Attempt {
        approach: Approach::Authored {
            why: answer["why"].as_str().unwrap_or_default().to_string(),
            fingerprint: fingerprint(&graph),
        },
        graph,
        inputs: answer["inputs"].as_object().cloned().unwrap_or_default(),
        // Filled by `decide`, which is what knows what the planner was shown.
        lessons_shown: Vec::new(),
    })
}

/// Config strings that embed a binding path in prose instead of being one.
///
/// A string that does not start with `=` is a literal, whole. One that
/// mentions `run.inputs.`, `run.trigger.` or `nodes.<id>.` inside prose was
/// almost certainly meant to interpolate — and will instead hand the model,
/// the tool or the request those exact characters. Expression strings
/// (leading `=`) are exempt: `="about \(.run.inputs.topic)"` legitimately
/// contains the path.
fn prose_bindings(graph: &WorkflowGraph) -> Vec<String> {
    const PATHS: [&str; 4] = ["run.inputs.", "run.trigger.", "=run.", "=nodes."];

    fn scan(node: &str, field: &str, value: &Value, out: &mut Vec<String>) {
        match value {
            Value::String(s) if !s.starts_with('=') => {
                if PATHS.iter().any(|p| s.contains(p)) {
                    out.push(format!(
                        "node `{node}` config `{field}` embeds a binding path in literal \
                         text, which the engine passes through as those exact characters. \
                         Make the whole string one expression instead: \
                         =\"… \\(.run.inputs.name) …\""
                    ));
                }
            }
            Value::Object(map) => {
                for (key, nested) in map {
                    scan(node, key, nested, out);
                }
            }
            Value::Array(items) => {
                for nested in items {
                    scan(node, field, nested, out);
                }
            }
            _ => {}
        }
    }

    let mut out = Vec::new();
    for node in &graph.nodes {
        scan(&node.id, "config", &node.config, &mut out);
    }
    out
}

/// A digest of the graph's runnable shape.
///
/// Nodes, edges and declared inputs — not the name, not the description. Two
/// graphs that run identically and differ in prose are the same attempt, and
/// the whole point of the exclusion list is that the second one is recognised
/// as a repeat rather than counted as a fresh idea. Inputs are in because a
/// graph that requires a value behaves differently from one that does not,
/// even when every node matches.
fn fingerprint(graph: &WorkflowGraph) -> String {
    // The stable digest, because this string is persisted in ledger rows as
    // the exclusion-list signature — see `reuse::digest_hex` on why not
    // `DefaultHasher`.
    crate::reuse::digest_hex(&crate::reuse::shape_bytes(graph))
}

/// The node catalogue, rendered for a prompt.
///
/// Generated from [`all_contracts`] rather than written out here, so a node
/// kind the engine gains appears without this file being touched — and a field
/// this file could describe wrongly cannot exist.
fn catalogue() -> String {
    all_contracts()
        .iter()
        .map(render)
        .collect::<Vec<_>>()
        .join("\n")
}

fn render(contract: &NodeKindContract) -> String {
    let fields = contract
        .config_fields
        .iter()
        .map(|field| {
            let mark = if field.required { "*" } else { " " };
            let allowed = match field.enum_values.as_ref() {
                Some(values) if !values.is_empty() => format!(" [{}]", values.join("|")),
                _ => String::new(),
            };
            format!("    {mark}{}: {}{allowed}", field.name, field.value_type)
        })
        .collect::<Vec<_>>()
        .join("\n");

    // Only the outputs, and only when they are not the default. `from_port` is
    // the field an author gets wrong; inputs are almost always `main` and
    // listing them on every kind is noise that hides the one that matters.
    let outputs = &contract.ports.outputs;
    let ports = if outputs.as_slice() == ["main".to_string()] || outputs.is_empty() {
        String::new()
    } else {
        format!("  out ports: {}\n", outputs.join(", "))
    };

    format!("{}: {}\n{ports}{fields}", contract.kind, contract.summary)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalogue_is_generated_from_the_engine() {
        // If this list were written by hand it would already be wrong: it is
        // the thing the prompt calls the truth.
        let rendered = catalogue();
        for kind in [
            "trigger",
            "agent",
            "tool_call",
            "http_request",
            "condition",
            "loop",
        ] {
            assert!(rendered.contains(kind), "catalogue is missing {kind}");
        }
    }

    #[test]
    fn required_fields_are_marked() {
        let rendered = catalogue();
        // `trigger_kind` is required on a trigger; a model that misses it
        // authors a graph that cannot start.
        assert!(rendered.contains("*trigger_kind"), "{rendered}");
    }

    #[test]
    fn enum_fields_show_their_allowed_values() {
        assert!(
            catalogue().contains("manual"),
            "trigger_kind's values must be listed"
        );
    }

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
                let graph = if prompts.len() == 1 {
                    // No trigger: fails `validate_all`, must come back.
                    serde_json::json!({
                        "schema_version": 1, "name": "broken",
                        "inputs": [], "nodes": [], "edges": []
                    })
                } else {
                    assert!(
                        shown.contains("refused"),
                        "the retry prompt must carry the refusal, got: {shown}"
                    );
                    serde_json::json!({
                        "schema_version": 1, "name": "fixed", "inputs": [],
                        "nodes": [{
                            "id": "start", "kind": "trigger", "name": "manual",
                            "config": { "trigger_kind": "manual" }
                        }],
                        "edges": []
                    })
                };
                Ok(serde_json::json!({ "graph": graph, "why": "test", "inputs": {} }))
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
            &Permissive,
            "",
            &caps,
            None,
        )
        .await
        .expect("the corrected graph must land");
        assert_eq!(attempt.graph.name, "fixed");
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
}

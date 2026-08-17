//! The simple authoring surface: a recipe of steps, lowered to a real graph.
//!
//! Six field-test runs established why full-graph authoring fails at small
//! model tiers: the author must emit exact tokens across three foreign
//! syntaxes at once — the graph dialect (`=`-expressions, envelope paths),
//! the host's tools, and whatever CLI its scripts drive — blind, with
//! feedback one round away. One wrong token anywhere is a dead graph, and
//! the defects surface serially.
//!
//! So the model does not write graphs. It writes a **recipe** — steps that
//! either `run` a script or `ask` an agent, with `reads` naming which
//! earlier steps' output an agent needs — and [`lower`] compiles that into a
//! valid [`WorkflowGraph`] deterministically. Every expression, envelope
//! path and edge is generated here, by code that knows the engine's shapes
//! exactly. The model's remaining obligations are things models are good
//! at: choosing steps, writing commands, writing prose.
//!
//! The lowered graph still walks every downstream gate. By construction it
//! should pass them all; a construction bug surfacing as a refusal instead
//! of a run-time null is the point of keeping them.

use serde_json::{Map, Value, json};
use tinyflows::model::{Edge, InputType, Node, NodeKind, WorkflowGraph, WorkflowInput};

use super::IntakeError;

/// The authoring prompt for the recipe surface.
///
/// Deliberately free of graph syntax: nothing here teaches nodes, edges,
/// bindings or envelopes, because the model never writes them.
pub const SYSTEM: &str = "\
You plan how to achieve a goal as a short sequence of steps.

Return JSON:
{\"why\": str,
 \"declared\": [{\"name\": str, \"description\": str, \"required\": bool}],
 \"inputs\": {name: value},
 \"steps\": [
   {\"id\": str, \"run\": str},
   {\"id\": str, \"ask\": str, \"reads\": [str], \"worker\": str?}
 ]}

- Steps execute in the order listed. Each step has an `id` (a short
  snake_case name) and exactly ONE of:
    run  a shell script. It must PRINT its result to stdout — a result in a
         file or nowhere is a result the next step cannot see. Print JSON
         when the output is structured.
    ask  an instruction for an AI agent. Say exactly what to produce and
         that it should be produced directly. The agent's reply text is the
         step's output.
- `reads` (ask steps only): the ids of EARLIER steps whose output this agent
  needs. Their output is attached to the instruction automatically — do not
  describe how to fetch it, and do not use placeholders for it.
- `worker` (ask steps only, optional): a worker this host lists, when the
  step must run somewhere specific. Omit it otherwise.
- `declared`: the workflow's inputs — anything the goal supplies as data (a
  repository, a topic, an id), so the plan works for the NEXT goal of its
  kind with different values. `inputs` supplies this run's value for every
  required one. Declared values are attached to ask steps automatically.
- The LAST step's output is the run's answer: make it the step that produces
  the deliverable.

Keep it short. One step is often right: an agent asked for the whole
deliverable, with the goal's data declared as inputs. Use `run` steps for
deterministic fetching or checking, not for judgement.

Where a section below states what this host permits, it is enforced when the
plan runs. Where a section lists what this episode already tried, produce a
DIFFERENT plan, not the same one reworded.";

/// One parsed step of a recipe.
struct Step {
    id: String,
    action: Action,
    reads: Vec<String>,
}

enum Action {
    Run(String),
    Ask {
        prompt: String,
        worker: Option<String>,
    },
}

/// Lower a recipe reply into a runnable graph plus its run values.
///
/// # Errors
/// [`IntakeError::Invalid`] naming every structural problem at once — absent
/// steps, duplicate or malformed ids, `reads` pointing forward or nowhere, a
/// step that is both `run` and `ask` or neither. The messages are written
/// for the feedback round: each states the fix, not just the fault.
pub fn lower(answer: &Value) -> Result<(WorkflowGraph, Map<String, Value>, String), IntakeError> {
    let why = answer["why"].as_str().unwrap_or_default().to_string();
    let steps = parse_steps(answer)?;
    let declared = parse_declared(answer);
    let inputs = answer["inputs"].as_object().cloned().unwrap_or_default();

    let mut nodes = vec![Node {
        id: "start".into(),
        kind: NodeKind::Trigger,
        type_version: 1,
        name: "start".into(),
        config: json!({ "trigger_kind": "manual" }),
        ports: Vec::new(),
        position: None,
    }];
    let mut edges = Vec::new();
    let mut previous = "start".to_string();

    for step in &steps {
        let node = match &step.action {
            Action::Run(script) => Node {
                id: step.id.clone(),
                kind: NodeKind::Shell,
                type_version: 1,
                name: step.id.clone(),
                config: json!({ "script": script }),
                ports: Vec::new(),
                position: None,
            },
            Action::Ask { prompt, worker } => {
                let mut config = json!({
                    "prompt": ask_expression(prompt, &step.reads, &steps, &declared)
                });
                if let Some(worker) = worker {
                    config["agent_ref"] = json!(worker);
                }
                Node {
                    id: step.id.clone(),
                    kind: NodeKind::Agent,
                    type_version: 1,
                    name: step.id.clone(),
                    config,
                    ports: Vec::new(),
                    position: None,
                }
            }
        };
        nodes.push(node);
        edges.push(Edge {
            from_node: previous.clone(),
            from_port: "main".into(),
            to_node: step.id.clone(),
            to_port: "main".into(),
        });
        previous = step.id.clone();
    }

    let graph = WorkflowGraph {
        schema_version: 1,
        id: None,
        name: graph_name(&why, &steps),
        inputs: declared
            .iter()
            .map(|(name, description, required)| {
                let input = WorkflowInput::new(name.clone(), InputType::String)
                    .with_description(description.clone());
                if *required { input.required() } else { input }
            })
            .collect(),
        agents: Vec::new(),
        nodes,
        edges,
    };
    Ok((graph, inputs, why))
}

/// The generated prompt expression for an ask step.
///
/// A jq program the model never sees: the instruction as a quoted literal,
/// then every declared input, then each read step's output through the path
/// its kind actually produces — `stdout` for a script (with the engine's
/// pre-parsed `stdout_json` unnecessary here: the agent reads text), `text`
/// for an upstream agent. Missing values render as an explicit marker
/// rather than vanishing, because an agent told "output: (missing)" says so
/// instead of improvising.
fn ask_expression(
    prompt: &str,
    reads: &[String],
    steps: &[Step],
    declared: &[(String, String, bool)],
) -> String {
    let mut program = format!("={}", jq_quote(prompt));
    for (name, _, _) in declared {
        program.push_str(&format!(
            " + {} + ((.run.inputs.{name} // \"(not provided)\") | tostring)",
            jq_quote(&format!("\n\n# Input `{name}`\n"))
        ));
    }
    for read in reads {
        let path = match steps
            .iter()
            .find(|step| &step.id == read)
            .map(|step| &step.action)
        {
            Some(Action::Run(_)) => format!("(.nodes.{read}.item.json.stdout // \"(no output)\")"),
            _ => format!("(.nodes.{read}.item.json.text // \"(no output)\")"),
        };
        program.push_str(&format!(
            " + {} + ({path} | tostring)",
            jq_quote(&format!("\n\n# Output of step `{read}`\n"))
        ));
    }
    program
}

/// A string as a jq literal: quoted, with the characters jq treats specially
/// escaped. Newlines become `\n` so the program stays one line.
fn jq_quote(text: &str) -> String {
    let mut quoted = String::with_capacity(text.len() + 2);
    quoted.push('"');
    for ch in text.chars() {
        match ch {
            '"' => quoted.push_str("\\\""),
            '\\' => quoted.push_str("\\\\"),
            '\n' => quoted.push_str("\\n"),
            '\r' => quoted.push_str("\\r"),
            '\t' => quoted.push_str("\\t"),
            other => quoted.push(other),
        }
    }
    quoted.push('"');
    quoted
}

fn parse_steps(answer: &Value) -> Result<Vec<Step>, IntakeError> {
    let raw = answer["steps"]
        .as_array()
        .filter(|steps| !steps.is_empty())
        .ok_or_else(|| {
            IntakeError::Invalid(
                "the reply has no `steps` — return at least one step with an `id` and a \
                 `run` script or an `ask` instruction"
                    .to_string(),
            )
        })?;

    let mut problems = Vec::new();
    let mut steps: Vec<Step> = Vec::new();
    for (index, step) in raw.iter().enumerate() {
        let id = sanitize_id(step["id"].as_str().unwrap_or_default());
        if id.is_empty() {
            problems.push(format!("step {index} has no usable `id`"));
            continue;
        }
        if id == "start" || steps.iter().any(|existing| existing.id == id) {
            problems.push(format!("step id `{id}` is taken — ids must be unique"));
            continue;
        }
        let run = step["run"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let ask = step["ask"]
            .as_str()
            .map(str::trim)
            .filter(|s| !s.is_empty());
        let action = match (run, ask) {
            (Some(script), None) => Action::Run(script.to_string()),
            (None, Some(prompt)) => Action::Ask {
                prompt: prompt.to_string(),
                worker: step["worker"]
                    .as_str()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToString::to_string),
            },
            (Some(_), Some(_)) => {
                problems.push(format!(
                    "step `{id}` has both `run` and `ask` — split it into two steps"
                ));
                continue;
            }
            (None, None) => {
                problems.push(format!(
                    "step `{id}` has neither a `run` script nor an `ask` instruction"
                ));
                continue;
            }
        };
        let mut reads = Vec::new();
        if let Some(raw_reads) = step["reads"].as_array() {
            for read in raw_reads {
                let read = sanitize_id(read.as_str().unwrap_or_default());
                if steps.iter().any(|existing| existing.id == read) {
                    reads.push(read);
                } else {
                    problems.push(format!(
                        "step `{id}` reads `{read}`, which is not an EARLIER step id"
                    ));
                }
            }
        }
        if matches!(action, Action::Run(_)) && !reads.is_empty() {
            problems.push(format!(
                "step `{id}`: `reads` only works on ask steps — a run script sees nothing"
            ));
        }
        steps.push(Step { id, action, reads });
    }
    if !problems.is_empty() {
        return Err(IntakeError::Invalid(problems.join("; ")));
    }
    Ok(steps)
}

fn parse_declared(answer: &Value) -> Vec<(String, String, bool)> {
    answer["declared"]
        .as_array()
        .map(|declared| {
            declared
                .iter()
                .filter_map(|input| {
                    let name = sanitize_id(input["name"].as_str().unwrap_or_default());
                    if name.is_empty() {
                        return None;
                    }
                    Some((
                        name,
                        input["description"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string(),
                        input["required"].as_bool().unwrap_or(false),
                    ))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// A graph name from the recipe: the first ask step's opening words, or the
/// step ids — something a shelf listing can show, not an id.
fn graph_name(why: &str, steps: &[Step]) -> String {
    let head: String = why.split_whitespace().take(6).collect::<Vec<_>>().join(" ");
    if !head.is_empty() {
        return head;
    }
    steps
        .iter()
        .map(|step| step.id.as_str())
        .collect::<Vec<_>>()
        .join(" → ")
}

/// Identifiers the engine and jq both accept: lowercase, alnum and `_`.
fn sanitize_id(raw: &str) -> String {
    let mut id: String = raw
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    while id.starts_with('_') {
        id.remove(0);
    }
    while id.ends_with('_') {
        id.pop();
    }
    id
}

#[cfg(test)]
#[path = "recipe_tests.rs"]
mod tests;

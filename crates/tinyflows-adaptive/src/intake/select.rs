//! Choosing a stored workflow, or declining to.
//!
//! The cheap path, and the one that should win once anything has been learned.
//! A selection is one small call against a list; authoring is a large call that
//! also discards whatever the existing procedure had proved about itself.
//!
//! Declining is a first-class answer, not a failure. A model pushed to always
//! pick something will pick the nearest thing, and a near-miss workflow runs to
//! completion producing confidently wrong work — which is more expensive than
//! authoring, not less.

use serde_json::{Map, Value};
use tinyflows::caps::Capabilities;
use tinyflows::model::WorkflowGraph;
use tinyflows::store::WorkflowStore;

use super::{Attempt, IntakeError, Result, ask};
use crate::contracts::{Approach, Goal, Tier};

/// One stored workflow as the chooser sees it.
#[derive(Debug, Clone)]
pub struct Candidate {
    /// The id the choice is made on.
    pub id: String,
    /// Display name; falls back to the id when blank.
    pub name: String,
    /// What the model actually reads to decide. A workflow with none is a row
    /// nobody can choose on purpose.
    pub description: String,
    /// A rough cost signal.
    pub node_count: usize,
    /// Times chosen and run.
    pub applied: u32,
    /// Times that ended satisfied.
    pub helped: u32,
    /// Its declared inputs: name and whether it is required.
    ///
    /// Listed because the chooser is asked to supply values for them. It was
    /// being asked to fill inputs it had never been shown, which is a guess
    /// dressed as a binding — and a required input guessed wrong is a run
    /// that fails after the choice has already been made.
    pub inputs: Vec<(String, bool)>,
}

impl Candidate {
    fn render(&self) -> String {
        let name = if self.name.is_empty() {
            &self.id
        } else {
            &self.name
        };
        let description = if self.description.is_empty() {
            "(no description — nobody can choose this on purpose)"
        } else {
            &self.description
        };
        // Both numbers, never a rate: 1/1 and 40/40 are the same rate and are
        // not the same evidence, and the model is being asked to weigh exactly
        // that difference.
        let record = match self.applied {
            0 => "never run".to_string(),
            applied => format!("run {applied}×, satisfied {}×", self.helped),
        };
        let inputs = if self.inputs.is_empty() {
            String::new()
        } else {
            format!("\n  inputs: {}", super::recipe::render_inputs(&self.inputs))
        };
        format!(
            "- id: {}\n  name: {name}\n  steps: {}, {record}{inputs}\n  {description}",
            self.id, self.node_count
        )
    }
}

const SYSTEM: &str = "\
You choose whether a saved workflow already does what a goal asks for.

Return JSON: {\"workflow_id\": str | null, \"errand\": bool, \"why\": str,
              \"inputs\": {name: value}}

- workflow_id: the id of the workflow that does this, or null.
- errand: true only when the goal is one turn of work with no procedure in it.
- why: one line. When you decline, say what is missing — it is read by whoever
  writes the replacement.
- inputs: values for that workflow's declared inputs, taken from the goal. Only
  what the goal actually states; never invent a repository, a path or an id.

Choose one ONLY when it does what the goal asks. A workflow that does something
adjacent is worse than none: it will run to completion and produce confident
work for a job nobody wanted, which costs more than writing a new one.

Prefer a workflow with a record over one without, and weigh both numbers rather
than the ratio — run 40× satisfied 30× is a known quantity, run 1× satisfied 1×
is a coin landing once. A workflow that has never run is still a fair choice
when it plainly matches; it just carries no evidence.

When this episode has already tried something, decline rather than choose a
workflow that would fall short the same way. Being told a second time that the
report has no numbers in it costs a full run and establishes nothing.

Set errand only when there is no procedure in the goal — one turn of work,
answered and finished, with nothing a later goal would want to reuse. Ask
whether you would want this written down and offered as a choice next month.

  errand   \"how much disk is this directory using\"
  errand   \"what did the last commit change\"
  NOT      \"summarise a paper into three bullets\"  — one step, and exactly the
           kind of thing worth having on the shelf
  NOT      \"check the PR and fix whatever CI says\"  — one sentence, many turns

Short is not the test, and a single step is not the test: a one-step procedure
can be the most reused thing here. The test is whether a *procedure* exists.

An errand is not an escape from a hard goal. Anything that needs several turns,
or that could fail in a way worth retrying differently, is not one — say so and
decline instead, so a graph gets written.";

/// Ask whether any candidate does the job, and bind its inputs if one does.
///
/// `Ok(None)` means nothing fitted — the ordinary case on a cold store, and the
/// caller's cue to author. `Ok(Some)` is either a
/// [`Selected`](Approach::Selected) whose graph the caller loads from the store,
/// or an [`Errand`](Approach::Errand) whose graph the caller lowers; both come
/// back with an empty graph, because what fills it is not this function's job.
///
/// `errand_allowed` is false once this episode has already spent its errand —
/// see [`Approach::signature`]. Withholding the option is structural rather
/// than left to the prompt, because "you already tried that" is exactly the
/// instruction a model talks itself out of on attempt three.
///
/// # Errors
/// When inference fails, or the chosen workflow cannot be loaded or bound.
pub async fn select(
    goal: &Goal,
    candidates: &[Candidate],
    past: &str,
    errand_allowed: bool,
    caps: &Capabilities,
    conn: Option<&str>,
) -> Result<Option<Attempt>> {
    // With nothing to choose from and no errand to offer, the answer can only
    // be "none" and asking costs a call to be told so.
    //
    // This used to be unconditional, on that reasoning — and the reasoning
    // stopped holding the moment there was a third answer. A cold store is
    // precisely where a trivial goal is most likely, so short-circuiting here
    // would have made the errand path unreachable exactly where it pays most,
    // while looking correct. The cost is honest and worth stating: a cold-store
    // episode that is *not* an errand now pays one small extra call, against
    // saving a full authoring call and its run whenever it is.
    if candidates.is_empty() && !errand_allowed {
        return Ok(None);
    }

    let shelf = if candidates.is_empty() {
        "# Saved workflows\n(none yet — nothing to choose from, so the only \
         question is whether this is an errand)"
            .to_string()
    } else {
        format!(
            "# Saved workflows\n{}",
            candidates
                .iter()
                .map(Candidate::render)
                .collect::<Vec<_>>()
                .join("\n")
        )
    };
    let spent = if errand_allowed {
        String::new()
    } else {
        "\n\n# This episode has already spent its errand\nOne turn was tried \
         and did not finish the goal, so it is not an errand. Choose a workflow \
         or decline; `errand` will be ignored."
            .to_string()
    };
    let user = format!("# Goal\n{}\n\n{shelf}{past}{spent}", goal.text.trim());

    let answer = ask(caps, conn, Tier::Select, SYSTEM, &user).await?;
    let Some(id) = answer["workflow_id"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
    else {
        // Declining and calling it an errand are different answers, and only
        // one of them skips authoring. Read second so a model that names a
        // workflow *and* sets the flag is taken at its first word — the
        // workflow is the more specific claim, and the more easily checked.
        if errand_allowed && answer["errand"].as_bool().unwrap_or(false) {
            return Ok(Some(Attempt {
                approach: Approach::Errand {
                    why: answer["why"].as_str().unwrap_or_default().to_string(),
                },
                // Lowered by the caller, which is what holds the host facts the
                // one-step graph has to be checked against.
                graph: WorkflowGraph::default(),
                inputs: Map::new(),
                resume: None,
                lessons_shown: Vec::new(),
            }));
        }
        return Ok(None);
    };
    // A model naming something that is not on the list has hallucinated an id;
    // treat it as a decline rather than looking it up, or a typo becomes a
    // store read for a workflow nobody offered.
    if !candidates.iter().any(|c| c.id == id) {
        return Ok(None);
    }

    Ok(Some(Attempt {
        approach: Approach::Selected {
            workflow_id: id.to_string(),
            why: answer["why"].as_str().unwrap_or_default().to_string(),
        },
        graph: WorkflowGraph::default(),
        inputs: inputs_of(&answer),
        // Intake never continues a run: only the loop knows whether the
        // repair it just made is safe to skip a prefix over.
        resume: None,
        // Filled by `decide`, which is what knows what the planner was shown.
        lessons_shown: Vec::new(),
    }))
}

/// Load the chosen workflow and check every declared input has a value.
///
/// Binding is checked here, *after* the model picks and before anything runs.
/// The model is confident about inputs it did not actually find in the goal, so
/// the cheap deterministic check catches what the expensive one asserted.
///
/// The check runs in both directions. A required input the model did not
/// supply is an error. An input the model supplied that the graph never
/// declared is *dropped*: the engine rejects undeclared keys before any node
/// executes, so one invented key — and models invent them freely — would
/// otherwise turn a sound selection into an attempt that ran nothing.
///
/// # Errors
/// When the workflow is gone, or an input has no value.
pub fn bind(attempt: Attempt, store: &dyn WorkflowStore) -> Result<Attempt> {
    let Approach::Selected {
        ref workflow_id, ..
    } = attempt.approach
    else {
        return Ok(attempt);
    };
    let record = store
        .get(workflow_id)
        .map_err(|e| IntakeError::Store(e.to_string()))?
        .ok_or_else(|| IntakeError::Store(format!("workflow {workflow_id} vanished")))?;

    for declared in &record.graph.inputs {
        if !declared.required {
            continue;
        }
        let filled = attempt
            .inputs
            .get(&declared.name)
            .is_some_and(|v| !v.is_null() && v.as_str() != Some(""));
        if !filled {
            return Err(IntakeError::Unbindable {
                id: workflow_id.clone(),
                missing: declared.name.clone(),
            });
        }
    }

    let mut attempt = attempt;
    attempt
        .inputs
        .retain(|name, _| record.graph.inputs.iter().any(|d| d.name == *name));

    Ok(Attempt {
        graph: record.graph,
        ..attempt
    })
}

fn inputs_of(answer: &Value) -> Map<String, Value> {
    answer["inputs"].as_object().cloned().unwrap_or_default()
}

#[cfg(test)]
#[path = "select_tests.rs"]
mod tests;

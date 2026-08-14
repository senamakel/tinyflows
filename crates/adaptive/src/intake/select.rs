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
use crate::contracts::{Approach, Goal};

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
        format!(
            "- id: {}\n  name: {name}\n  steps: {}, {record}\n  {description}",
            self.id, self.node_count
        )
    }
}

const SYSTEM: &str = "\
You choose whether a saved workflow already does what a goal asks for.

Return JSON: {\"workflow_id\": str | null, \"why\": str, \"inputs\": {name: value}}

- workflow_id: the id of the workflow that does this, or null.
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
when it plainly matches; it just carries no evidence.";

/// Ask whether any candidate does the job, and bind its inputs if one does.
///
/// `Ok(None)` means nothing fitted — the ordinary case on a cold store, and the
/// caller's cue to author.
///
/// # Errors
/// When inference fails, or the chosen workflow cannot be loaded or bound.
pub async fn select(
    goal: &Goal,
    candidates: &[Candidate],
    caps: &Capabilities,
    conn: Option<&str>,
) -> Result<Option<Attempt>> {
    // Not a shortcut — a correctness point. With nothing to choose from the
    // answer can only be "none", and asking costs a call to be told so.
    if candidates.is_empty() {
        return Ok(None);
    }

    let listing = candidates
        .iter()
        .map(Candidate::render)
        .collect::<Vec<_>>()
        .join("\n");
    let user = format!(
        "# Goal\n{}\n\n# Saved workflows\n{listing}",
        goal.text.trim()
    );

    let answer = ask(caps, conn, SYSTEM, &user).await?;
    let Some(id) = answer["workflow_id"]
        .as_str()
        .filter(|s| !s.trim().is_empty())
    else {
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
    }))
}

/// Load the chosen workflow and check every declared input has a value.
///
/// Binding is checked here, *after* the model picks and before anything runs.
/// The model is confident about inputs it did not actually find in the goal, so
/// the cheap deterministic check catches what the expensive one asserted.
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

    Ok(Attempt {
        graph: record.graph,
        ..attempt
    })
}

fn inputs_of(answer: &Value) -> Map<String, Value> {
    answer["inputs"].as_object().cloned().unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, applied: u32, helped: u32) -> Candidate {
        Candidate {
            id: id.to_string(),
            name: format!("the {id} workflow"),
            description: "reviews a closed issue end to end".to_string(),
            node_count: 4,
            applied,
            helped,
        }
    }

    #[test]
    fn a_listing_shows_both_counters_not_a_rate() {
        let rendered = candidate("pr-review", 40, 30).render();
        assert!(rendered.contains("run 40×, satisfied 30×"), "{rendered}");
        assert!(!rendered.contains("75"), "a rate hides the sample size");
    }

    #[test]
    fn a_workflow_that_has_never_run_says_so_rather_than_showing_zeroes() {
        let rendered = candidate("fresh", 0, 0).render();
        assert!(rendered.contains("never run"), "{rendered}");
    }

    #[test]
    fn a_workflow_with_no_description_says_it_cannot_be_chosen_on_purpose() {
        let mut c = candidate("bare", 0, 0);
        c.description = String::new();
        assert!(c.render().contains("nobody can choose this on purpose"));
    }

    #[test]
    fn a_blank_name_falls_back_to_the_id() {
        let mut c = candidate("only-an-id", 1, 1);
        c.name = String::new();
        assert!(c.render().contains("name: only-an-id"));
    }

    #[test]
    fn the_prompt_tells_the_model_that_declining_is_allowed() {
        // The single most important line in it: a model pushed to always pick
        // will pick the nearest thing, and a near miss runs to completion.
        assert!(SYSTEM.contains("or null"));
        assert!(SYSTEM.contains("worse than none"));
    }
}

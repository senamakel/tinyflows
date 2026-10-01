//! What a tenant has, for a reader rather than for a planner.
//!
//! [`crate::intake`] builds a catalogue too, and it is a different question.
//! That one answers *what may this attempt choose* — so it drops what is
//! disabled, what this episode already tried, and every family member but the
//! champion. Answering "what does this tenant have" with that view would hide a
//! workflow the moment an episode used it.
//!
//! This one hides nothing and decides nothing. It is the read behind a screen,
//! an audit, or a support question, which is why the standing is reported
//! rather than applied.

use std::sync::Arc;

use tinyflows::store::WorkflowStore;

use crate::intake::{IntakeError, Result};
use crate::ledger::{Ledger, Score};
use crate::promotion::{Standing, standing};

/// One stored workflow, with everything known about it.
#[derive(Debug, Clone)]
pub struct Listing {
    /// The id it is stored and scored under.
    pub id: String,
    /// Display name.
    pub name: String,
    /// What a planner reads to choose it.
    pub description: String,
    /// A rough cost signal.
    pub node_count: usize,
    /// Whether an operator has switched it off. Reported, not filtered: a
    /// disabled workflow is exactly what someone asking this question is often
    /// looking for.
    pub enabled: bool,
    /// Runs and successes, for this tenant.
    pub score: Score,
    /// Where it sits in its family.
    pub standing: Standing,
    /// The workflow it was repaired from, when it was.
    pub parent: Option<String>,
    /// Whether the loop wrote it, rather than a person.
    ///
    /// Read off the id rather than stored, because the alternative is a flag on
    /// `WorkflowRecord` — the engine's type, which an upstream merge would
    /// contend with for a fact only we care about.
    pub learned: bool,
}

/// Every workflow this tenant can see, with its record.
///
/// # Errors
/// When the store or the ledger cannot be read.
pub async fn shelf(store: &Arc<dyn WorkflowStore>, ledger: &dyn Ledger) -> Result<Vec<Listing>> {
    let listed = store
        .list()
        .map_err(|e| IntakeError::Store(e.to_string()))?;

    let mut out = Vec::with_capacity(listed.len());
    for summary in listed {
        let lineage = ledger.lineage(&summary.id).await?;
        let mut family: Vec<(String, Score)> = Vec::with_capacity(lineage.len());
        for id in &lineage {
            family.push((id.clone(), ledger.workflow_score(id).await?));
        }
        let score = family
            .iter()
            .find(|(id, _)| id == &summary.id)
            .map_or_else(Score::default, |(_, score)| *score);

        out.push(Listing {
            standing: standing(&summary.id, &family),
            parent: ledger.parent_of(&summary.id).await?,
            learned: summary.id.starts_with("learned-"),
            score,
            id: summary.id,
            name: summary.name,
            description: summary.description,
            node_count: summary.node_count,
            enabled: summary.enabled,
        });
    }
    Ok(out)
}

#[cfg(test)]
#[path = "inventory_tests.rs"]
mod tests;

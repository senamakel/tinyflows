//! Which incarnation of a flow a run or revision belongs to.
//!
//! The port has no multi-document transaction, so `remove_flow` cannot delete
//! a definition and its dependents atomically, and a run insert or graph
//! update racing the removal can land a dependent after the final sweep — or
//! be cancelled between its write and the check that would undo it. Instead
//! of trying to make that window impossible, a dependent is made meaningless
//! outside it: every definition carries an `incarnation` id minted when the
//! document is first created, every run and revision records the incarnation
//! it was written under (`flow_incarnation`), and readers only ever show a
//! dependent whose flow still exists **with the same incarnation**. An orphan
//! is therefore never visible — not even when a flow with the same id is
//! created again later — and readers reclaim the ones they meet.
//!
//! Documents written before this field existed have neither side, and match
//! each other (`None == None`) until the flow is removed.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;
use tinystoragedrivers_core::{DocumentStore, DocumentStoreExt, ErrorKind, Query, Versioned};

use super::{DEFINITIONS, text};

/// The definition field holding its incarnation id.
pub(crate) const INCARNATION: &str = "incarnation";
/// The run / revision field naming the incarnation it was written under.
pub(crate) const FLOW_INCARNATION: &str = "flow_incarnation";

/// A definition document's incarnation (`None` for a pre-fence document).
pub(crate) fn incarnation(definition: &Value) -> Option<&str> {
    text(definition, INCARNATION)
}

/// Whether `dependent` (a run or revision) belongs to `definition`, the
/// current definition of its flow (`None` when the flow is gone).
pub(crate) fn belongs(dependent: &Value, definition: Option<&Value>) -> bool {
    definition
        .is_some_and(|definition| text(dependent, FLOW_INCARNATION) == incarnation(definition))
}

/// The incarnation of every existing flow, by id.
pub(crate) async fn live_flows(
    docs: &Arc<dyn DocumentStore>,
) -> Result<HashMap<String, Option<String>>> {
    Ok(docs
        .query_all(DEFINITIONS, &Query::all())
        .await?
        .into_iter()
        .map(|stored| {
            let incarnation = incarnation(&stored.doc).map(str::to_string);
            (stored.id, incarnation)
        })
        .collect())
}

/// [`belongs`] against a [`live_flows`] map.
pub(crate) fn belongs_in(dependent: &Value, flows: &HashMap<String, Option<String>>) -> bool {
    text(dependent, "flow_id")
        .and_then(|flow_id| flows.get(flow_id))
        .is_some_and(|live| text(dependent, FLOW_INCARNATION) == live.as_deref())
}

/// Deletes orphans a reader met, each only at the version read. Best effort:
/// an orphan is invisible either way, so a failure is logged, not returned.
pub(crate) async fn reclaim(
    docs: &Arc<dyn DocumentStore>,
    collection: &str,
    orphans: &[Versioned<Value>],
) {
    for orphan in orphans {
        match docs
            .delete(collection, &orphan.id, orphan.unchanged())
            .await
        {
            Ok(_) => tracing::debug!(
                target: "flows",
                collection,
                id = %orphan.id,
                "[flows] reclaimed a record of a removed flow"
            ),
            Err(error) if error.kind() == ErrorKind::Conflict => {}
            Err(error) => tracing::warn!(
                target: "flows",
                collection,
                id = %orphan.id,
                %error,
                "[flows] could not reclaim a record of a removed flow (it stays hidden)"
            ),
        }
    }
}

#[cfg(test)]
#[path = "lineage_tests.rs"]
mod tests;

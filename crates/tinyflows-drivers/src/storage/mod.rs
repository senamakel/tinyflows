//! The storage schema v2 declarations of this crate.
//!
//! - [`storage_schema()`] is owner `flows` (scope class Tenant): the flow
//!   catalog (`flows.definitions`, `.revisions`, `.runs`, `.suggestions`,
//!   `.drafts`) and the library workflow store (feature `store`:
//!   `flows.workflows`, `.workflow_revisions`, `.workflow_runs`,
//!   `.workflow_notes`, `.workflow_proposals`, `.workflow_quarantine`). An
//!   owner may be declared once, so `tinyflows-adaptive` extends this same
//!   schema with `flows.adaptive_*`.
//! - [`cron_storage_schema()`] is owner `cron`: `cron.jobs`, `cron.runs`,
//!   `cron.admission`.
//! - The graph checkpointer is **not** declared here: owner `graph`
//!   (`tinyagents-graph`) declares `graph.{checkpoints,threads,writes}` and
//!   migrates `flows_graph_*` into namespace `flows`.
//! - Key-value state and counters live in `tsd.kv` / `tsd.counters`; a document
//!   step cannot write another owner's collections, so [`import_legacy_kv`] and
//!   [`import_legacy_cron_counters`] move them (host calls them once per scope
//!   after `Registry::migrate`).
//!
//! A host composes `Registry::compose([storage_schema(), cron_storage_schema(),
//! ...])` and runs `Registry::migrate` per scope.

mod kv_import;

use tinyflows_storage::{CollectionSpec, Owner, Schema, ScopeClass};

pub use kv_import::{
    CATALOG_NAMESPACE_PREFIX, KV_OWNER, STATE_NAMESPACE, catalog_namespace, import_legacy_kv,
};

/// The owner name of the flows schema.
pub const FLOWS_OWNER: &str = "flows";

/// An empty schema for owner `flows`, for the feature-gated pieces to extend.
#[allow(clippy::expect_used, reason = "`flows` is a valid owner name")]
fn flows_schema() -> Schema {
    Schema::new(Owner::new(FLOWS_OWNER).expect("`flows` is a valid owner"))
}

/// The `flows` owner's declarations and v1 -> v2 steps for the features this
/// build has. `tinyflows-adaptive` extends the result with its collections
/// (`tinyflows_adaptive::drivers::extend_storage_schema`): an owner may be
/// declared only once, so the host passes one schema through both.
#[must_use]
pub fn storage_schema() -> Schema {
    #[allow(unused_mut)]
    let mut schema = flows_schema();
    #[cfg(feature = "catalog")]
    {
        schema = crate::catalog::extend_schema(schema);
    }
    #[cfg(feature = "store")]
    {
        schema = crate::store::extend_schema(schema);
    }
    schema
}

/// Collections owned by TinyFlows drivers for host bootstrap and docs.
#[must_use]
pub fn collections() -> Vec<CollectionSpec> {
    let mut specs = storage_schema().collections();
    #[cfg(feature = "schedule")]
    specs.extend(cron_storage_schema().collections());
    specs.extend(tinyflows_storage::primitives::collection_specs());
    specs
}

/// The catalog-only portion of owner `flows` and its v1 -> v2 steps.
///
/// Unlike [`storage_schema`], this excludes workflow-store schemas so the
/// catalog module can migrate only the collections behind its own callback.
#[cfg(feature = "catalog")]
#[must_use]
pub fn catalog_storage_schema() -> Schema {
    crate::catalog::extend_schema(flows_schema())
}

/// The `cron` owner's declarations and v1 -> v2 steps.
#[cfg(feature = "schedule")]
#[must_use]
pub fn cron_storage_schema() -> Schema {
    crate::schedule::storage_schema()
}

#[cfg(feature = "schedule")]
pub use crate::schedule::import_legacy_cron_counters;

//! The ledger and the vault over a `tinystoragedrivers` document port.
//!
//! A host that has opened a storage backend (SQLite on a desktop, MongoDB in
//! the cloud, memory in tests) hands [`Storage::from_documents`] a document
//! handle — usually one already bound to a storage scope — and gets the whole
//! adaptive persistence stack on it, scoped with the same
//! [`Storage::for_tenant`](crate::storage::Storage::for_tenant) as every other
//! backend.
//!
//! [`Storage::from_documents`]: crate::storage::Storage::from_documents
//!
//! # Tenancy
//!
//! Exactly the Mongo backend's model, which this module is a port of: every
//! record carries its bucket (`scope_key`, `""` for global) as a field, writes
//! go to the handle's bucket, reads return the handle's bucket plus global.
//! The storage scope of the document handle sits *outside* that — a host
//! serving several agents gives each one its own handle, and the adaptive
//! tenants live inside it.
//!
//! # Layout (storage schema v2, owner `flows`)
//!
//! | Collection | Document id | Holds |
//! | --- | --- | --- |
//! | `flows.adaptive_rows` | `ldg_<seq>` | one attempt |
//! | `flows.adaptive_lessons` | `les_<seq>` | one lesson and its counters |
//! | `flows.adaptive_evidence` | `Key::of([lesson, row])` | one citation |
//! | `flows.adaptive_scores` | `Key::of([scope_key, workflow])` | one workflow's counters |
//! | `flows.adaptive_variants` | `Key::of([scope_key, variant])` | one lineage edge |
//! | `flows.adaptive_episodes` | `Key::of([scope_key, episode])` | one episode |
//! | `flows.adaptive_steps` | `Key::of([scope_key, row, seq])` | one step of an attempt |
//! | `flows.adaptive_workflows` | `Key::of([scope_key, workflow])` | one vault record |
//!
//! Bodies are bare snake_case objects with the tenant bucket as `scope_key`
//! (and as a key part where the record is per bucket); every instant is an
//! epoch-millisecond `*_ms` field. The record types keep their RFC 3339
//! strings; the conversion is in `convert`. The id sequences of rows and
//! lessons are `tsd.counters` (owner `flows`, names `adaptive_rows` and
//! `adaptive_lessons`), so concurrent loops writing one ledger never lose an
//! increment or share an id. First-write-wins edges (`$setOnInsert` in Mongo)
//! are insert-only writes.
//!
//! [`storage_schema`] declares the collections and their v1 -> v2 steps;
//! `Storage::from_documents` runs those steps and carries the v1 sequences
//! through [`import_legacy_adaptive_counters`] before returning the stores.

mod convert;
mod ledger;
mod migrate;
mod vault;

use tinyflows_storage::{CollectionSpec, IndexSpec, StorageError};

pub use ledger::DriverLedger;
pub use migrate::{collections, import_legacy_adaptive_counters, storage_schema};
pub use vault::DriverVault;

/// Compare-and-swap attempts before a contended counter update gives up.
const CAS_ATTEMPTS: usize = 64;

/// The `tsd.counters` owner of the id sequences.
pub(crate) const COUNTER_OWNER: &str = "flows";

/// The v1 sequence collection (`import_legacy_adaptive_counters` reads it).
pub(crate) const COUNTERS: &str = "adaptive_counters";
pub(crate) const ROWS: &str = "flows.adaptive_rows";
pub(crate) const LESSONS: &str = "flows.adaptive_lessons";
pub(crate) const EVIDENCE: &str = "flows.adaptive_evidence";
pub(crate) const SCORES: &str = "flows.adaptive_scores";
pub(crate) const VARIANTS: &str = "flows.adaptive_variants";
pub(crate) const EPISODES: &str = "flows.adaptive_episodes";
pub(crate) const STEPS: &str = "flows.adaptive_steps";
pub(crate) const WORKFLOWS: &str = "flows.adaptive_workflows";

/// The body version of every collection here (`adaptive_*` was version 1).
pub(crate) const SCHEMA_VERSION: u32 = 2;

/// Every collection the stores use, with its indexes.
pub(crate) fn indexed_collection_specs() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(ROWS).index(IndexSpec::new(
            "by_episode",
            ["scope_key", "episode", "seq"],
        )),
        CollectionSpec::new(LESSONS).index(IndexSpec::new("by_scope", ["scope_key", "seq"])),
        CollectionSpec::new(EVIDENCE).index(IndexSpec::new("by_lesson", ["lesson_id"])),
        CollectionSpec::new(SCORES),
        CollectionSpec::new(VARIANTS).index(IndexSpec::new("by_parent", ["scope_key", "parent"])),
        CollectionSpec::new(EPISODES)
            .index(IndexSpec::new("by_scope", ["scope_key", "updated_ms"])),
        CollectionSpec::new(STEPS).index(IndexSpec::new("by_row", ["scope_key", "row_id", "seq"])),
        CollectionSpec::new(WORKFLOWS)
            .index(IndexSpec::new("by_scope", ["scope_key", "workflow_id"])),
    ]
}

/// A document id for `parts` (`Key::of`: escaped so no two tuples collide,
/// hashed when it would exceed the driver's id limit).
fn key(parts: &[&str]) -> Result<String, StorageError> {
    tinyflows_storage::Key::of(parts.iter().copied()).map(|key| key.as_str().to_owned())
}

/// Whether `error` means another writer got there first.
fn is_race(error: &StorageError) -> bool {
    error.kind() == tinyflows_storage::ErrorKind::Conflict
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

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
//! # Layout
//!
//! | Collection | Document id | Holds |
//! | --- | --- | --- |
//! | `adaptive_counters` | sequence name | the next id of rows and lessons |
//! | `adaptive_rows` | `ldg_<seq>` | one attempt |
//! | `adaptive_lessons` | `les_<seq>` | one lesson and its counters |
//! | `adaptive_evidence` | `(lesson, row)` | one citation |
//! | `adaptive_scores` | `(bucket, workflow)` | one workflow's counters |
//! | `adaptive_variants` | `(bucket, variant)` | one lineage edge |
//! | `adaptive_episodes` | `(bucket, episode)` | one episode |
//! | `adaptive_steps` | `(bucket, row)` | one attempt's steps |
//! | `adaptive_workflows` | `(bucket, workflow)` | one vault record |
//!
//! Counters (`$inc` in Mongo) and first-write-wins edges (`$setOnInsert`) are
//! compare-and-swap and insert-only writes here, so concurrent loops writing
//! one ledger never lose an increment.

mod ledger;
mod vault;

use sha2::{Digest, Sha256};
use tinystoragedrivers_core::StorageError;

pub use ledger::DriverLedger;
pub use vault::DriverVault;

/// Compare-and-swap attempts before a contended counter update gives up.
const CAS_ATTEMPTS: usize = 64;

/// Longest id stored as is; longer ones are hashed.
const MAX_KEY_LEN: usize = 400;

/// A document id for `parts`: length-prefixed so no two tuples collide, and
/// replaced by its SHA-256 when it would exceed the driver's id limit.
fn key(parts: &[&str]) -> String {
    let joined: String = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<Vec<_>>()
        .join("/");
    if joined.len() <= MAX_KEY_LEN {
        joined
    } else {
        let digest = Sha256::digest(joined.as_bytes());
        let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        format!("h:{hex}")
    }
}

/// Whether `error` means another writer got there first.
fn is_race(error: &StorageError) -> bool {
    error.kind() == tinystoragedrivers_core::ErrorKind::Conflict
}

#[cfg(test)]
#[path = "mod_tests.rs"]
mod tests;

//! A workflow store for tests and examples: the engine's `WorkflowStore` on an
//! in-memory `tinystoragedrivers` backend, so every case starts empty and
//! nothing touches the filesystem.
//!
//! Included with `#[path]` by each test crate (and the unit tests and the
//! example) that needs one, since integration tests cannot share a module.

#![allow(dead_code)]

use std::sync::Arc;

use tinyflows::store::WorkflowStore;
use tinyflows_drivers::store::DriverWorkflowStore;
use tinyflows_storage::{Scope, StorageBackend};
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

/// An empty store over its own in-memory backend.
pub fn empty() -> DriverWorkflowStore {
    let scoped = MemoryStorage::new()
        .for_scope(&Scope::new("local").expect("valid scope"))
        .expect("a local scope");
    DriverWorkflowStore::new(scoped.documents().clone())
}

/// [`empty`], behind the engine's trait object.
pub fn shared() -> Arc<dyn WorkflowStore> {
    Arc::new(empty())
}

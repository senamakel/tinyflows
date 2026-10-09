//! `tinyflows` run state and graph checkpoints on
//! [tinystoragedrivers](https://github.com/tinyhumansai/tinystoragedrivers)
//! document ports.
//!
//! A host that has opened a storage backend (SQLite on a desktop, MongoDB in
//! the cloud, memory in tests) hands these a document handle — usually one
//! already bound to a tenant scope — and plugs them into the engine:
//!
//! - [`DriverStateStore`] is the engine's [`StateStore`] capability
//!   (`Capabilities::state`), one document per key.
//! - [`DriverCheckpointer`] is a graph [`Checkpointer`]: checkpoints, pending
//!   writes, namespace-scoped reads and a bulk `state_history`, for
//!   `engine::run_with_checkpointer` / `resume_with_checkpointer`.
//!
//! The default collections (`flows_state`, `flows_graph_*`) stay apart from
//! TinyAgents' own checkpointer collections, so both can share one scoped
//! backend.
//!
//! [`StateStore`]: tinyflows::caps::StateStore
//! [`Checkpointer`]: tinyflows::graph::Checkpointer

mod checkpoint;
mod state_store;

pub use checkpoint::{DEFAULT_PREFIX, DriverCheckpointer};
pub use state_store::DriverStateStore;

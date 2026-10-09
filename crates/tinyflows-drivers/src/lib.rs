//! `tinyflows` run state and graph checkpoints on
//! [tinystoragedrivers](https://github.com/tinyhumansai/tinystoragedrivers)
//! document ports.
//!
//! A host that has opened a storage backend (SQLite on a desktop, MongoDB in
//! the cloud, memory in tests) hands these a document handle — usually one
//! already bound to a tenant scope — and plugs them into the engine:
//!
//! - `DriverStateStore` is the engine's [`StateStore`] capability
//!   (`Capabilities::state`), one document per key.
//! - `DriverCheckpointer` is a graph [`Checkpointer`]: checkpoints, pending
//!   writes, namespace-scoped reads and a bulk `state_history`, for
//!   `engine::run_with_checkpointer` / `resume_with_checkpointer`.
//!
//! - `catalog` (feature `catalog`) is the flow catalog and authoring
//!   drafts: `catalog::FlowCatalogDocuments`, the document-port counterpart
//!   of `tinyflows-sqlite`'s `flows` and `drafts`, and
//!   `catalog::FlowStateDocuments`, one flow's namespaced state.
//! - `schedule::CronDocuments` (feature `schedule`) is the cron job and
//!   run-history store, the document-port twin of `tinyflows_sqlite::schedule`.
//!   It needs only `tinyflows-schedule`, not the engine.
//!
//! The default collections (`flows_state`, `flows_graph_*`) stay apart from
//! TinyAgents' own checkpointer collections, so both can share one scoped
//! backend.
//!
//! The items above are behind Cargo features, so they are named rather than
//! linked: a link would not resolve in a build without its feature.
//!
//! [`StateStore`]: https://docs.rs/tinyflows/latest/tinyflows/caps/trait.StateStore.html
//! [`Checkpointer`]: https://docs.rs/tinyflows/latest/tinyflows/graph/checkpoint/trait.Checkpointer.html

#[cfg(feature = "catalog")]
pub mod catalog;
#[cfg(feature = "engine")]
mod checkpoint;
#[cfg(feature = "engine")]
mod checkpoint_keys;
#[cfg(feature = "schedule")]
pub mod schedule;
#[cfg(feature = "engine")]
mod state_store;

#[cfg(feature = "engine")]
pub use checkpoint::{DEFAULT_PREFIX, DriverCheckpointer};
#[cfg(feature = "engine")]
pub use state_store::DriverStateStore;

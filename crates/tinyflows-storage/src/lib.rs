//! TinyFlows-owned storage helpers built on the public storage ports.
//!
//! These helpers contain record and migration behavior owned by TinyFlows;
//! backend selection stays with the host.

pub use tinystoragedrivers_api::{DocumentStore, DocumentStoreExt, async_trait};
pub use tinystoragedrivers_types::*;
mod key;
pub use key::Key;
pub use primitives::{Acquire, Acquisition, Counters, Kv, Lease, LeaseRecord, Leases, Repo, RetryPolicy};

pub(crate) type Clock = std::sync::Arc<dyn Fn() -> u64 + Send + Sync>;

pub(crate) fn system_clock() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
}

pub mod primitives {
    use std::sync::Arc;

    use tinystoragedrivers_types::{CollectionSpec, IndexSpec, Result};

    mod counters;
    mod kv;
    mod leases;
    mod repo;

    pub use counters::Counters;
    pub use kv::Kv;
    pub use leases::{Acquire, Acquisition, Lease, LeaseRecord, Leases};
    pub use repo::{Repo, RetryPolicy};

    pub const LEASES: &str = "tsd.leases";
    pub const KV: &str = "tsd.kv";
    pub const COUNTERS: &str = "tsd.counters";

    #[must_use]
    pub fn collection_specs() -> Vec<CollectionSpec> {
        vec![
            CollectionSpec::new(LEASES),
            CollectionSpec::new(KV).index(IndexSpec::new("kv_by_key", ["owner", "ns", "key"])),
            CollectionSpec::new(COUNTERS),
        ]
    }

    pub async fn ensure(docs: &Arc<dyn crate::DocumentStore>) -> Result<()> {
        for spec in collection_specs() {
            docs.ensure_collection(&spec).await?;
        }
        Ok(())
    }
}

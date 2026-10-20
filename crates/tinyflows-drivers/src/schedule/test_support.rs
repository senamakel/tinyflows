//! Fixtures the schedule tests share.

use std::sync::Arc;
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;

use tinyflows_schedule::{JobOrigin, Schedule};
use tinyflows_storage::{DocumentStore, Scope, StorageBackend};

use super::CronDocuments;

/// `storage`'s documents under `scope`.
pub(super) fn docs_in(storage: &MemoryStorage, scope: &str) -> Arc<dyn DocumentStore> {
    Arc::clone(
        storage
            .for_scope(&Scope::new(scope).unwrap())
            .unwrap()
            .documents(),
    )
}

/// A store over a fresh in-memory backend.
pub(super) fn store() -> CronDocuments {
    CronDocuments::new(docs_in(&MemoryStorage::new(), "local"))
}

/// Every day at 09:00 UTC.
pub(super) fn daily() -> Schedule {
    Schedule::Cron {
        expr: "0 9 * * *".into(),
        tz: None,
        active_hours: None,
    }
}

pub(super) fn channel_origin() -> JobOrigin {
    JobOrigin::Channel {
        channel: "telegram".into(),
        reply_target: "chat-42".into(),
        history_key: "telegram:chat-42".into(),
        sender: Some("alice".into()),
        thread_id: None,
    }
}

/// A forwarding driver with controlled capability and transaction failures.
#[derive(Debug)]
pub(super) struct FaultDocuments {
    pub inner: Arc<dyn DocumentStore>,
    pub transactions: bool,
    pub fail_batch: std::sync::atomic::AtomicBool,
    pub batch_barrier: std::sync::Mutex<Option<Arc<tokio::sync::Barrier>>>,
    pub faults: Faults,
}

/// Failures a test arms on a [`FaultDocuments`].
#[derive(Debug, Default)]
pub(super) struct Faults {
    /// Writes (`put` and `delete`) to `tsd.leases` fail while set.
    pub lease_writes: std::sync::atomic::AtomicBool,
    /// Reads of `cron.jobs` are counted; the one numbered like this (from 1)
    /// fails. `0` fails none.
    pub fail_job_read: std::sync::atomic::AtomicUsize,
    pub job_reads: std::sync::atomic::AtomicUsize,
}

impl FaultDocuments {
    fn lease_write_fault(&self, collection: &str) -> tinyflows_storage::Result<()> {
        if collection == "tsd.leases"
            && self
                .faults
                .lease_writes
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(tinyflows_storage::StorageError::unavailable(
                "fixture refuses lease writes",
            ));
        }
        Ok(())
    }
}
#[async_trait::async_trait]
impl DocumentStore for FaultDocuments {
    fn capabilities(&self) -> tinyflows_storage::Capabilities {
        if self.transactions {
            self.inner.capabilities()
        } else {
            self.inner
                .capabilities()
                .without(tinyflows_storage::Capability::Transactions)
        }
    }
    async fn ensure_collection(
        &self,
        spec: &tinyflows_storage::CollectionSpec,
    ) -> tinyflows_storage::Result<()> {
        self.inner.ensure_collection(spec).await
    }
    async fn get(
        &self,
        c: &str,
        id: &str,
    ) -> tinyflows_storage::Result<Option<tinyflows_storage::Versioned<serde_json::Value>>> {
        if c == "cron.jobs" {
            let read = self
                .faults
                .job_reads
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            if read
                == self
                    .faults
                    .fail_job_read
                    .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(tinyflows_storage::StorageError::unavailable(
                    "fixture refuses this job read",
                ));
            }
        }
        self.inner.get(c, id).await
    }
    async fn put(
        &self,
        c: &str,
        id: &str,
        doc: serde_json::Value,
        p: tinyflows_storage::Precondition,
    ) -> tinyflows_storage::Result<tinyflows_storage::Version> {
        self.lease_write_fault(c)?;
        self.inner.put(c, id, doc, p).await
    }
    async fn put_with(
        &self,
        c: &str,
        id: &str,
        doc: serde_json::Value,
        p: tinyflows_storage::Precondition,
        options: tinyflows_storage::WriteOptions,
    ) -> tinyflows_storage::Result<tinyflows_storage::Version> {
        self.lease_write_fault(c)?;
        self.inner.put_with(c, id, doc, p, options).await
    }
    async fn delete(
        &self,
        c: &str,
        id: &str,
        p: tinyflows_storage::Precondition,
    ) -> tinyflows_storage::Result<bool> {
        self.lease_write_fault(c)?;
        self.inner.delete(c, id, p).await
    }
    async fn query(
        &self,
        c: &str,
        q: &tinyflows_storage::Query,
    ) -> tinyflows_storage::Result<
        tinyflows_storage::Page<tinyflows_storage::Versioned<serde_json::Value>>,
    > {
        self.inner.query(c, q).await
    }
    async fn count(
        &self,
        c: &str,
        f: &tinyflows_storage::Filter,
    ) -> tinyflows_storage::Result<u64> {
        self.inner.count(c, f).await
    }
    async fn delete_where(
        &self,
        c: &str,
        f: &tinyflows_storage::Filter,
    ) -> tinyflows_storage::Result<u64> {
        self.inner.delete_where(c, f).await
    }
    async fn claim(
        &self,
        c: &str,
        f: &tinyflows_storage::Filter,
        s: &[tinyflows_storage::Sort],
        p: &serde_json::Value,
    ) -> tinyflows_storage::Result<Option<tinyflows_storage::Versioned<serde_json::Value>>> {
        self.inner.claim(c, f, s, p).await
    }
    async fn drop_collection(&self, c: &str) -> tinyflows_storage::Result<()> {
        self.inner.drop_collection(c).await
    }
    async fn atomic_batch(
        &self,
        ops: Vec<tinyflows_storage::WriteOp>,
    ) -> tinyflows_storage::Result<Vec<tinyflows_storage::WriteResult>> {
        let barrier = self.batch_barrier.lock().unwrap().take();
        if let Some(barrier) = barrier {
            barrier.wait().await;
        }
        if self.fail_batch.load(std::sync::atomic::Ordering::SeqCst) || !self.transactions {
            return Err(tinyflows_storage::StorageError::unsupported(
                tinyflows_storage::Capability::Transactions,
                "fixture refuses transaction",
            ));
        }
        self.inner.atomic_batch(ops).await
    }
}

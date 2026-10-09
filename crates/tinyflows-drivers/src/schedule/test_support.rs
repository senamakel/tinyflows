//! Fixtures the schedule tests share.

use std::sync::Arc;

use tinyflows_schedule::{JobOrigin, Schedule};
use tinystoragedrivers_core::{DocumentStore, MemoryStorage, Scope, StorageBackend};

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

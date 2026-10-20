use super::*;
use tinystoragedrivers_testkit::MemoryBackend as MemoryStorage;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::time::Duration;
use tinyflows_schedule::runtime::*;
use tinyflows_schedule::{DeliveryStatus, Schedule};
use tinyflows_storage::{ Scope, StorageBackend};
use tokio::sync::Notify;

struct Replacement(CronDocuments);
#[async_trait]
impl JobExecutor for Replacement {
    async fn execute(&self, job: &CronJob, _: &str) -> Result<Attempt> {
        self.0.remove_job(&job.id).await?;
        let replacement = self
            .0
            .add_flow_schedule_job("history-fence", job.schedule.clone())
            .await?;
        assert_eq!(job.id, replacement.id);
        assert_ne!(job.created_at, replacement.created_at);
        Ok(Attempt {
            success: true,
            output: "old incarnation".into(),
            retryable: false,
            raw_detail: None,
        })
    }
}
#[async_trait]
impl RunDelivery for Replacement {
    async fn send(&self, _: &CronJob, _: &str, _: &Attempt) -> Result<DeliveryStatus> {
        Ok(DeliveryStatus::NotRequested)
    }
    async fn deliver(&self, _: &tinyflows_bus::schedule::DeliveryProjection) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl TriggerSink for Replacement {
    async fn schedule(&self, job: &CronJob, run_id: &str) -> Result<Attempt> {
        self.execute(job, run_id).await
    }
}
#[tokio::test]
async fn old_run_history_does_not_attach_to_a_recreated_flow_job() {
    let backend = MemoryStorage::new();
    let scoped = backend
        .for_scope(&Scope::new("history-fence").unwrap())
        .unwrap();
    let documents = CronDocuments::new(scoped.documents().clone());
    let mut job = documents
        .add_flow_schedule_job("history-fence", Schedule::Every { every_ms: 60_000 })
        .await
        .unwrap();
    let due_at = Utc::now() - chrono::Duration::seconds(1);
    let docs = scoped.documents().clone();
    let stored = docs.get("cron.jobs", &job.id).await.unwrap().unwrap();
    let mut body = stored.doc.clone();
    body["next_run_ms"] = serde_json::json!(due_at.timestamp_millis());
    docs.put("cron.jobs", &job.id, body, stored.unchanged())
        .await
        .unwrap();
    job = documents.get_job(&job.id).await.unwrap();
    let io = Arc::new(Replacement(documents.clone()));
    let ports = RuntimePorts {
        store: Arc::new(Store(documents.clone())),
        executor: io.clone(),
        delivery: io.clone(),
        triggers: io,
        clock: Arc::new(SystemClock),
    };
    let mut scheduler = Scheduler::new("history-fence", SchedulerConfig::default(), ports);
    scheduler.dispatch(vec![job.clone()]).await.unwrap();
    let error = scheduler.drain().await.unwrap_err();
    assert!(
        error.to_string().contains("lost the occurrence"),
        "{error:#}"
    );
    assert!(
        documents.list_runs(&job.id, 10).await.unwrap().is_empty(),
        "old execution contaminated the replacement job history"
    );
}

struct HeldRun {
    started: Notify,
}
#[async_trait]
impl JobExecutor for HeldRun {
    async fn execute(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        self.started.notify_one();
        std::future::pending().await
    }
}

struct FrozenClock(std::sync::Mutex<DateTime<Utc>>);
#[async_trait]
impl Clock for FrozenClock {
    fn now(&self) -> DateTime<Utc> {
        *self.0.lock().unwrap()
    }
    async fn sleep(&self, _: Duration) {
        std::future::pending().await
    }
}

struct NoDelivery;
#[async_trait]
impl RunDelivery for NoDelivery {
    async fn send(&self, _: &CronJob, _: &str, _: &Attempt) -> Result<DeliveryStatus> {
        Ok(DeliveryStatus::NotRequested)
    }
    async fn deliver(&self, _: &tinyflows_bus::schedule::DeliveryProjection) -> Result<()> {
        Ok(())
    }
}
#[async_trait]
impl TriggerSink for NoDelivery {
    async fn schedule(&self, _: &CronJob, _: &str) -> Result<Attempt> {
        anyhow::bail!("not used")
    }
}

struct LegacyClaimSlotStore(Store);
#[async_trait]
impl JobStore for LegacyClaimSlotStore {
    async fn due_jobs(&self, now: DateTime<Utc>) -> Result<Vec<CronJob>> {
        self.0.due_jobs(now).await
    }
    async fn due_jobs_excluding(
        &self,
        now: DateTime<Utc>,
        excluded: &[String],
    ) -> Result<Vec<CronJob>> {
        self.0.due_jobs_excluding(now, excluded).await
    }
    async fn claim_due_occurrence(
        &self,
        job: &CronJob,
        owner: &str,
        now: DateTime<Utc>,
        _: chrono::Duration,
    ) -> Result<Option<tinyflows_bus::schedule::OccurrenceClaim>> {
        let next_run = if matches!(job.schedule, Schedule::At { .. }) {
            job.next_run
        } else {
            tinyflows_schedule::next_run_for_schedule(&job.schedule, now)?
        };
        if !self.0.0.claim_slot(job, next_run).await? {
            return Ok(None);
        }
        Ok(Some(tinyflows_bus::schedule::OccurrenceClaim {
            receipt: "legacy-claim-slot-control".into(),
            job: job.clone(),
            occurrence: job.next_run,
            started_at: now,
            owner: owner.into(),
            run_id: None,
        }))
    }
    async fn renew_occurrence(
        &self,
        _: &tinyflows_bus::schedule::OccurrenceClaim,
        _: DateTime<Utc>,
        _: chrono::Duration,
    ) -> Result<bool> {
        Ok(true)
    }
}

async fn interrupted_occurrence_recovery_count(schedule: Schedule, legacy: bool) -> usize {
    // Stored instants have millisecond precision, so the in-memory job must too.
    let now = DateTime::from_timestamp_millis(
        (Utc::now() - chrono::Duration::seconds(60)).timestamp_millis(),
    )
    .unwrap();
    let backend = MemoryStorage::new();
    let scoped = backend
        .for_scope(&Scope::new("lease-recovery").unwrap())
        .unwrap();
    let documents = CronDocuments::new(scoped.documents().clone());
    let mut job = documents
        .add_shell_job(Some("lease-recovery".into()), schedule, "true")
        .await
        .unwrap();
    job.next_run = now;
    let docs = scoped.documents().clone();
    let stored = docs.get("cron.jobs", &job.id).await.unwrap().unwrap();
    let mut body = stored.doc.clone();
    body["next_run_ms"] = serde_json::json!(now.timestamp_millis());
    docs.put("cron.jobs", &job.id, body, stored.unchanged())
        .await
        .unwrap();
    let frozen = Arc::new(FrozenClock(std::sync::Mutex::new(now)));
    let held = Arc::new(HeldRun {
        started: Notify::new(),
    });
    let store: Arc<dyn JobStore> = if legacy {
        Arc::new(LegacyClaimSlotStore(Store(documents.clone())))
    } else {
        Arc::new(Store(documents.clone()))
    };
    let ports = RuntimePorts {
        store,
        executor: held.clone(),
        delivery: Arc::new(NoDelivery),
        triggers: Arc::new(NoDelivery),
        clock: frozen.clone(),
    };
    let mut first = Scheduler::new("lease-recovery", SchedulerConfig::default(), ports.clone());
    assert_eq!(first.dispatch(vec![job.clone()]).await.unwrap(), 1);
    held.started.notified().await;
    drop(first);
    *frozen.0.lock().unwrap() = now + chrono::Duration::hours(3);

    let mut restarted = Scheduler::new("lease-recovery", SchedulerConfig::default(), ports);
    let recovered = restarted.tick().await.unwrap();
    restarted.shutdown().await;
    recovered
}

#[tokio::test]
async fn interrupted_recurring_occurrence_is_recovered_before_next_recurrence() {
    assert_eq!(
        interrupted_occurrence_recovery_count(
            Schedule::Every {
                every_ms: 24 * 60 * 60 * 1_000,
            },
            false,
        )
        .await,
        1,
        "the interrupted occurrence must still be due"
    );
}

#[tokio::test]
async fn interrupted_one_shot_occurrence_is_recovered_after_restart() {
    assert_eq!(
        interrupted_occurrence_recovery_count(
            Schedule::At {
                at: Utc::now() + chrono::Duration::hours(1),
            },
            false,
        )
        .await,
        1,
        "the interrupted one-shot must still be due"
    );
}

#[tokio::test]
async fn legacy_scheduler_advance_before_execution_fails_the_same_recovery_assertion() {
    assert_eq!(
        interrupted_occurrence_recovery_count(
            Schedule::Every {
                every_ms: 24 * 60 * 60 * 1_000,
            },
            true,
        )
        .await,
        0,
        "the legacy Scheduler claim_slot path advances the same recurrence away before execution"
    );
    assert_eq!(
        interrupted_occurrence_recovery_count(
            Schedule::At {
                at: Utc::now() + chrono::Duration::hours(1),
            },
            true,
        )
        .await,
        0,
        "the legacy Scheduler claim_slot path disables the same one-shot before execution"
    );
}

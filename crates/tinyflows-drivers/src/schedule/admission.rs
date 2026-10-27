//! Shared admission state, transacted with job writes to fence policy races.
use super::codec::{doc_to_job, incarnation};
use super::{ADMISSION, CAS_ATTEMPTS, CronDocuments, JOBS, storage_error};
use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tinyflows_storage::{
    Capability, DocumentStoreExt, ErrorKind, Precondition, Query, Versioned, WriteOp, WriteResult,
};

/// The id of the ledger document in `cron.admission`.
pub(super) const LEDGER: &str = "ledger";

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    job_id: String,
    incarnation: Option<String>,
    enabled: bool,
}
#[derive(Default)]
struct Admission {
    limit: Option<usize>,
    jobs: std::collections::BTreeMap<String, Member>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AdmissionRecord {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
    // Job ids can contain Mongo-sensitive key characters, so the members are
    // a list carrying their id, not an object keyed by it.
    members: Vec<Member>,
}
fn decode(doc: Value) -> Result<Admission> {
    let record: AdmissionRecord = serde_json::from_value(doc)?;
    Ok(Admission {
        limit: record.limit,
        jobs: record
            .members
            .into_iter()
            .map(|member| (member.job_id.clone(), member))
            .collect(),
    })
}
fn encode(state: Admission) -> Result<Value> {
    Ok(serde_json::to_value(AdmissionRecord {
        limit: state.limit,
        members: state.jobs.into_values().collect(),
    })?)
}
impl Admission {
    fn check(&self) -> Result<()> {
        if let Some(limit) = self.limit
            && (limit == 0 || self.jobs.values().filter(|member| member.enabled).count() > limit)
        {
            bail!("cron store: active job limit exceeded");
        }
        Ok(())
    }
}
fn member(id: &str, doc: &Value) -> Result<Member> {
    Ok(Member {
        job_id: id.to_owned(),
        incarnation: incarnation(doc).map(str::to_owned),
        enabled: match doc.get("enabled") {
            None => false,
            Some(Value::Bool(enabled)) => *enabled,
            Some(_) => return Err(anyhow!("cron store: invalid enabled state")),
        },
    })
}

impl CronDocuments {
    /// Installs or removes an operator-owned, scope-wide enabled-job cap.
    /// Existing handles observe the stored policy. Installation refuses a cap
    /// below the existing count. Installation reconciles authoritative rows,
    /// fencing the snapshot and policy in one transaction. Stop legacy writers
    /// before adoption. All participating writers need atomic batches;
    /// unsupported backends fail closed, without a local fallback.
    ///
    /// # Errors
    /// Zero or exceeded limit, malformed admission state, unavailable atomic
    /// batches, exhausted retries, or backend failure.
    pub async fn set_max_active_jobs(&self, limit: Option<usize>) -> Result<()> {
        self.docs
            .capabilities()
            .require(Capability::Transactions)
            .map_err(storage_error)?;
        self.ensure().await?;
        for _ in 0..CAS_ATTEMPTS {
            let stored = self.admission().await?;
            let mut state: Admission = decode(stored.doc.clone())?;
            state.limit = limit;
            // Reconcile authoritative rows at policy adoption. An older writer
            // may have added/deleted a row before the deployment stopped it.
            // Fencing every row and the ledger makes this snapshot safe against
            // all upgraded writers, including inserts absent from the snapshot.
            let rows = self
                .docs
                .query_all(JOBS, &Query::all())
                .await
                .map_err(storage_error)?;
            state.jobs.clear();
            let mut ops = Vec::new();
            for row in rows {
                doc_to_job(&row)?;
                state
                    .jobs
                    .insert(row.id.clone(), member(&row.id, &row.doc)?);
                ops.push(WriteOp::Put {
                    collection: JOBS.into(),
                    id: row.id.clone(),
                    doc: row.doc.clone(),
                    precondition: row.unchanged(),
                });
            }
            state.check()?;
            ops.push(WriteOp::Put {
                collection: ADMISSION.into(),
                id: LEDGER.into(),
                doc: encode(state)?,
                precondition: stored.unchanged(),
            });
            match self.docs.atomic_batch(ops).await {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
        bail!("cron store: admission policy kept changing")
    }

    /// Initializes authoritative membership from legacy rows, fencing every
    /// snapshot row and the absent ledger in one transaction. Upgraded writers
    /// all consult the ledger, including those without a configured cap.
    async fn admission(&self) -> Result<Versioned<Value>> {
        for _ in 0..CAS_ATTEMPTS {
            if let Some(stored) = self
                .docs
                .get(ADMISSION, LEDGER)
                .await
                .map_err(storage_error)?
            {
                let state: Admission = decode(stored.doc.clone())?;
                state.check()?;
                return Ok(stored);
            }
            let rows = self
                .docs
                .query_all(JOBS, &Query::all())
                .await
                .map_err(storage_error)?;
            let mut state = Admission::default();
            let mut ops = Vec::new();
            for row in rows {
                doc_to_job(&row)?;
                state
                    .jobs
                    .insert(row.id.clone(), member(&row.id, &row.doc)?);
                ops.push(WriteOp::Put {
                    collection: JOBS.into(),
                    id: row.id.clone(),
                    doc: row.doc.clone(),
                    precondition: row.unchanged(),
                });
            }
            ops.push(WriteOp::Put {
                collection: ADMISSION.into(),
                id: LEDGER.into(),
                doc: encode(state)?,
                precondition: Precondition::Absent,
            });
            match self.docs.atomic_batch(ops).await {
                Ok(_) => {}
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
        bail!("cron store: admission initialization kept changing")
    }

    /// Writes a new job with its original creation time in the document body
    /// and enrols it in the admission ledger. The job is written
    /// first and enrolled second, so a job over the cap is taken back.
    ///
    /// # Errors
    /// The cap would be exceeded, or the backend fails.
    pub(super) async fn insert_with_created(
        &self,
        id: &str,
        mut doc: Value,
        created_ms: i64,
    ) -> Result<()> {
        let body = doc
            .as_object_mut()
            .ok_or_else(|| anyhow!("cron store: imported job must be an object"))?;
        body.insert("created_ms".into(), created_ms.into());
        let options = tinyflows_storage::WriteOptions::new().schema(super::SCHEMA_VERSION);
        let member = member(id, &doc)?;
        // A nontransactional backend cannot enrol, so check the persisted
        // policy before the job exists rather than after it is written.
        let transactional = self.docs.capabilities().contains(Capability::Transactions);
        if !transactional {
            self.require_unlimited_backend().await?;
        }
        match self
            .docs
            .put_with(JOBS, id, doc.clone(), Precondition::Absent, options)
            .await
        {
            Ok(_) => {}
            Err(error) => return Err(storage_error(error)),
        }
        if !transactional {
            return Ok(());
        }
        let enrolled = self.enrol(member).await;
        if enrolled.is_err() {
            let _ = self.docs.delete(JOBS, id, Precondition::None).await;
        }
        enrolled
    }

    async fn enrol(&self, member: Member) -> Result<()> {
        for _ in 0..CAS_ATTEMPTS {
            let ledger = self.admission().await?;
            let mut state: Admission = decode(ledger.doc.clone())?;
            state.jobs.insert(member.job_id.clone(), member.clone());
            state.check()?;
            let op = WriteOp::Put {
                collection: ADMISSION.into(),
                id: LEDGER.into(),
                doc: encode(state)?,
                precondition: ledger.unchanged(),
            };
            match self.docs.atomic_batch(vec![op]).await {
                Ok(_) => return Ok(()),
                Err(error) if error.kind() == ErrorKind::Conflict => {}
                Err(error) => return Err(storage_error(error)),
            }
        }
        bail!("cron store: admission enrolment kept changing")
    }

    /// A nontransactional backend must never bypass persisted policy.
    pub(super) async fn require_unlimited_backend(&self) -> Result<()> {
        if self
            .docs
            .get(ADMISSION, LEDGER)
            .await
            .map_err(storage_error)?
            .is_some()
        {
            anyhow::bail!("cron store: transactions required for stored admission policy");
        }
        Ok(())
    }

    /// Commits a job plus membership and optional run writes atomically. The
    /// ledger incarnation check closes the delete/re-create version ABA gap.
    pub(super) async fn commit_job(
        &self,
        id: &str,
        previous: Option<&Versioned<Value>>,
        next: Option<Value>,
        mut extra: Vec<WriteOp>,
    ) -> Result<Vec<WriteResult>> {
        if !self.docs.capabilities().contains(Capability::Transactions) {
            // This backend cannot install admission policy, so absent policy
            // is a stable fact, not a race with another participating writer.
            self.require_unlimited_backend().await?;
            if !extra.is_empty() {
                anyhow::bail!("cron store: transactions required for atomic run writes");
            }
            let precondition = previous.map_or(Precondition::Absent, Versioned::unchanged);
            return match next {
                Some(doc) => self
                    .docs
                    .put(JOBS, id, doc, precondition)
                    .await
                    .map(|version| vec![WriteResult::Put { version }])
                    .map_err(storage_error),
                None => self
                    .docs
                    .delete(JOBS, id, precondition)
                    .await
                    .map(|removed| vec![WriteResult::Delete { removed }])
                    .map_err(storage_error),
            };
        }
        let ledger = self.admission().await?;
        let mut state: Admission = decode(ledger.doc.clone())?;
        let owns = match (previous, state.jobs.get(id)) {
            (None, None) => true,
            (Some(old), Some(current)) => current.incarnation.as_deref() == incarnation(&old.doc),
            _ => false,
        };
        if !owns {
            return Err(storage_error(tinyflows_storage::StorageError::conflict(
                "cron job incarnation changed",
            )));
        }
        let current = self.docs.get(JOBS, id).await.map_err(storage_error)?;
        let precondition = match (previous, current.as_ref()) {
            (None, None) => Precondition::Absent,
            (Some(old), Some(current)) if old.doc == current.doc => current.unchanged(),
            _ => {
                return Err(storage_error(tinyflows_storage::StorageError::conflict(
                    "cron job changed",
                )));
            }
        };
        let op = match next {
            Some(doc) => {
                state.jobs.insert(id.into(), member(id, &doc)?);
                WriteOp::Put {
                    collection: JOBS.into(),
                    id: id.into(),
                    doc,
                    precondition,
                }
            }
            None => {
                state.jobs.remove(id);
                WriteOp::Delete {
                    collection: JOBS.into(),
                    id: id.into(),
                    precondition,
                }
            }
        };
        state.check()?;
        let mut ops = vec![op];
        ops.append(&mut extra);
        ops.push(WriteOp::Put {
            collection: ADMISSION.into(),
            id: LEDGER.into(),
            doc: encode(state)?,
            precondition: ledger.unchanged(),
        });
        self.docs.atomic_batch(ops).await.map_err(storage_error)
    }
}

/// A conflicted atomic write is retryable; all other errors fail closed.
pub(super) fn conflict(error: &anyhow::Error) -> bool {
    // `storage_error` retains the storage error as an anyhow source below.
    error
        .downcast_ref::<tinyflows_storage::StorageError>()
        .is_some_and(|error| error.kind() == ErrorKind::Conflict)
}

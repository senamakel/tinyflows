//! [`Ledger`] over a driver [`DocumentStore`]; see the module docs for the
//! layout and the tenancy model.

use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};
use tinystoragedrivers_core::{
    CollectionSpec, DocumentStore, DocumentStoreExt, Filter, IndexSpec, Precondition, Query, Sort,
    StorageError, Versioned,
};
use tokio::sync::OnceCell;

use super::{CAS_ATTEMPTS, is_race, key};
use crate::execute::StepRecord;
use crate::ledger::{
    Episode, EpisodeStatus, Ledger, LedgerError, LedgerRow, Lesson, LessonKind, Page, Result, Score,
};

const COUNTERS: &str = "adaptive_counters";
const ROWS: &str = "adaptive_rows";
const LESSONS: &str = "adaptive_lessons";
const EVIDENCE: &str = "adaptive_evidence";
const SCORES: &str = "adaptive_scores";
const VARIANTS: &str = "adaptive_variants";
const EPISODES: &str = "adaptive_episodes";
const STEPS: &str = "adaptive_steps";

fn backend(error: StorageError) -> LedgerError {
    LedgerError::Backend(error.to_string())
}

fn corrupt(error: impl std::fmt::Display) -> LedgerError {
    LedgerError::Corrupt(error.to_string())
}

fn encode(value: &impl Serialize) -> Result<Value> {
    serde_json::to_value(value).map_err(corrupt)
}

/// The `record` field of a stored document, decoded.
fn record<T: DeserializeOwned>(stored: &Versioned<Value>) -> Result<T> {
    let raw = stored
        .doc
        .get("record")
        .cloned()
        .ok_or_else(|| corrupt(format!("{} has no record", stored.id)))?;
    serde_json::from_value(raw).map_err(|error| corrupt(format!("{}: {error}", stored.id)))
}

fn counter(doc: &Value, field: &str) -> u32 {
    doc.get(field)
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .unwrap_or(0)
}

/// A ledger stored in a driver document store.
#[derive(Clone)]
pub struct DriverLedger {
    docs: Arc<dyn DocumentStore>,
    scope: Option<String>,
    declared: Arc<OnceCell<()>>,
}

impl std::fmt::Debug for DriverLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverLedger")
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

impl DriverLedger {
    /// A ledger in `docs`, reading and writing the global bucket.
    pub fn new(docs: Arc<dyn DocumentStore>) -> Self {
        Self {
            docs,
            scope: None,
            declared: Arc::new(OnceCell::new()),
        }
    }

    /// A handle onto the same store, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: impl Into<String>) -> Self {
        Self {
            docs: Arc::clone(&self.docs),
            scope: Some(scope.into()),
            declared: Arc::clone(&self.declared),
        }
    }

    /// This handle's bucket, as stored: `""` for global.
    fn bucket(&self) -> &str {
        self.scope.as_deref().unwrap_or_default()
    }

    /// This bucket plus global, the one read rule everywhere.
    fn visible(&self) -> Filter {
        Filter::one_of("scope_key", [self.bucket(), ""])
    }

    async fn declared(&self) -> Result<()> {
        self.declared
            .get_or_try_init(|| async {
                let specs = [
                    CollectionSpec::new(COUNTERS),
                    CollectionSpec::new(ROWS).index(IndexSpec::new(
                        "by_episode",
                        ["scope_key", "episode", "seq"],
                    )),
                    CollectionSpec::new(LESSONS)
                        .index(IndexSpec::new("by_scope", ["scope_key", "seq"])),
                    CollectionSpec::new(EVIDENCE)
                        .index(IndexSpec::new("by_lesson", ["lesson_id"])),
                    CollectionSpec::new(SCORES),
                    CollectionSpec::new(VARIANTS)
                        .index(IndexSpec::new("by_parent", ["scope_key", "parent"])),
                    CollectionSpec::new(EPISODES)
                        .index(IndexSpec::new("by_scope", ["scope_key", "updated_at"])),
                    CollectionSpec::new(STEPS),
                ];
                for spec in &specs {
                    self.docs.ensure_collection(spec).await.map_err(backend)?;
                }
                Ok::<(), LedgerError>(())
            })
            .await
            .map(|_| ())
    }

    /// The next value of the sequence `name`, by compare-and-swap so two
    /// writers never share one.
    async fn next_seq(&self, name: &str) -> Result<u64> {
        for _ in 0..CAS_ATTEMPTS {
            let current = self.docs.get(COUNTERS, name).await.map_err(backend)?;
            let (next, precondition) = match &current {
                Some(found) => (
                    found.doc.get("seq").and_then(Value::as_u64).unwrap_or(0) + 1,
                    found.unchanged(),
                ),
                None => (1, Precondition::Absent),
            };
            match self
                .docs
                .put(COUNTERS, name, json!({ "seq": next }), precondition)
                .await
            {
                Ok(_) => return Ok(next),
                Err(error) if is_race(&error) => {}
                Err(error) => return Err(backend(error)),
            }
        }
        Err(LedgerError::Backend(format!(
            "the {name} sequence kept changing under {CAS_ATTEMPTS} attempts"
        )))
    }

    /// Adds one application (and one help when `helped`) to the counters of
    /// the document `id` in `collection`, creating it from `fresh` when
    /// absent; `None` from `fresh` means "do not create".
    async fn bump(
        &self,
        collection: &str,
        id: &str,
        helped: bool,
        fresh: Option<Value>,
        visible: impl Fn(&Value) -> bool,
    ) -> Result<()> {
        for _ in 0..CAS_ATTEMPTS {
            let current = self.docs.get(collection, id).await.map_err(backend)?;
            let (mut doc, precondition) = match current {
                Some(found) if visible(&found.doc) => {
                    let pre = found.unchanged();
                    (found.doc, pre)
                }
                Some(_) => return Ok(()),
                None => match &fresh {
                    Some(fresh) => (fresh.clone(), Precondition::Absent),
                    None => return Ok(()),
                },
            };
            doc["applied"] = json!(u64::from(counter(&doc, "applied")) + 1);
            doc["helped"] = json!(u64::from(counter(&doc, "helped")) + u64::from(helped));
            match self.docs.put(collection, id, doc, precondition).await {
                Ok(_) => return Ok(()),
                Err(error) if is_race(&error) => {}
                Err(error) => return Err(backend(error)),
            }
        }
        Err(LedgerError::Backend(format!(
            "counters of {id} kept changing under {CAS_ATTEMPTS} attempts"
        )))
    }

    /// Writes `doc` at `id` only if nothing is there yet.
    async fn insert_once(&self, collection: &str, id: &str, doc: Value) -> Result<()> {
        match self.docs.put(collection, id, doc, Precondition::Absent).await {
            Ok(_) => Ok(()),
            Err(error) if is_race(&error) => Ok(()),
            Err(error) => Err(backend(error)),
        }
    }

    async fn all(&self, collection: &str, query: Query) -> Result<Vec<Versioned<Value>>> {
        self.docs.query_all(collection, &query).await.map_err(backend)
    }

    fn lesson_from(stored: &Versioned<Value>) -> Result<Lesson> {
        let mut lesson: Lesson = record(stored)?;
        lesson.id.clone_from(&stored.id);
        lesson.applied = counter(&stored.doc, "applied");
        lesson.helped = counter(&stored.doc, "helped");
        let scope = stored
            .doc
            .get("scope_key")
            .and_then(Value::as_str)
            .unwrap_or_default();
        lesson.scope_key = (!scope.is_empty()).then(|| scope.to_string());
        Ok(lesson)
    }

    fn row_from(stored: &Versioned<Value>) -> Result<LedgerRow> {
        let mut row: LedgerRow = record(stored)?;
        row.id.clone_from(&stored.id);
        Ok(row)
    }
}

fn kind_str(kind: LessonKind) -> &'static str {
    match kind {
        LessonKind::Strategy => "strategy",
        LessonKind::Constraint => "constraint",
        LessonKind::FailureMode => "failure_mode",
        LessonKind::Calibration => "calibration",
    }
}

#[async_trait]
impl Ledger for DriverLedger {
    fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    async fn append(&self, row: &LedgerRow) -> Result<String> {
        self.declared().await?;
        let seq = self.next_seq(ROWS).await?;
        let id = format!("ldg_{seq:08}");
        let doc = json!({
            "episode": row.episode,
            "scope_key": self.bucket(),
            "seq": seq,
            "record": encode(row)?,
        });
        self.docs
            .put(ROWS, &id, doc, Precondition::Absent)
            .await
            .map_err(backend)?;
        Ok(id)
    }

    async fn rows(&self, episode: &str) -> Result<Vec<LedgerRow>> {
        self.declared().await?;
        let query = Query::filter(
            Filter::eq("scope_key", self.bucket()).and(Filter::eq("episode", episode)),
        )
        .sort(Sort::asc("seq"));
        self.all(ROWS, query)
            .await?
            .iter()
            .map(Self::row_from)
            .collect()
    }

    async fn promote(&self, lesson: &Lesson, cites: &[String]) -> Result<String> {
        self.declared().await?;
        let seq = self.next_seq(LESSONS).await?;
        let id = format!("les_{seq:08}");
        let doc = json!({
            "kind": kind_str(lesson.kind),
            // The handle's scope, never the argument's.
            "scope_key": self.bucket(),
            "seq": seq,
            "applied": lesson.applied,
            "helped": lesson.helped,
            "record": encode(lesson)?,
        });
        self.docs
            .put(LESSONS, &id, doc, Precondition::Absent)
            .await
            .map_err(backend)?;
        for row_id in cites {
            self.insert_once(
                EVIDENCE,
                &key(&[&id, row_id]),
                json!({ "lesson_id": id, "row_id": row_id }),
            )
            .await?;
        }
        Ok(id)
    }

    async fn lessons(&self, kind: Option<LessonKind>) -> Result<Vec<Lesson>> {
        self.declared().await?;
        let mut filter = self.visible();
        if let Some(want) = kind {
            filter = filter.and(Filter::eq("kind", kind_str(want)));
        }
        self.all(LESSONS, Query::filter(filter).sort(Sort::asc("seq")))
            .await?
            .iter()
            .map(Self::lesson_from)
            .collect()
    }

    async fn evidence(&self, lesson_id: &str) -> Result<Vec<LedgerRow>> {
        self.declared().await?;
        let cited: Vec<String> = self
            .all(EVIDENCE, Query::filter(Filter::eq("lesson_id", lesson_id)))
            .await?
            .into_iter()
            .filter_map(|edge| {
                edge.doc
                    .get("row_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect();
        if cited.is_empty() {
            return Ok(Vec::new());
        }
        let query = Query::filter(
            self.visible()
                .and(Filter::one_of(tinystoragedrivers_core::ID_FIELD, cited)),
        )
        .sort(Sort::asc("seq"));
        self.all(ROWS, query)
            .await?
            .iter()
            .map(Self::row_from)
            .collect()
    }

    async fn score_lesson(&self, lesson_id: &str, helped: bool) -> Result<()> {
        self.declared().await?;
        // Only what this handle can see: its bucket, or global. The ids reach
        // here from model output, and a tenant must not be able to move
        // another tenant's score by naming its id.
        let bucket = self.bucket().to_string();
        self.bump(LESSONS, lesson_id, helped, None, move |doc| {
            let scope = doc.get("scope_key").and_then(Value::as_str).unwrap_or_default();
            scope.is_empty() || scope == bucket
        })
        .await
    }

    async fn score_workflow(&self, workflow_id: &str, helped: bool) -> Result<()> {
        self.declared().await?;
        let fresh = json!({
            "scope_key": self.bucket(),
            "workflow_id": workflow_id,
            "applied": 0,
            "helped": 0,
        });
        self.bump(
            SCORES,
            &key(&[self.bucket(), workflow_id]),
            helped,
            Some(fresh),
            |_| true,
        )
        .await
    }

    async fn workflow_score(&self, workflow_id: &str) -> Result<Score> {
        self.declared().await?;
        let found = self
            .docs
            .get(SCORES, &key(&[self.bucket(), workflow_id]))
            .await
            .map_err(backend)?;
        Ok(found.map_or_else(Score::default, |stored| Score {
            applied: counter(&stored.doc, "applied"),
            helped: counter(&stored.doc, "helped"),
        }))
    }

    async fn link_variant(&self, parent: &str, variant: &str) -> Result<()> {
        self.declared().await?;
        self.insert_once(
            VARIANTS,
            &key(&[self.bucket(), variant]),
            json!({ "scope_key": self.bucket(), "variant": variant, "parent": parent }),
        )
        .await
    }

    async fn parent_of(&self, id: &str) -> Result<Option<String>> {
        self.declared().await?;
        let found = self
            .docs
            .get(VARIANTS, &key(&[self.bucket(), id]))
            .await
            .map_err(backend)?;
        Ok(found
            .and_then(|stored| {
                stored
                    .doc
                    .get("parent")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .filter(|parent| !parent.is_empty()))
    }

    async fn children_of(&self, id: &str) -> Result<Vec<String>> {
        self.declared().await?;
        let query = Query::filter(
            Filter::eq("scope_key", self.bucket()).and(Filter::eq("parent", id)),
        )
        .sort(Sort::asc("variant"));
        Ok(self
            .all(VARIANTS, query)
            .await?
            .into_iter()
            .filter_map(|edge| {
                edge.doc
                    .get("variant")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            })
            .collect())
    }

    async fn save_episode(&self, episode: &Episode) -> Result<()> {
        self.declared().await?;
        let id = key(&[self.bucket(), &episode.id]);
        for _ in 0..CAS_ATTEMPTS {
            let current = self.docs.get(EPISODES, &id).await.map_err(backend)?;
            let mut stored = Episode {
                scope_key: self.scope.clone(),
                ..episode.clone()
            };
            let precondition = match &current {
                Some(found) => {
                    // `started_at` is a fact about creation, not progress, so
                    // an update leaves it alone — matching mongo's
                    // `$setOnInsert`.
                    let existing: Episode = record(found)?;
                    stored.started_at = existing.started_at;
                    found.unchanged()
                }
                None => Precondition::Absent,
            };
            let doc = json!({
                "scope_key": self.bucket(),
                "episode_id": stored.id,
                "updated_at": stored.updated_at,
                "running": stored.status == EpisodeStatus::Running,
                "record": encode(&stored)?,
            });
            match self.docs.put(EPISODES, &id, doc, precondition).await {
                Ok(_) => return Ok(()),
                Err(error) if is_race(&error) => {}
                Err(error) => return Err(backend(error)),
            }
        }
        Err(LedgerError::Backend(format!(
            "episode {} kept changing under {CAS_ATTEMPTS} attempts",
            episode.id
        )))
    }

    async fn episode(&self, id: &str) -> Result<Option<Episode>> {
        self.declared().await?;
        self.docs
            .get(EPISODES, &key(&[self.bucket(), id]))
            .await
            .map_err(backend)?
            .as_ref()
            .map(record)
            .transpose()
    }

    async fn episodes(&self, running_only: bool, page: Page) -> Result<Vec<Episode>> {
        self.declared().await?;
        let mut filter = Filter::eq("scope_key", self.bucket());
        if running_only {
            filter = filter.and(Filter::eq("running", true));
        }
        // Newest first, ids breaking ties — the order `Page` documents.
        let query = Query::filter(filter)
            .sort(Sort::desc("updated_at"))
            .sort(Sort::asc("episode_id"));
        let found: Vec<Episode> = self
            .all(EPISODES, query)
            .await?
            .iter()
            .map(record)
            .collect::<Result<_>>()?;
        Ok(page.apply(found))
    }

    async fn save_steps(&self, row_id: &str, steps: &[StepRecord]) -> Result<()> {
        self.declared().await?;
        let doc = json!({
            "scope_key": self.bucket(),
            "row_id": row_id,
            "record": encode(&steps)?,
        });
        self.docs
            .put(STEPS, &key(&[self.bucket(), row_id]), doc, Precondition::None)
            .await
            .map(|_| ())
            .map_err(backend)
    }

    async fn steps(&self, row_id: &str) -> Result<Vec<StepRecord>> {
        self.declared().await?;
        self.docs
            .get(STEPS, &key(&[self.bucket(), row_id]))
            .await
            .map_err(backend)?
            .as_ref()
            .map(record)
            .transpose()
            .map(Option::unwrap_or_default)
    }
}

#[cfg(test)]
#[path = "ledger_tests.rs"]
mod tests;

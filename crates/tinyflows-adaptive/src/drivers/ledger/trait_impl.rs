//! The [`Ledger`] trait for [`DriverLedger`].

use async_trait::async_trait;
use serde_json::{Value, json};
use tinystoragedrivers_core::{Filter, Precondition, Query, Sort};

use super::{
    DriverLedger, EPISODES, EVIDENCE, LESSONS, ROWS, SCORES, STEPS, VARIANTS, backend, counter,
    encode, kind_str, record,
};
use crate::drivers::{CAS_ATTEMPTS, is_race, key};
use crate::execute::StepRecord;
use crate::ledger::{
    Episode, EpisodeStatus, Ledger, LedgerError, LedgerRow, Lesson, LessonKind, Page, Result, Score,
};

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
            let scope = doc
                .get("scope_key")
                .and_then(Value::as_str)
                .unwrap_or_default();
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
        let query =
            Query::filter(Filter::eq("scope_key", self.bucket()).and(Filter::eq("parent", id)))
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
        // One document per step, as the Mongo ledger writes them: a looped
        // graph can produce more step output than one document may hold.
        let mine = Filter::eq("scope_key", self.bucket()).and(Filter::eq("row_id", row_id));
        self.docs
            .delete_where(STEPS, &mine)
            .await
            .map_err(backend)?;
        for (seq, step) in steps.iter().enumerate() {
            let doc = json!({
                "scope_key": self.bucket(),
                "row_id": row_id,
                "seq": seq,
                "record": encode(step)?,
            });
            self.docs
                .put(
                    STEPS,
                    &key(&[self.bucket(), row_id, &format!("{seq:08}")]),
                    doc,
                    Precondition::None,
                )
                .await
                .map_err(backend)?;
        }
        Ok(())
    }

    async fn steps(&self, row_id: &str) -> Result<Vec<StepRecord>> {
        self.declared().await?;
        let query =
            Query::filter(Filter::eq("scope_key", self.bucket()).and(Filter::eq("row_id", row_id)))
                .sort(Sort::asc("seq"));
        self.all(STEPS, query).await?.iter().map(record).collect()
    }
}

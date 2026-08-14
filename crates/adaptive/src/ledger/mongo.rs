//! A [`Ledger`] on MongoDB, for a hosted deployment.
//!
//! Four collections mirroring the sqlite tables, and the same conformance suite
//! runs against both. Where the two differ is concurrency: this one is a real
//! async driver and several loops may write the same ledger at once, so the two
//! counter updates use `$inc` rather than read-modify-write. A read-modify-write
//! here loses increments under exactly the load a hosted deployment has.

use async_trait::async_trait;
use mongodb::bson::{Document, doc};
use mongodb::options::{IndexOptions, ReturnDocument};
use mongodb::{Client, Collection, Database, IndexModel};

use super::{Ledger, LedgerError, LedgerRow, Lesson, LessonKind, Result, Score};

impl From<mongodb::error::Error> for LedgerError {
    fn from(err: mongodb::error::Error) -> Self {
        Self::Backend(err.to_string())
    }
}

impl From<mongodb::bson::ser::Error> for LedgerError {
    fn from(err: mongodb::bson::ser::Error) -> Self {
        Self::Corrupt(err.to_string())
    }
}

impl From<mongodb::bson::de::Error> for LedgerError {
    fn from(err: mongodb::bson::de::Error) -> Self {
        Self::Corrupt(err.to_string())
    }
}

const ROWS: &str = "ledger_rows";
const LESSONS: &str = "lessons";
const EVIDENCE: &str = "lesson_evidence";
const SCORES: &str = "workflow_scores";
const COUNTERS: &str = "counters";

/// A ledger backed by a MongoDB database.
pub struct MongoLedger {
    db: Database,
}

impl MongoLedger {
    /// Connect to `uri` and use the database named `database`.
    ///
    /// # Errors
    /// When the URI is malformed, the server is unreachable, or an index
    /// cannot be created.
    pub async fn connect(uri: &str, database: &str) -> Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        Self::with_database(client.database(database)).await
    }

    /// Use an already-connected database. For a host that manages its own
    /// client and pool.
    ///
    /// # Errors
    /// When an index cannot be created.
    pub async fn with_database(db: Database) -> Result<Self> {
        let store = Self { db };
        store.ensure_indexes().await?;
        Ok(store)
    }

    async fn ensure_indexes(&self) -> Result<()> {
        // Ordered by `seq`, never by timestamp: two attempts finishing in the
        // same second would otherwise read back in an arbitrary order, which
        // silently reorders the exclusion list.
        self.rows()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "episode": 1, "seq": 1 })
                    .build(),
            )
            .await?;
        self.evidence()
            .create_index(IndexModel::builder().keys(doc! { "lesson_id": 1 }).build())
            .await?;
        let unique = IndexOptions::builder().unique(true).build();
        self.scores()
            .create_index(
                IndexModel::builder()
                    .keys(doc! { "workflow_id": 1 })
                    .options(unique)
                    .build(),
            )
            .await?;
        Ok(())
    }

    fn rows(&self) -> Collection<Document> {
        self.db.collection(ROWS)
    }
    fn lessons_c(&self) -> Collection<Document> {
        self.db.collection(LESSONS)
    }
    fn evidence(&self) -> Collection<Document> {
        self.db.collection(EVIDENCE)
    }
    fn scores(&self) -> Collection<Document> {
        self.db.collection(SCORES)
    }

    /// The next value in a named sequence.
    ///
    /// A counter document rather than a `count()` of the collection: counting
    /// races with a concurrent insert and hands two writers the same number,
    /// while `findAndModify` with `$inc` is atomic on the server.
    async fn next_seq(&self, name: &str) -> Result<i64> {
        let updated = self
            .db
            .collection::<Document>(COUNTERS)
            .find_one_and_update(doc! { "_id": name }, doc! { "$inc": { "seq": 1 } })
            .upsert(true)
            .return_document(ReturnDocument::After)
            .await?;
        Ok(updated.and_then(|d| d.get_i64("seq").ok()).unwrap_or(1))
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

fn as_u32(doc: &Document, key: &str) -> u32 {
    doc.get_i64(key)
        .ok()
        .and_then(|v| u32::try_from(v).ok())
        .or_else(|| doc.get_i32(key).ok().and_then(|v| u32::try_from(v).ok()))
        .unwrap_or(0)
}

fn text(doc: &Document, key: &str) -> String {
    doc.get_str(key).unwrap_or_default().to_string()
}

fn read_row(doc: &Document) -> LedgerRow {
    LedgerRow {
        id: text(doc, "_id"),
        episode: text(doc, "episode"),
        attempt: as_u32(doc, "attempt"),
        approach_sig: text(doc, "approach_sig"),
        approach_desc: text(doc, "approach_desc"),
        // An absent key and a stored null are the same thing to a reader.
        workflow_id: doc.get_str("workflow_id").ok().map(ToString::to_string),
        outcome: text(doc, "outcome"),
        cause: text(doc, "cause"),
        cost_usd: doc.get_f64("cost_usd").unwrap_or(0.0),
        at: text(doc, "at"),
    }
}

#[async_trait]
impl Ledger for MongoLedger {
    async fn append(&self, row: &LedgerRow) -> Result<String> {
        let seq = self.next_seq(ROWS).await?;
        let id = format!("ldg_{seq:08}");
        self.rows()
            .insert_one(doc! {
                "_id": &id,
                "episode": &row.episode,
                "attempt": i64::from(row.attempt),
                "approach_sig": &row.approach_sig,
                "approach_desc": &row.approach_desc,
                "workflow_id": row.workflow_id.clone(),
                "outcome": &row.outcome,
                "cause": &row.cause,
                "cost_usd": row.cost_usd,
                "at": &row.at,
                "seq": seq,
            })
            .await?;
        Ok(id)
    }

    async fn rows(&self, episode: &str) -> Result<Vec<LedgerRow>> {
        let mut cursor = self
            .rows()
            .find(doc! { "episode": episode })
            .sort(doc! { "seq": 1 })
            .await?;
        let mut out = Vec::new();
        while cursor.advance().await? {
            out.push(read_row(&cursor.deserialize_current()?));
        }
        Ok(out)
    }

    async fn promote(&self, lesson: &Lesson, cites: &[String]) -> Result<String> {
        let seq = self.next_seq(LESSONS).await?;
        let id = format!("les_{seq:08}");
        self.lessons_c()
            .insert_one(doc! {
                "_id": &id,
                "kind": kind_str(lesson.kind),
                "trigger": &lesson.trigger,
                "mechanism": &lesson.mechanism,
                "claim": &lesson.claim,
                "applied": i64::from(lesson.applied),
                "helped": i64::from(lesson.helped),
                "seq": seq,
            })
            .await?;
        for row_id in cites {
            // Upsert on the pair so re-promoting the same citation is a no-op
            // rather than a duplicate edge.
            self.evidence()
                .update_one(
                    doc! { "lesson_id": &id, "row_id": row_id },
                    doc! { "$setOnInsert": { "lesson_id": &id, "row_id": row_id } },
                )
                .upsert(true)
                .await?;
        }
        Ok(id)
    }

    async fn lessons(&self, kind: Option<LessonKind>) -> Result<Vec<Lesson>> {
        let filter = match kind {
            Some(want) => doc! { "kind": kind_str(want) },
            None => doc! {},
        };
        let mut cursor = self
            .lessons_c()
            .find(filter)
            .sort(doc! { "seq": 1 })
            .await?;
        let mut out = Vec::new();
        while cursor.advance().await? {
            let d = cursor.deserialize_current()?;
            out.push(Lesson {
                id: text(&d, "_id"),
                kind: LessonKind::parse(&text(&d, "kind")),
                trigger: text(&d, "trigger"),
                mechanism: text(&d, "mechanism"),
                claim: text(&d, "claim"),
                applied: as_u32(&d, "applied"),
                helped: as_u32(&d, "helped"),
            });
        }
        Ok(out)
    }

    async fn evidence(&self, lesson_id: &str) -> Result<Vec<LedgerRow>> {
        let mut cursor = self
            .evidence()
            .find(doc! { "lesson_id": lesson_id })
            .await?;
        let mut ids = Vec::new();
        while cursor.advance().await? {
            ids.push(text(&cursor.deserialize_current()?, "row_id"));
        }
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut found = self
            .rows()
            .find(doc! { "_id": { "$in": ids } })
            .sort(doc! { "seq": 1 })
            .await?;
        let mut out = Vec::new();
        while found.advance().await? {
            out.push(read_row(&found.deserialize_current()?));
        }
        Ok(out)
    }

    async fn score_lesson(&self, lesson_id: &str, helped: bool) -> Result<()> {
        self.lessons_c()
            .update_one(
                doc! { "_id": lesson_id },
                doc! { "$inc": { "applied": 1_i64, "helped": i64::from(helped) } },
            )
            .await?;
        Ok(())
    }

    async fn score_workflow(&self, workflow_id: &str, helped: bool) -> Result<()> {
        // `$inc` on an upsert, not read-modify-write: several loops may finish
        // the same workflow at once, and a lost increment is a promotion gate
        // reading the wrong evidence.
        self.scores()
            .update_one(
                doc! { "workflow_id": workflow_id },
                doc! { "$inc": { "applied": 1_i64, "helped": i64::from(helped) } },
            )
            .upsert(true)
            .await?;
        Ok(())
    }

    async fn workflow_score(&self, workflow_id: &str) -> Result<Score> {
        let found = self
            .scores()
            .find_one(doc! { "workflow_id": workflow_id })
            .await?;
        Ok(found.map_or_else(Score::default, |d| Score {
            applied: as_u32(&d, "applied"),
            helped: as_u32(&d, "helped"),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::conformance;

    /// Runs the same suite the sqlite backend passes, against a real server.
    ///
    /// Ignored by default: it needs one. Point `ADAPTIVE_MONGO_URI` at a
    /// throwaway database and run with `--ignored`. Skipping silently when the
    /// variable is absent would let this rot unnoticed, so the case is
    /// `#[ignore]` and visible in the run summary instead.
    #[tokio::test]
    #[ignore = "needs a MongoDB server; set ADAPTIVE_MONGO_URI"]
    async fn passes_the_conformance_suite() {
        let uri = std::env::var("ADAPTIVE_MONGO_URI").expect("ADAPTIVE_MONGO_URI");
        let name = format!("adaptive_conformance_{}", std::process::id());
        let store = MongoLedger::connect(&uri, &name).await.expect("connect");
        conformance::run_all(&store).await;
        store.db.drop().await.expect("drop the throwaway database");
    }
}

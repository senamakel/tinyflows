//! A [`Ledger`] on sqlite, for a single-process deployment.
//!
//! Synchronous work behind an async trait, on purpose. Every call here is one
//! or two short statements against a local file; wrapping them in
//! `spawn_blocking` would add a thread hop and a tokio dependency to save
//! microseconds nobody can measure. If a deployment ever puts this behind
//! enough concurrency for the lock to matter, that is the moment to move —
//! not before, and the trait means the move costs one file.

use std::sync::Mutex;

use async_trait::async_trait;
use rusqlite::{Connection, OptionalExtension, params};

use super::{Ledger, LedgerError, LedgerRow, Lesson, LessonKind, Result, Score};

impl From<rusqlite::Error> for LedgerError {
    fn from(err: rusqlite::Error) -> Self {
        Self::Backend(err.to_string())
    }
}

/// The schema, applied on open.
///
/// `IF NOT EXISTS` throughout so opening an existing ledger is a no-op, and
/// every table carries its own id rather than relying on rowid — a row id
/// leaves this process (a lesson cites them) and rowid is not stable across a
/// vacuum.
const DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS ledger_rows (
        id            TEXT PRIMARY KEY,
        episode       TEXT NOT NULL,
        attempt       INTEGER NOT NULL,
        approach_sig  TEXT NOT NULL,
        approach_desc TEXT NOT NULL DEFAULT '',
        workflow_id   TEXT,
        outcome       TEXT NOT NULL DEFAULT '',
        cause         TEXT NOT NULL DEFAULT '',
        cost_usd      REAL NOT NULL DEFAULT 0,
        at            TEXT NOT NULL,
        seq           INTEGER NOT NULL
    )",
    // Ordered by `seq`, not by `at`: two attempts finishing in the same second
    // are common, and a timestamp tie makes the ledger read in an arbitrary
    // order — which silently reorders the exclusion list.
    "CREATE INDEX IF NOT EXISTS ix_rows_episode ON ledger_rows(episode, seq)",
    "CREATE TABLE IF NOT EXISTS lessons (
        id        TEXT PRIMARY KEY,
        kind      TEXT NOT NULL,
        trigger   TEXT NOT NULL,
        mechanism TEXT NOT NULL DEFAULT '',
        claim     TEXT NOT NULL,
        applied   INTEGER NOT NULL DEFAULT 0,
        helped    INTEGER NOT NULL DEFAULT 0,
        seq       INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS lesson_evidence (
        lesson_id TEXT NOT NULL,
        row_id    TEXT NOT NULL,
        PRIMARY KEY (lesson_id, row_id)
    )",
    "CREATE TABLE IF NOT EXISTS workflow_scores (
        workflow_id TEXT PRIMARY KEY,
        applied     INTEGER NOT NULL DEFAULT 0,
        helped      INTEGER NOT NULL DEFAULT 0
    )",
];

/// A ledger backed by one sqlite file.
pub struct SqliteLedger {
    conn: Mutex<Connection>,
}

impl SqliteLedger {
    /// Open (or create) a ledger at `path`.
    ///
    /// # Errors
    /// When the file cannot be opened or the schema cannot be applied.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self> {
        Self::from_connection(Connection::open(path)?)
    }

    /// A ledger held entirely in memory. For tests, and for a host that wants
    /// the loop to run without learning anything durable.
    ///
    /// # Errors
    /// When the schema cannot be applied.
    pub fn in_memory() -> Result<Self> {
        Self::from_connection(Connection::open_in_memory()?)
    }

    fn from_connection(conn: Connection) -> Result<Self> {
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")
            .ok();
        for statement in DDL {
            conn.execute(statement, [])?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn guard(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        // A poisoned lock means a previous caller panicked mid-write. The
        // ledger is append-mostly and every write is a single statement, so
        // the data is intact; refusing every later call would turn one panic
        // into a dead loop.
        Ok(self
            .conn
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()))
    }
}

fn next_seq(conn: &Connection, table: &str) -> Result<i64> {
    let current: Option<i64> = conn
        .query_row(&format!("SELECT MAX(seq) FROM {table}"), [], |r| r.get(0))
        .optional()?
        .flatten();
    Ok(current.unwrap_or(0) + 1)
}

fn new_id(prefix: &str, seq: i64) -> String {
    format!("{prefix}_{seq:08}")
}

fn read_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<LedgerRow> {
    Ok(LedgerRow {
        id: r.get("id")?,
        episode: r.get("episode")?,
        attempt: r.get::<_, i64>("attempt")?.try_into().unwrap_or(0),
        approach_sig: r.get("approach_sig")?,
        approach_desc: r.get("approach_desc")?,
        workflow_id: r.get("workflow_id")?,
        outcome: r.get("outcome")?,
        cause: r.get("cause")?,
        cost_usd: r.get("cost_usd")?,
        at: r.get("at")?,
    })
}

#[async_trait]
impl Ledger for SqliteLedger {
    async fn append(&self, row: &LedgerRow) -> Result<String> {
        let conn = self.guard()?;
        let seq = next_seq(&conn, "ledger_rows")?;
        let id = new_id("ldg", seq);
        conn.execute(
            "INSERT INTO ledger_rows(id, episode, attempt, approach_sig, approach_desc,
                                     workflow_id, outcome, cause, cost_usd, at, seq)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
            params![
                id,
                row.episode,
                i64::from(row.attempt),
                row.approach_sig,
                row.approach_desc,
                row.workflow_id,
                row.outcome,
                row.cause,
                row.cost_usd,
                row.at,
                seq,
            ],
        )?;
        Ok(id)
    }

    async fn rows(&self, episode: &str) -> Result<Vec<LedgerRow>> {
        let conn = self.guard()?;
        let mut stmt = conn.prepare("SELECT * FROM ledger_rows WHERE episode = ?1 ORDER BY seq")?;
        let found = stmt
            .query_map([episode], read_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(found)
    }

    async fn promote(&self, lesson: &Lesson, cites: &[String]) -> Result<String> {
        let conn = self.guard()?;
        let seq = next_seq(&conn, "lessons")?;
        let id = new_id("les", seq);
        conn.execute(
            "INSERT INTO lessons(id, kind, trigger, mechanism, claim, applied, helped, seq)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                id,
                serde_json::to_string(&lesson.kind)
                    .map_err(|e| LedgerError::Corrupt(e.to_string()))?
                    .trim_matches('"'),
                lesson.trigger,
                lesson.mechanism,
                lesson.claim,
                i64::from(lesson.applied),
                i64::from(lesson.helped),
                seq,
            ],
        )?;
        for row_id in cites {
            conn.execute(
                "INSERT OR IGNORE INTO lesson_evidence(lesson_id, row_id) VALUES(?1,?2)",
                params![id, row_id],
            )?;
        }
        Ok(id)
    }

    async fn lessons(&self, kind: Option<LessonKind>) -> Result<Vec<Lesson>> {
        let conn = self.guard()?;
        let mut stmt = conn.prepare("SELECT * FROM lessons ORDER BY seq")?;
        let all = stmt
            .query_map([], |r| {
                Ok(Lesson {
                    id: r.get("id")?,
                    kind: LessonKind::parse(&r.get::<_, String>("kind")?),
                    trigger: r.get("trigger")?,
                    mechanism: r.get("mechanism")?,
                    claim: r.get("claim")?,
                    applied: r.get::<_, i64>("applied")?.try_into().unwrap_or(0),
                    helped: r.get::<_, i64>("helped")?.try_into().unwrap_or(0),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(match kind {
            Some(want) => all.into_iter().filter(|l| l.kind == want).collect(),
            None => all,
        })
    }

    async fn evidence(&self, lesson_id: &str) -> Result<Vec<LedgerRow>> {
        let conn = self.guard()?;
        let mut stmt = conn.prepare(
            "SELECT r.* FROM ledger_rows r
             JOIN lesson_evidence e ON e.row_id = r.id
             WHERE e.lesson_id = ?1 ORDER BY r.seq",
        )?;
        let found = stmt
            .query_map([lesson_id], read_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(found)
    }

    async fn score_lesson(&self, lesson_id: &str, helped: bool) -> Result<()> {
        let conn = self.guard()?;
        conn.execute(
            "UPDATE lessons SET applied = applied + 1, helped = helped + ?2 WHERE id = ?1",
            params![lesson_id, i64::from(helped)],
        )?;
        Ok(())
    }

    async fn score_workflow(&self, workflow_id: &str, helped: bool) -> Result<()> {
        let conn = self.guard()?;
        // Upsert: the first run of a workflow is the common case and must not
        // need a separate registration step.
        conn.execute(
            "INSERT INTO workflow_scores(workflow_id, applied, helped) VALUES(?1, 1, ?2)
             ON CONFLICT(workflow_id) DO UPDATE SET
                applied = applied + 1,
                helped  = helped + ?2",
            params![workflow_id, i64::from(helped)],
        )?;
        Ok(())
    }

    async fn workflow_score(&self, workflow_id: &str) -> Result<Score> {
        let conn = self.guard()?;
        let found = conn
            .query_row(
                "SELECT applied, helped FROM workflow_scores WHERE workflow_id = ?1",
                [workflow_id],
                |r| {
                    Ok(Score {
                        applied: r.get::<_, i64>(0)?.try_into().unwrap_or(0),
                        helped: r.get::<_, i64>(1)?.try_into().unwrap_or(0),
                    })
                },
            )
            .optional()?;
        Ok(found.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::conformance;

    #[tokio::test]
    async fn passes_the_conformance_suite() {
        let store = SqliteLedger::in_memory().expect("open in-memory ledger");
        conformance::run_all(&store).await;
    }

    #[tokio::test]
    async fn a_reopened_ledger_still_has_its_rows() {
        // The whole point of the sqlite backend over the in-memory one.
        let dir = std::env::temp_dir().join(format!("adaptive-ledger-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("ledger.db");
        let _ = std::fs::remove_file(&path);

        {
            let store = SqliteLedger::open(&path).expect("open");
            store
                .append(&conformance::row("ep", 1, "authored"))
                .await
                .expect("append");
        }
        let reopened = SqliteLedger::open(&path).expect("reopen");
        assert_eq!(reopened.rows("ep").await.expect("rows").len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn insertion_order_survives_a_timestamp_tie() {
        // Two attempts finishing in the same second is common; ordering by `at`
        // would make the exclusion list arbitrary.
        let store = SqliteLedger::in_memory().expect("open");
        for sig in ["first", "second", "third"] {
            let mut r = conformance::row("tie", 1, sig);
            r.at = "2026-01-01T00:00:00Z".to_string();
            store.append(&r).await.expect("append");
        }
        assert_eq!(
            store.tried("tie").await.expect("tried"),
            vec!["first", "second", "third"]
        );
    }
}

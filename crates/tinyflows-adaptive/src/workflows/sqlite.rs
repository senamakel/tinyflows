//! Workflows in the same sqlite file as the ledger.
//!
//! One file for everything durable, so a deployment backs up one thing and a
//! developer inspects one thing.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{Connection, params};
use tinyflows::store::types::{WorkflowError, WorkflowRecord};

use super::Vault;

const DDL: &str = "CREATE TABLE IF NOT EXISTS workflows (
        scope_key TEXT NOT NULL DEFAULT '',
        id        TEXT NOT NULL,
        document  TEXT NOT NULL,
        PRIMARY KEY (scope_key, id)
    )";

/// A vault backed by one sqlite file.
#[derive(Clone)]
pub struct SqliteVault {
    conn: Arc<Mutex<Connection>>,
    scope: Option<String>,
}

impl SqliteVault {
    /// Open (or create) a vault at `path`, creating the parent directory.
    ///
    /// # Errors
    /// When the file or its directory cannot be opened.
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Self, WorkflowError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .map_err(|e| WorkflowError::Engine(format!("{}: {e}", parent.display())))?;
        }
        Self::from_connection(Connection::open(path).map_err(sql)?)
    }

    /// A vault held entirely in memory. For tests.
    ///
    /// # Errors
    /// When the schema cannot be applied.
    pub fn in_memory() -> Result<Self, WorkflowError> {
        Self::from_connection(Connection::open_in_memory().map_err(sql)?)
    }

    fn from_connection(conn: Connection) -> Result<Self, WorkflowError> {
        conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")
            .ok();
        conn.execute(DDL, []).map_err(sql)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            scope: None,
        })
    }

    /// A handle onto the same file, scoped to one tenant.
    #[must_use]
    pub fn for_tenant(&self, scope: impl Into<String>) -> Self {
        Self {
            conn: Arc::clone(&self.conn),
            scope: Some(scope.into()),
        }
    }

    fn bucket(&self) -> String {
        self.scope.clone().unwrap_or_default()
    }

    fn guard(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn sql(err: rusqlite::Error) -> WorkflowError {
    WorkflowError::Engine(err.to_string())
}

#[async_trait]
impl Vault for SqliteVault {
    fn scope(&self) -> Option<&str> {
        self.scope.as_deref()
    }

    async fn load(&self) -> Result<Vec<WorkflowRecord>, WorkflowError> {
        let conn = self.guard();
        let mut stmt = conn
            .prepare(
                "SELECT document FROM workflows WHERE scope_key = ?1 OR scope_key = '' ORDER BY id",
            )
            .map_err(sql)?;
        let documents = stmt
            .query_map([self.bucket()], |r| r.get::<_, String>(0))
            .map_err(sql)?
            .collect::<rusqlite::Result<Vec<_>>>()
            .map_err(sql)?;

        documents
            .iter()
            .map(|document| {
                serde_json::from_str(document).map_err(|e| {
                    WorkflowError::Engine(format!("stored workflow no longer parses: {e}"))
                })
            })
            .collect()
    }

    async fn put(&self, record: &WorkflowRecord) -> Result<(), WorkflowError> {
        let document = serde_json::to_string(record)
            .map_err(|e| WorkflowError::Engine(format!("workflow will not serialize: {e}")))?;
        self.guard()
            .execute(
                "INSERT INTO workflows(scope_key, id, document) VALUES(?1,?2,?3)
                 ON CONFLICT(scope_key, id) DO UPDATE SET document = ?3",
                params![self.bucket(), record.id, document],
            )
            .map_err(sql)?;
        Ok(())
    }

    async fn remove(&self, id: &str) -> Result<(), WorkflowError> {
        self.guard()
            .execute(
                "DELETE FROM workflows WHERE scope_key = ?1 AND id = ?2",
                params![self.bucket(), id],
            )
            .map_err(sql)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "sqlite_tests.rs"]
mod tests;

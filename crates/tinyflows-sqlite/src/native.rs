//! The one way this crate reaches a SQLite file: the tinystoragedrivers
//! SQLite driver's native mode.
//!
//! [`run`] opens the driver's handle for a database file and runs a closure
//! on its connection. The driver keeps one connection per file per process
//! while anything holds it, so a host that already has the same file open
//! through the driver (a storage backend, another store) shares that
//! connection and its lock instead of contending for SQLite's file lock.
//! When nothing else holds it, each call opens the file fresh, exactly as
//! these stores always did — which is what lets a `flows.db` deleted under a
//! live process come back on the next call.
//!
//! The driver opens WAL with `synchronous = NORMAL` and a 5 s busy timeout:
//! a commit survives a process crash, but an OS crash or power loss can roll
//! back the most recent ones (the database stays consistent).

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;
use tinystoragedrivers_sqlite::SqliteNative;

/// Runs `f` on the driver's connection for the database file at `db_path`.
///
/// A transaction `f` leaves open — by returning an error, by panicking, or by
/// forgetting to commit — is rolled back before the lock is released, as
/// dropping a per-call connection always did. A panic is caught while the
/// lock is held and resumed after it is released, so one store's bug never
/// poisons the shared connection for every other caller in the process.
pub(crate) fn run<T>(db_path: &Path, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    let native = SqliteNative::open(db_path)
        .with_context(|| format!("Failed to open SQLite DB: {}", db_path.display()))?;
    let outcome = native
        .run_blocking(|conn| {
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(conn)));
            // However `f` ended — a panic, an error, even success — a
            // transaction it left open must not outlive the call: the
            // connection may be shared, and the next caller would inherit it.
            // A fresh connection per call used to drop it implicitly.
            if !conn.is_autocommit()
                && let Err(rollback) = conn.execute_batch("ROLLBACK")
            {
                tracing::warn!(
                    "[sqlite] rollback of a transaction a store call left open failed: {rollback}"
                );
            }
            outcome
        })
        .with_context(|| format!("SQLite connection unavailable: {}", db_path.display()))?;
    match outcome {
        Ok(result) => result,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;

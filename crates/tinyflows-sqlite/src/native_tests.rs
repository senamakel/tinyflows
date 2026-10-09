use super::*;

#[test]
fn a_call_runs_on_the_drivers_connection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("own.db");
    // Something else holds the file open through the driver ...
    let held = SqliteNative::open(&path).unwrap();
    held.run_blocking(|conn| conn.execute_batch("CREATE TABLE t (n INTEGER)"))
        .unwrap()
        .unwrap();
    // ... and a store call shares that connection and sees its schema.
    run(&path, |conn| {
        conn.execute("INSERT INTO t (n) VALUES (1)", [])
            .context("insert")?;
        Ok(())
    })
    .unwrap();
    let count: i64 = held
        .run_blocking(|conn| conn.query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0)))
        .unwrap()
        .unwrap();
    assert_eq!(count, 1);
}

#[test]
fn a_store_error_passes_through() {
    let dir = tempfile::tempdir().unwrap();
    let error = run(&dir.path().join("own.db"), |_| -> Result<()> {
        anyhow::bail!("store said no")
    })
    .unwrap_err();
    assert!(error.to_string().contains("store said no"), "{error}");
}

#[test]
fn a_panicking_call_leaves_the_shared_connection_usable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("own.db");
    let held = SqliteNative::open(&path).unwrap();
    let panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run(&path, |conn| -> Result<()> {
            conn.execute_batch("BEGIN IMMEDIATE; CREATE TABLE half (n INTEGER);")
                .context("half")?;
            panic!("store bug mid-transaction");
        })
    }));
    assert!(panicked.is_err(), "the panic still reaches the caller");
    let tables: i64 = held
        .run_blocking(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'half'",
                [],
                |row| row.get(0),
            )
        })
        .expect("the lock is not poisoned")
        .unwrap();
    assert_eq!(tables, 0, "the open transaction was rolled back");
}

#[test]
fn an_unopenable_path_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("file"), b"not a dir").unwrap();
    let error = run(&dir.path().join("file").join("own.db"), |_| Ok(())).unwrap_err();
    assert!(
        error.to_string().contains("Failed to open SQLite DB"),
        "{error}"
    );
}

#[test]
fn a_transaction_left_open_never_reaches_the_next_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("own.db");
    let held = SqliteNative::open(&path).unwrap();
    held.run_blocking(|conn| conn.execute_batch("CREATE TABLE t (n INTEGER)"))
        .unwrap()
        .unwrap();
    // An error mid-transaction, and a success that forgot to commit.
    let failed = run(&path, |conn| -> Result<()> {
        conn.execute_batch("BEGIN; INSERT INTO t (n) VALUES (1);")
            .context("insert")?;
        anyhow::bail!("store said no")
    });
    assert!(failed.is_err());
    run(&path, |conn| {
        conn.execute_batch("BEGIN; INSERT INTO t (n) VALUES (2);")
            .context("insert")?;
        Ok(())
    })
    .unwrap();
    // The next call starts its own transaction and sees neither write.
    let count: i64 = run(&path, |conn| {
        conn.execute_batch("BEGIN IMMEDIATE").context("begin")?;
        let count = conn
            .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
            .context("count")?;
        conn.execute_batch("COMMIT").context("commit")?;
        Ok(count)
    })
    .unwrap();
    assert_eq!(count, 0);
}

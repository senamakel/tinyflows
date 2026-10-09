# tinyflows-drivers

`tinyflows` run state and graph checkpoints on
[tinystoragedrivers](https://github.com/tinyhumansai/tinystoragedrivers)
document ports. A host that has opened a storage backend (SQLite on a desktop,
MongoDB in the cloud, memory in tests) gets the engine's durable pieces on it,
scoped per tenant by the document handle it passes in.

| Type | Implements | Layout |
| --- | --- | --- |
| `DriverStateStore` | `tinyflows::caps::StateStore` | one document per key in `flows_state` |
| `DriverCheckpointer<State>` | `tinyflows::graph::Checkpointer<State>` | `flows_graph_checkpoints`, `_threads`, `_writes` |
| `schedule::CronDocuments` | the cron job store of `tinyflows_sqlite::schedule` | `cron_jobs`, `cron_runs`, `cron_counters` |

## Features

| Feature | What it adds | Needs |
| --- | --- | --- |
| `engine` (default) | `DriverStateStore`, `DriverCheckpointer` | the `tinyflows` engine |
| `schedule` | `schedule::CronDocuments` | `tinyflows-schedule` only |

A host that runs scheduled jobs but no flows builds with
`default-features = false, features = ["schedule"]` and pulls neither the
engine nor its expression stack.

```rust
use std::sync::Arc;
use tinyflows_drivers::{DriverCheckpointer, DriverStateStore};

let docs = Arc::clone(scoped_storage.documents());
let state = Arc::new(DriverStateStore::new(Arc::clone(&docs)));
let checkpointer = DriverCheckpointer::<serde_json::Value>::new(docs);
```

## Cron store

`CronDocuments` has the same operations as `tinyflows_sqlite::schedule` (same
names and arguments minus the options struct, same results and error
messages), as `async` methods:

```rust
use tinyflows_drivers::schedule::CronDocuments;

let cron = CronDocuments::new(docs).with_limits(max_run_history, max_tasks);
let job = cron.add_job("0 9 * * *", "echo hi").await?;
for due in cron.due_jobs(chrono::Utc::now()).await? { /* run it */ }
```

Across processes sharing one database:

- every job update is compare-and-swap, so concurrent writers never lose a
  field;
- `reschedule_after_run` advances `next_run` only from the occurrence the
  caller fired, so it never advances twice or overwrites an edited schedule;
- a flow's schedule job has a deterministic id written only if absent, so
  registering it is idempotent;
- run numbers come from a compare-and-swap counter.

`due_jobs` is a read, so two schedulers can both run the same due job (as
with two processes on one SQLite file); recording a run and pruning history
are separate writes.

## Checkpointer guarantees

- One document per checkpoint with a per-thread `seq` advanced by
  compare-and-swap, so listing is insertion order and a re-used checkpoint id
  resolves to its latest write, as in the SQLite and file backends.
- `get_scoped` is one indexed `(thread, namespace, seq)` query, so a parent
  run and its subgraphs never load each other's checkpoints, even when ids
  repeat.
- `state_history` reads the namespace's checkpoints and pending writes in two
  queries and walks the lineage in memory, rather than two round trips per
  ancestor.
- Pending writes merge with `merge_writes` under compare-and-swap.
- Key components are length-prefixed and namespaces encoded injectively; ids
  over 400 bytes become their SHA-256.

## Why a separate crate

`tinystoragedrivers` is a git dependency: OpenHuman vendors it once (through
TinyAgents) and points the URL at that copy with
`[patch."https://github.com/tinyhumansai/tinystoragedrivers"]`, so there is
exactly one `DocumentStore` trait in its graph. The published `tinyflows`
package cannot carry a git dependency, so these adapters live here
(`publish = false`). The engine stays dependency-free and keeps its MSRV; this
crate needs Rust 1.88, the storage crates' MSRV.

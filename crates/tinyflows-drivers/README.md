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

```rust
use std::sync::Arc;
use tinyflows_drivers::{DriverCheckpointer, DriverStateStore};

let docs = Arc::clone(scoped_storage.documents());
let state = Arc::new(DriverStateStore::new(Arc::clone(&docs)));
let checkpointer = DriverCheckpointer::<serde_json::Value>::new(docs);
```

## Features

| Feature | Default | Brings |
| --- | --- | --- |
| `engine` | yes | `DriverStateStore`, `DriverCheckpointer` (needs `tinyflows`) |
| `catalog` | no | `catalog::FlowCatalogDocuments`, `catalog::FlowStateDocuments` |
| `schedule` | no | `schedule::CronDocuments`; needs only `tinyflows-schedule`, not the engine |
| `test-fixtures` | no | the catalog's two fixture-only writers |

A host that runs scheduled jobs but no flows builds with
`default-features = false, features = ["schedule"]` and pulls neither the
engine nor its expression stack.

## The flow catalog (`catalog`)

`FlowCatalogDocuments` is the document-port counterpart of
`tinyflows-sqlite`'s `flows` and `drafts`: one `async fn` per public function
there, same name, same arguments minus the catalog directory, same results
(`update_flow_graph` reports the shared `tinyflows_catalog::store::FlowUpdateError`).

```rust
use std::sync::Arc;
use tinyflows_drivers::catalog::{FlowCatalogDocuments, FlowStateDocuments};

let catalog = FlowCatalogDocuments::new(Arc::clone(scoped_storage.documents()));
let flow = catalog.create_flow("Digest".into(), graph, true, false).await?;
let state = Arc::new(FlowStateDocuments::new(catalog.clone(), flow.id.clone()));
```

| Collection | One document per |
| --- | --- |
| `flows_definitions` | flow |
| `flows_revisions` | superseded graph (newest 20 kept) |
| `flows_runs` | run, with its steps |
| `flows_kv` | `(namespace, key)` of flow state |
| `flows_suggestions` | discovery suggestion |
| `flows_drafts` | authoring draft |

- Every guarded SQL `UPDATE … WHERE status = …` (finish, resume, interrupt,
  TTL expiry) is a compare-and-swap on the run, so a transition happens at
  most once across processes sharing one database.
- A run's steps live on the run document (`steps_json`, as in SQLite), so a
  run settles with its step list in one compare-and-swap, and a step written
  by a parallel branch retries on the winner's list rather than losing to it
  (what SQLite's `BEGIN IMMEDIATE`, R-m1, protected).
- `update_flow_graph` writes the prior graph's revision *pending*, swaps the
  graph naming it (`last_revision_id`), then confirms it. A revision shows
  only once confirmed or named, so a lost race or a crash never surfaces a
  revision for an update that did not happen; `updated_at` advances strictly
  on every write, so it is a sound concurrency token and a total order.
- Removing a flow removes its runs and revisions first and the definition
  last (a failure part-way is finished by calling it again); a run inserted
  concurrently is swept or removes itself.
- Graphs, steps, drafts and state values are JSON strings, optional fields
  are omitted rather than `null`, and ordering uses epoch-nanosecond fields.

`FlowStateDocuments` is one flow's namespaced state over the same `flows_kv`
documents as `kv_get` / `kv_set` / `kv_delete`. It is the engine's
`StateStore` and the host's synchronous `DedupKv`; the latter runs on the
storage crates' `Blocking` bridge (one per process, or the host's own via
`with_bridge`), so it is safe inside any runtime. `DriverStateStore` is
unchanged: un-namespaced, for a host that gives each run its own collection.

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
  registering it is idempotent, and its `command` (the flow it fires) cannot
  be patched to another flow in either store (`tinyflows_schedule::check_patch`);
- run numbers come from a compare-and-swap counter.

`due_jobs` is a read, so two schedulers can both run the same due job (as
with two processes on one SQLite file); it orders by `next_run_ns`, falling
back to `next_run_ms` for documents written before that field. Recording a
run and pruning history are separate writes, and so are removing a job and
its runs; `sweep_orphan_runs` (run by `clear_all_jobs`) collects runs whose
job is gone or was re-created.

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

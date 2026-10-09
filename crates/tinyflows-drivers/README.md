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

```rust
use std::sync::Arc;
use tinyflows_drivers::{DriverCheckpointer, DriverStateStore};

let docs = Arc::clone(scoped_storage.documents());
let state = Arc::new(DriverStateStore::new(Arc::clone(&docs)));
let checkpointer = DriverCheckpointer::<serde_json::Value>::new(docs);
```

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

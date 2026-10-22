//! Storage declarations and legacy imports for `DriverCheckpointer`.
//!
//! The graph checkpointer remains generic; TinyFlows owns the schema because
//! its driver is the only remaining production user of these collections.

use serde_json::Value;
use tinystoragedrivers_core::{
    CollectionSpec, IndexSpec, Key, Owner, Schema, ScopeClass, SourceRecord, Step, TargetRecord,
};

const VERSION: u32 = 4;
const CHECKPOINTS: &str = "graph.checkpoints";
const THREADS: &str = "graph.threads";
const WRITES: &str = "graph.writes";

struct Legacy {
    namespace: &'static str,
    checkpoints: &'static str,
    threads: &'static str,
    writes: &'static str,
}

const LEGACY: [Legacy; 3] = [
    Legacy {
        namespace: "graph",
        checkpoints: "graph_checkpoints",
        threads: "graph_threads",
        writes: "graph_writes",
    },
    Legacy {
        namespace: "delegation",
        checkpoints: "delegation_graph_checkpoints",
        threads: "delegation_graph_threads",
        writes: "delegation_graph_writes",
    },
    Legacy {
        namespace: "flows",
        checkpoints: "flows_graph_checkpoints",
        threads: "flows_graph_threads",
        writes: "flows_graph_writes",
    },
];

/// Schema for the one durable checkpointer, including pre-v2 checkpoint
/// collections written by TinyAgents graph consumers.
#[must_use]
pub fn graph_checkpoint_schema() -> Schema {
    let owner = Owner::new("graph").expect("`graph` is a valid owner");
    let mut schema = Schema::new(owner)
        .collection_with(
            VERSION,
            CollectionSpec::new(CHECKPOINTS)
                .index(IndexSpec::new("by_thread_seq", ["ns", "thread", "seq"]))
                .index(IndexSpec::new(
                    "by_thread_id",
                    ["ns", "thread", "checkpoint_id"],
                ))
                .index(IndexSpec::new(
                    "by_thread_subgraph",
                    ["ns", "thread", "subgraph", "seq"],
                )),
            ScopeClass::Agent,
        )
        .collection_with(
            VERSION,
            CollectionSpec::new(THREADS).index(IndexSpec::new("by_ns", ["ns"])),
            ScopeClass::Agent,
        )
        .collection_with(
            VERSION,
            CollectionSpec::new(WRITES).index(IndexSpec::new("by_thread", ["ns", "thread"])),
            ScopeClass::Agent,
        );
    for (index, legacy) in LEGACY.iter().enumerate() {
        let from = 1 + u32::try_from(index).unwrap_or(0);
        schema = schema
            .step(checkpoint_step(from, legacy))
            .step(thread_step(from, legacy))
            .step(writes_step(from, legacy));
    }
    schema
}

fn checkpoint_step(from: u32, legacy: &'static Legacy) -> Step {
    Step::new(
        [
            "checkpoints_graph_v2",
            "checkpoints_delegation_v2",
            "checkpoints_flows_v2",
        ][usize::try_from(from - 1).unwrap_or(0)],
        CHECKPOINTS,
        from,
        from + 1,
        move |rows| {
            Ok(rows
                .iter()
                .filter_map(|source| checkpoint_row(legacy.namespace, source))
                .collect())
        },
    )
    .source(legacy.checkpoints)
}

fn thread_step(from: u32, legacy: &'static Legacy) -> Step {
    Step::new(
        [
            "threads_graph_v2",
            "threads_delegation_v2",
            "threads_flows_v2",
        ][usize::try_from(from - 1).unwrap_or(0)],
        THREADS,
        from,
        from + 1,
        move |rows| {
            Ok(rows
                .iter()
                .filter_map(|source| {
                    let thread = text(&source.body, "thread")?;
                    let next = source.body.get("next")?.as_u64()?;
                    Some(target(
                        THREADS,
                        key(&[legacy.namespace, &thread]),
                        serde_json::json!({"ns": legacy.namespace, "thread": thread, "next": next}),
                        source.meta.created_ms,
                    ))
                })
                .collect())
        },
    )
    .source(legacy.threads)
}

fn writes_step(from: u32, legacy: &'static Legacy) -> Step {
    Step::new(
        ["writes_graph_v2", "writes_delegation_v2", "writes_flows_v2"]
            [usize::try_from(from - 1).unwrap_or(0)],
        WRITES,
        from,
        from + 1,
        move |rows| {
            Ok(rows
                .iter()
                .filter_map(|source| {
                    let thread = text(&source.body, "thread")?;
                    let checkpoint_id = text(&source.body, "checkpoint_id")?;
                    let namespace = source
                        .body
                        .get("namespace")
                        .and_then(Value::as_array)
                        .map(|parts| namespace_key(parts))
                        .unwrap_or_default();
                    let writes = source.body.get("writes")?.clone();
                    let id = key(&[legacy.namespace, &thread, &namespace, &checkpoint_id]);
                    Some(target(
                        WRITES,
                        id,
                        serde_json::json!({
                            "ns": legacy.namespace,
                            "thread": thread,
                            "subgraph": namespace,
                            "checkpoint_id": checkpoint_id,
                            "writes": writes,
                        }),
                        source.meta.created_ms,
                    ))
                })
                .collect())
        },
    )
    .source(legacy.writes)
}

fn checkpoint_row(namespace: &str, source: &SourceRecord) -> Option<TargetRecord> {
    let thread = text(&source.body, "thread")?;
    let checkpoint = source.body.get("record")?.clone();
    if !checkpoint.is_object() {
        return None;
    }
    let sequence = source.body.get("seq")?.as_u64()?;
    let checkpoint_id = text(&source.body, "checkpoint_id")?;
    let subgraph = checkpoint
        .get("namespace")
        .and_then(Value::as_array)
        .map(|parts| namespace_key(parts))
        .unwrap_or_default();
    let id = key(&[namespace, &thread, &format!("{sequence:020}")]);
    let created_ms = checkpoint
        .get("created_at")
        .and_then(Value::as_i64)
        .filter(|created_ms| *created_ms > 0)
        .unwrap_or(source.meta.created_ms);
    Some(target(
        CHECKPOINTS,
        id,
        serde_json::json!({
            "ns": namespace,
            "thread": thread,
            "subgraph": subgraph,
            "seq": sequence,
            "checkpoint_id": checkpoint_id,
            "checkpoint": checkpoint,
        }),
        created_ms,
    ))
}

fn text(body: &Value, field: &str) -> Option<String> {
    body.get(field)?.as_str().map(str::to_owned)
}

fn target(collection: &str, id: String, body: Value, created_ms: i64) -> TargetRecord {
    let record = TargetRecord::new(collection, id, body);
    if created_ms > 0 {
        record.created_ms(created_ms)
    } else {
        record
    }
}

fn key(parts: &[&str]) -> String {
    Key::of(parts)
        .map(|key| key.as_str().to_owned())
        .unwrap_or_default()
}

fn namespace_key(parts: &[Value]) -> String {
    let parts = parts.iter().filter_map(Value::as_str).collect::<Vec<_>>();
    if parts.is_empty() {
        String::new()
    } else {
        Key::of(std::iter::once("n").chain(parts))
            .map(|key| key.as_str().to_owned())
            .unwrap_or_default()
    }
}

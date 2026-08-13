use super::*;

pub(super) fn contract_scatter() -> NodeKindContract {
    NodeKindContract {
        kind: "scatter".to_string(),
        summary: "Fans the DOWNSTREAM PATH out into parallel lanes — every node between here \
                      and the matching `gather` runs once per lane."
            .to_string(),
        description: "Different from an ordinary fan-out, which runs each SUCCESSOR once. \
                          Drawing two edges from one port runs both successors concurrently; a \
                          scatter runs the whole pipeline once per lane, so \
                          scatter -> enrich -> score -> gather over 8 items becomes 8 concurrent \
                          three-node pipelines. Use it when per-item work spans several nodes; \
                          for per-item work inside ONE node, that node's own `concurrency` is \
                          simpler."
            .to_string(),
        config_fields: vec![
            ConfigField::optional(
                "path",
                "string",
                "Dotted path to an array in the first input item to fan out over (like \
                     split_out). Without it, the node's own input items are the lanes.",
            ),
            ConfigField::optional(
                "lanes",
                "number",
                "Chunk the work into at most this many lanes instead of one per item \
                     (clamped to 256). A 1000-item input can then run 8 wide rather than 1000 \
                     wide without pre-chunking.",
            ),
        ],
        ports: PortSpec::new(&["main"], &["main"]),
        example: json!({
            "id": "fan", "kind": "scatter", "name": "One lane per repo",
            "config": { "path": "repos", "lanes": 8 }
        }),
        notes: vec![
            "Must reach a `gather`, and every node in between must have a path onward to it. \
                 A lane that dead-ends is not merely uncollected — a lane activation never \
                 writes the node's top-level slot, so its output is invisible."
                .to_string(),
            "Lane workers expose \"=nodes.<id>.lanes.<lane>\", NOT \"=nodes.<id>.item\": \
                 inside a region there is no single value for that node. Read the gather's \
                 aggregated output instead."
                .to_string(),
            "Not supported inside a lane (each refused by validation): a nested `scatter`, a \
                 `loop` head, or `requires_approval`. The last because a resume is addressed by \
                 node id, so every lane would share one approval."
                .to_string(),
            "`max_node_visits` is charged PER LANE ACTIVATION, so a wide scatter needs \
                 headroom on that and on `recursion_limit`."
                .to_string(),
        ],
    }
}

pub(super) fn contract_gather() -> NodeKindContract {
    NodeKindContract {
        kind: "gather".to_string(),
        summary: "Collects the lanes a `scatter` opened, on a release policy.".to_string(),
        description: "Not a topological barrier. A `merge` waits for its declared \
                          predecessors — a static fact about the graph — but how many lanes exist \
                          is decided at run time from data, so a gather counts arrivals against \
                          the lane count the scatter recorded and re-checks until its release \
                          policy is satisfied. That is also why it supports the same policies as \
                          `gate`: once waiting is a decision rather than a topological fact, \
                          \"proceed on a quorum\" becomes expressible."
            .to_string(),
        config_fields: vec![
            ConfigField::required(
                "from",
                "array",
                "Ids of the lane-terminal nodes whose lane slots to collect — the last node \
                     of the lane body.",
            ),
            ConfigField::optional(
                "release",
                "enum",
                "When to proceed: all (default) | any | first_n | quorum | timeout_partial.",
            )
            .with_enum(&["all", "any", "first_n", "quorum", "timeout_partial"]),
            ConfigField::optional("n", "number", "Required (and > 0) for first_n and quorum."),
            ConfigField::optional(
                "on_lane_error",
                "enum",
                "collect (default: a failed lane becomes an item with {failed, error, lane}) \
                     | skip (drop it) | fail_fast (fail the gather).",
            )
            .with_enum(&["collect", "skip", "fail_fast"]),
            ConfigField::optional(
                "poll_interval_ms",
                "number",
                "Gap between checks (default 5).",
            ),
            ConfigField::optional(
                "max_polls",
                "number",
                "Check budget before the wait is called spent (default 500). Each check costs \
                     a super-step.",
            ),
        ],
        ports: PortSpec::new(&["main"], &["main", "error"]),
        example: json!({
            "id": "collect", "kind": "gather", "name": "Collect the lanes",
            "config": { "from": ["score"], "release": "all", "on_lane_error": "collect" }
        }),
        notes: vec![
            "Output is ordered by LANE INDEX, not by which lane finished first, and each \
                 item keeps its lane index as `paired_item`. Two runs therefore emit the same \
                 order whatever the timing."
                .to_string(),
            "A partial release (any / first_n / quorum) leaves the remaining lanes running; \
                 their results are simply not collected."
                .to_string(),
        ],
    }
}

pub(super) fn contract_void() -> NodeKindContract {
    NodeKindContract {
        kind: "void".to_string(),
        summary: "Terminal sink: accepts items, discards them, and runs nothing downstream."
            .to_string(),
        description: "The branch ends here, on purpose. A branch could always dead-end — a \
                          node with no outgoing edges terminates — but an unwired port reads \
                          exactly like a forgotten one, so there was no way to SAY \"this is a \
                          side effect and nothing waits on it\". This node is that sentence. It \
                          adds no concurrency: the work upstream still runs inline in its own \
                          super-step, and only the result is dropped. For work that should \
                          actually overlap, use `spawn`."
            .to_string(),
        config_fields: vec![],
        ports: PortSpec::new(&["main"], &[]),
        example: json!({
            "id": "drop", "kind": "void", "name": "Fire and forget: audit log",
            "config": {}
        }),
        notes: vec![
            "An outgoing edge is a validation error, and so is having no incoming edge. \
                 `on_error: \"route\"` is refused for the same reason — an `error` edge is an \
                 outgoing edge; use \"stop\" or \"continue\"."
                .to_string(),
            "It writes {items: [], port: null, discarded: N} into its slot. A node that never \
                 ran has no slot at all, so \"never activated\", \"activated with nothing to \
                 drop\" (discarded: 0) and \"dropped N items\" stay distinguishable."
                .to_string(),
            "`discarded` counts THIS activation, not a running total: inside a loop body the \
                 last iteration's value is what survives, and inside a scatter lane it lands \
                 under lanes.<lane> rather than at the top level."
                .to_string(),
            "spawn -> void is the explicit spelling of a ticket no `gate` will ever collect. \
                 Same abandon semantics as leaving it unwired, but said out loud."
                .to_string(),
            "It is the one dead end a scatter lane may have. Every other lane branch must \
                 reach the `gather`, because a stranded lane's output is invisible rather than \
                 merely uncollected — a void makes that invisibility the contract."
                .to_string(),
            "There is no `reason` config. The node's `name` is where the human explanation \
                 goes, and unlike a config key it is rendered in the graph."
                .to_string(),
        ],
    }
}

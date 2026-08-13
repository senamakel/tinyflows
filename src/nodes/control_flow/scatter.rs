//! The `scatter` node: fan the *downstream path* out into parallel lanes.
//!
//! # How this differs from an ordinary fan-out
//!
//! Drawing two edges from one port already runs both successors concurrently.
//! What that cannot express is running the same *pipeline* several times over
//! different data: `split_out → agent → score → merge` runs `agent` once with
//! N items, not N times with one item each. Widening `agent`'s own per-item
//! concurrency helps only that node — `score` still sees the whole batch.
//!
//! A scatter opens **lanes**. Every node between it and its [`gather`] runs once
//! per lane, so a five-node pipeline becomes N concurrent five-node pipelines,
//! each carrying its own slice.
//!
//! [`gather`]: super::gather

use async_trait::async_trait;
use serde_json::{Value, json};

use crate::data::Item;
use crate::error::{EngineError, Result};
use crate::nodes::{NodeContext, NodeExecutor, NodeOutput};

/// The ceiling on how many lanes one scatter may open.
///
/// Clamped rather than refused, matching how per-item `concurrency` is treated:
/// a graph asking for a lane per row of a 100k-row table has a mistake in it,
/// and running it sensibly while saying so beats refusing the run outright.
pub const MAX_LANES: usize = 256;

/// Fans the downstream path out into one lane per slice of its input.
#[derive(Debug, Default, Clone)]
pub struct ScatterNode;

/// Splits `items` into at most `lanes` slices, keeping input order.
///
/// With `lanes` unset every item gets its own lane — the common intent, and
/// what makes a scatter feel like "run this per item". With `lanes: n` the input
/// is chunked into at most `n` slices, so a 1000-item input can run 8 wide
/// instead of 1000 wide without the author pre-chunking it.
fn split(items: &[Item], lanes: Option<usize>) -> Vec<Vec<Item>> {
    if items.is_empty() {
        return Vec::new();
    }
    let requested = lanes.unwrap_or(items.len()).clamp(1, MAX_LANES);
    if requested >= items.len() {
        return items.iter().map(|item| vec![item.clone()]).collect();
    }
    // Ceiling division, so the last chunk is the short one rather than the
    // split producing more chunks than lanes were asked for.
    let per_lane = items.len().div_ceil(requested);
    items.chunks(per_lane).map(<[Item]>::to_vec).collect()
}

#[async_trait]
impl NodeExecutor for ScatterNode {
    async fn execute(&self, ctx: NodeContext<'_>) -> Result<NodeOutput> {
        // A scatter nested inside a lane would need lane ids that compose and a
        // gather that knows which level it is closing. Refused rather than
        // silently mis-collected; `validate` catches the static case, and this
        // covers a graph that reached the engine another way.
        if ctx.lane.is_some() {
            return Err(EngineError::Capability(format!(
                "scatter node {:?}: nested scatter is not supported — this activation is \
                 already inside a lane",
                ctx.node.id
            )));
        }

        // Which items to fan out over. `path` reads an array out of the first
        // item (like `split_out`) for the common "one lane per row of this
        // field" shape; without it the node's own input items are the lanes.
        let items: Vec<Item> = match ctx.node.config.get("path").and_then(Value::as_str) {
            Some(path) => {
                let source = ctx.input.first().map(|item| &item.json);
                let array = path
                    .split('.')
                    .fold(source, |value, segment| value.and_then(|v| v.get(segment)))
                    .and_then(Value::as_array);
                match array {
                    Some(values) => values.iter().cloned().map(Item::new).collect(),
                    // Not an array: one lane carrying the input unchanged, the
                    // same fail-soft `split_out` uses.
                    None => ctx.input.to_vec(),
                }
            }
            None => ctx.input.to_vec(),
        };

        let requested = ctx
            .node
            .config
            .get("lanes")
            .and_then(Value::as_u64)
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| *n > 0);
        if let Some(n) = requested
            && n > MAX_LANES
        {
            tracing::warn!(
                node = %ctx.node.id,
                requested = n,
                max = MAX_LANES,
                "scatter lanes above the engine ceiling; clamping"
            );
        }

        let lanes = split(&items, requested);
        tracing::debug!(
            node = %ctx.node.id,
            lanes = lanes.len(),
            items = items.len(),
            "scatter: splitting work into lanes"
        );
        Ok(
            NodeOutput::scatter(lanes.clone(), json!({ "lane_count": lanes.len() }))
                // The scatter's own slot keeps the items it fanned out, so the run
                // state still shows what went in even though the lanes carry the work.
                .with_meta(json!({ "lane_count": lanes.len() })),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn items(n: usize) -> Vec<Item> {
        (0..n).map(|i| Item::new(json!({ "i": i }))).collect()
    }

    #[test]
    fn without_a_lane_count_every_item_gets_its_own_lane() {
        let lanes = split(&items(4), None);
        assert_eq!(lanes.len(), 4);
        assert!(lanes.iter().all(|lane| lane.len() == 1));
    }

    #[test]
    fn a_lane_count_chunks_the_input_and_preserves_order() {
        let lanes = split(&items(7), Some(3));
        assert_eq!(lanes.len(), 3, "at most the requested number of lanes");
        let flattened: Vec<i64> = lanes
            .iter()
            .flatten()
            .filter_map(|item| item.json["i"].as_i64())
            .collect();
        assert_eq!(
            flattened,
            (0..7).collect::<Vec<i64>>(),
            "chunking must not reorder the work"
        );
    }

    /// Asking for more lanes than there are items yields one lane each, not a
    /// pile of empty lanes a gather would then wait on forever.
    #[test]
    fn more_lanes_than_items_yields_one_lane_per_item() {
        let lanes = split(&items(2), Some(9));
        assert_eq!(lanes.len(), 2);
    }

    #[test]
    fn an_empty_input_opens_no_lanes() {
        assert!(split(&[], None).is_empty());
        assert!(split(&[], Some(4)).is_empty());
    }

    #[test]
    fn the_lane_count_is_clamped_to_the_ceiling() {
        let lanes = split(&items(MAX_LANES + 50), Some(MAX_LANES + 50));
        assert!(
            lanes.len() <= MAX_LANES,
            "opened {} lanes, above the ceiling",
            lanes.len()
        );
    }
}

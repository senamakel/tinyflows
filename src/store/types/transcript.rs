//! One line of what an agent did, as a run record keeps it.
//!
//! Part of the stored model rather than of the engine: nothing in
//! [`crate::engine`] produces these, and nothing in it reads them. A host that
//! runs `agent` nodes against something with an event stream folds that stream
//! into these entries and hangs them off a [`RunStep`](super::RunStep), so a run
//! read back tomorrow still says what happened inside a step and not only
//! whether it passed.
//!
//! Deliberately flat and stringly-typed. Mirroring a host's own event
//! vocabulary into the record would make every event kind it adds later a
//! breaking change to a file format that must stay readable by older builds. A
//! reader meeting an unfamiliar `kind` still has a timestamp and a line of text
//! to render.

use serde::{Deserialize, Serialize};

/// Bytes of one entry's `text` kept on the durable record.
///
/// [`RunRecord`](super::RunRecord) bounds step `input`, `output`, and its own
/// `inputs` through `bounded_within` so no single value can grow a run record
/// without limit; a transcript entry is the same kind of host-produced text
/// (a tool result, a model message) and needs the same ceiling. Small on
/// purpose — a transcript is many short lines, not one large payload, and a
/// step with hundreds of entries must not turn one long one into the whole
/// record's size budget.
pub const MAX_ENTRY_TEXT_BYTES: usize = 4 * 1024;

/// One thing an agent did, in the order it did it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEntry {
    /// Epoch milliseconds, as the host stamped the event.
    pub at_ms: i64,
    /// The host event kind this was folded from — `agent_message`, `tool_call`,
    /// `tool_result`, `agent_thinking`, `error`, and so on.
    ///
    /// Carried verbatim rather than mapped to a closed set, so a kind added to
    /// a host's wire vocabulary later shows up here without a change to this
    /// file.
    pub kind: String,
    /// The renderable line: the message text, the tool's one-line summary, the
    /// error message.
    pub text: String,
}

impl TranscriptEntry {
    /// Build an entry with `text` capped at [`MAX_ENTRY_TEXT_BYTES`].
    ///
    /// Nothing in this crate folds a host's event stream into these — that
    /// happens entirely on the host side, as the module doc says — so this is
    /// the bound a host's folding code is expected to apply per entry, the way
    /// [`bounded_within`](super::bounded_within) bounds the record's other
    /// host-produced text.
    #[must_use]
    pub fn bounded(at_ms: i64, kind: impl Into<String>, text: impl Into<String>) -> Self {
        let mut text = text.into();
        if text.len() > MAX_ENTRY_TEXT_BYTES {
            let end = text
                .char_indices()
                .map(|(index, _)| index)
                .take_while(|index| *index <= MAX_ENTRY_TEXT_BYTES)
                .last()
                .unwrap_or(0);
            text.truncate(end);
            text.push_str(" …[truncated]");
        }
        Self {
            at_ms,
            kind: kind.into(),
            text,
        }
    }
}

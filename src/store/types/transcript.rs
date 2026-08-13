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

//! Versioned messages exchanged with the TinyFlows Chrome extension.

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// The first supported browser relay protocol version.
pub const BROWSER_PROTOCOL_VERSION: u32 = 1;

/// One browser operation sent to an explicitly shared Chrome tab.
///
/// The enum is internally tagged, so workflow arguments use the direct form
/// `{ "action": "click", "selector": "button" }`. Unknown actions and
/// unknown fields are rejected instead of being silently ignored.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserAction {
    /// Navigate the shared tab to `url`.
    Open {
        /// The HTTP(S) URL to open.
        url: String,
    },
    /// Capture a structured accessibility/DOM snapshot.
    Snapshot,
    /// Click the element matching `selector`.
    Click {
        /// A CSS selector in the current document.
        selector: String,
    },
    /// Replace an input's current value.
    Fill {
        /// A CSS selector for the input.
        selector: String,
        /// The complete replacement value.
        value: String,
    },
    /// Type text, optionally into a selected element.
    Type {
        /// A CSS selector for the target, or the focused element when omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
        /// Text to enter as keyboard input.
        text: String,
    },
    /// Read visible text from an element or the whole document.
    GetText {
        /// A CSS selector, or the document when omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
    },
    /// Read the current document title.
    GetTitle,
    /// Read the current document URL.
    GetUrl,
    /// Capture a PNG screenshot.
    Screenshot {
        /// A CSS selector to capture, or the viewport when omitted.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
        /// Capture the full document rather than only the viewport.
        #[serde(default)]
        full_page: bool,
    },
    /// Wait for a duration and/or for an element to appear.
    Wait {
        /// Delay in milliseconds before completing.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        duration_ms: Option<u64>,
        /// A CSS selector that must become present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        selector: Option<String>,
    },
    /// Send a Chrome DevTools key name such as `Enter`.
    Press {
        /// The key to press.
        key: String,
    },
    /// Move the pointer over an element.
    Hover {
        /// A CSS selector for the target.
        selector: String,
    },
    /// Scroll the document by the supplied pixel deltas.
    Scroll {
        /// Horizontal delta in CSS pixels.
        #[serde(default)]
        x: i64,
        /// Vertical delta in CSS pixels.
        #[serde(default)]
        y: i64,
    },
    /// Test whether an element is currently visible.
    IsVisible {
        /// A CSS selector for the target.
        selector: String,
    },
    /// Close the explicitly shared tab.
    Close,
    /// Semantically locate an element by human-readable text or role.
    Find {
        /// Human-readable description to locate.
        query: String,
    },
}

#[derive(Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum BrowserActionUnchecked {
    Open {
        url: String,
    },
    Snapshot,
    Click {
        selector: String,
    },
    Fill {
        selector: String,
        value: String,
    },
    Type {
        #[serde(default)]
        selector: Option<String>,
        text: String,
    },
    GetText {
        #[serde(default)]
        selector: Option<String>,
    },
    GetTitle,
    GetUrl,
    Screenshot {
        #[serde(default)]
        selector: Option<String>,
        #[serde(default)]
        full_page: bool,
    },
    Wait {
        #[serde(default)]
        duration_ms: Option<u64>,
        #[serde(default)]
        selector: Option<String>,
    },
    Press {
        key: String,
    },
    Hover {
        selector: String,
    },
    Scroll {
        #[serde(default)]
        x: i64,
        #[serde(default)]
        y: i64,
    },
    IsVisible {
        selector: String,
    },
    Close,
    Find {
        query: String,
    },
}

impl<'de> Deserialize<'de> for BrowserAction {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        let object = value
            .as_object()
            .ok_or_else(|| serde::de::Error::custom("browser action must be an object"))?;
        let action = object.get("action").and_then(Value::as_str);
        if matches!(action, Some("snapshot" | "get_title" | "get_url" | "close"))
            && object.len() != 1
        {
            return Err(serde::de::Error::custom(
                "unit browser actions accept only the `action` field",
            ));
        }
        serde_json::from_value::<BrowserActionUnchecked>(value)
            .map(Into::into)
            .map_err(serde::de::Error::custom)
    }
}

impl From<BrowserActionUnchecked> for BrowserAction {
    fn from(value: BrowserActionUnchecked) -> Self {
        match value {
            BrowserActionUnchecked::Open { url } => Self::Open { url },
            BrowserActionUnchecked::Snapshot => Self::Snapshot,
            BrowserActionUnchecked::Click { selector } => Self::Click { selector },
            BrowserActionUnchecked::Fill { selector, value } => Self::Fill { selector, value },
            BrowserActionUnchecked::Type { selector, text } => Self::Type { selector, text },
            BrowserActionUnchecked::GetText { selector } => Self::GetText { selector },
            BrowserActionUnchecked::GetTitle => Self::GetTitle,
            BrowserActionUnchecked::GetUrl => Self::GetUrl,
            BrowserActionUnchecked::Screenshot {
                selector,
                full_page,
            } => Self::Screenshot {
                selector,
                full_page,
            },
            BrowserActionUnchecked::Wait {
                duration_ms,
                selector,
            } => Self::Wait {
                duration_ms,
                selector,
            },
            BrowserActionUnchecked::Press { key } => Self::Press { key },
            BrowserActionUnchecked::Hover { selector } => Self::Hover { selector },
            BrowserActionUnchecked::Scroll { x, y } => Self::Scroll { x, y },
            BrowserActionUnchecked::IsVisible { selector } => Self::IsVisible { selector },
            BrowserActionUnchecked::Close => Self::Close,
            BrowserActionUnchecked::Find { query } => Self::Find { query },
        }
    }
}

/// A correlated command sent from the native companion to Chrome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserRequest {
    /// Protocol version used to encode this message.
    pub protocol_version: u32,
    /// Unique correlation id for this action.
    pub request_id: String,
    /// Workflow run that owns this action.
    pub run_id: String,
    /// Explicitly shared Chrome tab targeted by this action.
    pub tab_id: u64,
    /// Maximum time the relay may spend on the action.
    pub timeout_ms: u64,
    /// Browser operation to execute.
    pub action: BrowserAction,
}

/// A native cancellation instruction for one in-flight browser action.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserCancel {
    /// Protocol version used to encode this message.
    pub protocol_version: u32,
    /// Fixed message discriminator.
    #[serde(rename = "type")]
    pub message_type: BrowserCancelType,
    /// Correlation id of the action that must stop.
    pub request_id: String,
}

/// Wire discriminator for [`BrowserCancel`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BrowserCancelType {
    /// Cancel one correlated browser action.
    #[serde(rename = "browser.cancel")]
    BrowserCancel,
}

/// Structured successful action data returned by Chrome.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserResult {
    /// Action-specific JSON output.
    pub data: Value,
}

/// Stable, machine-readable browser failure categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BrowserErrorCode {
    /// The requested tab was never explicitly shared.
    TabNotShared,
    /// The user revoked access while the run was active.
    TabRevoked,
    /// The companion lost its authenticated extension connection.
    RelayDisconnected,
    /// Chrome forbids debugger access to the requested page.
    UnsupportedPage,
    /// The action exceeded its bounded deadline.
    ActionTimeout,
    /// No matching element was found.
    ElementNotFound,
    /// The request did not match the versioned protocol schema.
    InvalidRequest,
    /// The peers do not support a common protocol version.
    ProtocolMismatch,
    /// The owning workflow cancelled the action.
    Cancelled,
    /// Chrome or the debugger reported an otherwise unclassified failure.
    BrowserFailure,
}

impl BrowserErrorCode {
    /// Returns the stable wire code used in engine error messages and events.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TabNotShared => "tab_not_shared",
            Self::TabRevoked => "tab_revoked",
            Self::RelayDisconnected => "relay_disconnected",
            Self::UnsupportedPage => "unsupported_page",
            Self::ActionTimeout => "action_timeout",
            Self::ElementNotFound => "element_not_found",
            Self::InvalidRequest => "invalid_request",
            Self::ProtocolMismatch => "protocol_mismatch",
            Self::Cancelled => "cancelled",
            Self::BrowserFailure => "browser_failure",
        }
    }
}

/// Structured browser action failure.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrowserError {
    /// Stable failure category.
    pub code: BrowserErrorCode,
    /// Human-readable, non-secret diagnostic.
    pub message: String,
    /// Optional action-specific diagnostic fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

/// The outcome of a correlated browser command.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserResponse {
    /// The action completed successfully.
    Ok {
        /// Protocol version used to encode this message.
        protocol_version: u32,
        /// Correlation id copied from the request.
        request_id: String,
        /// Structured action output.
        result: BrowserResult,
    },
    /// The action failed with a stable error.
    Error {
        /// Protocol version used to encode this message.
        protocol_version: u32,
        /// Correlation id copied from the request.
        request_id: String,
        /// Structured action failure.
        error: BrowserError,
    },
}

impl BrowserResponse {
    /// Returns the response protocol version.
    pub const fn protocol_version(&self) -> u32 {
        match self {
            Self::Ok {
                protocol_version, ..
            }
            | Self::Error {
                protocol_version, ..
            } => *protocol_version,
        }
    }

    /// Returns the request correlation id.
    pub fn request_id(&self) -> &str {
        match self {
            Self::Ok { request_id, .. } | Self::Error { request_id, .. } => request_id,
        }
    }
}

/// Progress and lifecycle events emitted by the browser relay.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrowserEvent {
    /// Chrome accepted an action for execution.
    ActionStarted {
        /// Protocol version used to encode this message.
        protocol_version: u32,
        /// Correlated request id.
        request_id: String,
        /// Owning workflow run.
        run_id: String,
        /// Target Chrome tab.
        tab_id: u64,
    },
    /// Chrome completed an action.
    ActionCompleted {
        /// Protocol version used to encode this message.
        protocol_version: u32,
        /// Correlated request id.
        request_id: String,
        /// Structured action output.
        result: BrowserResult,
    },
    /// Chrome failed an action.
    ActionFailed {
        /// Protocol version used to encode this message.
        protocol_version: u32,
        /// Correlated request id.
        request_id: String,
        /// Structured action failure.
        error: BrowserError,
    },
    /// The user revoked a tab while it was shared.
    TabRevoked {
        /// Protocol version used to encode this message.
        protocol_version: u32,
        /// Revoked Chrome tab.
        tab_id: u64,
    },
    /// The authenticated relay connection closed.
    RelayDisconnected {
        /// Protocol version used to encode this message.
        protocol_version: u32,
    },
}

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod tests;

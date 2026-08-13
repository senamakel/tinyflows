//! Browser relay adapter and deterministic tool routing.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use serde_json::Value;

use super::protocol::{
    BROWSER_PROTOCOL_VERSION, BrowserAction, BrowserError, BrowserErrorCode, BrowserRequest,
    BrowserResponse,
};
use crate::caps::ToolInvoker;
use crate::error::{EngineError, Result};

/// Default deadline attached to a browser action when the host does not override it.
pub const DEFAULT_BROWSER_ACTION_TIMEOUT_MS: u64 = 30_000;
/// Largest deadline accepted by both sides of protocol v1.
pub const MAX_BROWSER_ACTION_TIMEOUT_MS: u64 = 60_000;

/// Authenticated transport from the native companion to the Chrome extension.
///
/// Implementations own WebSocket authentication, connection health, and tab
/// registration. Returning an error fails the workflow step closed; TinyFlows'
/// ordinary node retry and error-port policies then decide what happens next.
#[async_trait]
pub trait BrowserRelay: Send + Sync {
    /// Sends one correlated request and waits for its terminal response.
    async fn execute(
        &self,
        request: BrowserRequest,
    ) -> std::result::Result<BrowserResponse, BrowserError>;
}

/// A [`ToolInvoker`] that turns `browser` tool calls into Chrome relay requests.
///
/// Each instance is bound to exactly one workflow run and one explicitly shared
/// tab. There is no fallback tab selection. Construct a fresh instance when a
/// side panel or CLI run selects its owning tab.
pub struct ChromeToolInvoker {
    relay: Arc<dyn BrowserRelay>,
    run_id: String,
    tab_id: u64,
    timeout_ms: u64,
    sequence: AtomicU64,
}

impl ChromeToolInvoker {
    /// Creates an invoker bound to `run_id` and the explicitly shared `tab_id`.
    pub fn new(relay: Arc<dyn BrowserRelay>, run_id: impl Into<String>, tab_id: u64) -> Self {
        Self {
            relay,
            run_id: run_id.into(),
            tab_id,
            timeout_ms: DEFAULT_BROWSER_ACTION_TIMEOUT_MS,
            sequence: AtomicU64::new(0),
        }
    }

    /// Overrides the bounded per-action relay deadline.
    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    fn next_request_id(&self) -> String {
        let sequence = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;
        format!("{}:{sequence}", self.run_id)
    }

    fn capability_error(error: &BrowserError) -> EngineError {
        EngineError::Capability(format!(
            "browser:{}: {}",
            error.code.as_str(),
            error.message
        ))
    }
}

#[async_trait]
impl ToolInvoker for ChromeToolInvoker {
    async fn invoke(&self, slug: &str, args: Value, _conn: Option<&str>) -> Result<Value> {
        if slug != "browser" {
            return Err(Self::capability_error(&BrowserError {
                code: BrowserErrorCode::InvalidRequest,
                message: format!("ChromeToolInvoker only accepts slug `browser`, got `{slug}`"),
                details: None,
            }));
        }

        let action: BrowserAction = serde_json::from_value(args).map_err(|error| {
            Self::capability_error(&BrowserError {
                code: BrowserErrorCode::InvalidRequest,
                message: format!("invalid browser args: {error}"),
                details: None,
            })
        })?;
        if !(1..=MAX_BROWSER_ACTION_TIMEOUT_MS).contains(&self.timeout_ms) {
            return Err(Self::capability_error(&BrowserError {
                code: BrowserErrorCode::InvalidRequest,
                message: format!(
                    "browser timeout must be between 1 and {MAX_BROWSER_ACTION_TIMEOUT_MS} ms"
                ),
                details: None,
            }));
        }
        validate_action(&action).map_err(|message| {
            Self::capability_error(&BrowserError {
                code: BrowserErrorCode::InvalidRequest,
                message,
                details: None,
            })
        })?;
        let request_id = self.next_request_id();
        let request = BrowserRequest {
            protocol_version: BROWSER_PROTOCOL_VERSION,
            request_id: request_id.clone(),
            run_id: self.run_id.clone(),
            tab_id: self.tab_id,
            timeout_ms: self.timeout_ms,
            action,
        };

        let response = self
            .relay
            .execute(request)
            .await
            .map_err(|error| Self::capability_error(&error))?;

        if response.protocol_version() != BROWSER_PROTOCOL_VERSION {
            return Err(Self::capability_error(&BrowserError {
                code: BrowserErrorCode::ProtocolMismatch,
                message: format!(
                    "relay responded with protocol version {}, expected {}",
                    response.protocol_version(),
                    BROWSER_PROTOCOL_VERSION
                ),
                details: None,
            }));
        }
        if response.request_id() != request_id {
            return Err(Self::capability_error(&BrowserError {
                code: BrowserErrorCode::InvalidRequest,
                message: format!(
                    "relay response correlation mismatch: expected `{request_id}`, got `{}`",
                    response.request_id()
                ),
                details: None,
            }));
        }

        match response {
            BrowserResponse::Ok { result, .. } => Ok(result.data),
            BrowserResponse::Error { error, .. } => Err(Self::capability_error(&error)),
        }
    }
}

fn validate_action(action: &BrowserAction) -> std::result::Result<(), String> {
    let required = match action {
        BrowserAction::Open { url } => {
            if !(url.starts_with("http://") || url.starts_with("https://")) {
                return Err("browser open URL must use HTTP(S)".into());
            }
            Some(("url", url.as_str()))
        }
        BrowserAction::Click { selector }
        | BrowserAction::Hover { selector }
        | BrowserAction::IsVisible { selector } => Some(("selector", selector.as_str())),
        BrowserAction::Fill { selector, .. } => Some(("selector", selector.as_str())),
        BrowserAction::Press { key } => Some(("key", key.as_str())),
        BrowserAction::Find { query } => Some(("query", query.as_str())),
        BrowserAction::Type { text, .. } => Some(("text", text.as_str())),
        BrowserAction::Snapshot
        | BrowserAction::GetText { .. }
        | BrowserAction::GetTitle
        | BrowserAction::GetUrl
        | BrowserAction::Screenshot { .. }
        | BrowserAction::Wait { .. }
        | BrowserAction::Scroll { .. }
        | BrowserAction::Close => None,
    };
    if let Some((field, value)) = required
        && value.is_empty()
    {
        return Err(format!("browser action field `{field}` must not be empty"));
    }
    Ok(())
}

/// Routes explicit browser calls to Chrome and delegates every other tool unchanged.
pub struct RoutingToolInvoker {
    browser: Arc<ChromeToolInvoker>,
    fallback: Arc<dyn ToolInvoker>,
}

impl RoutingToolInvoker {
    /// Creates a router around one run/tab-bound browser invoker and a host invoker.
    pub fn new(browser: Arc<ChromeToolInvoker>, fallback: Arc<dyn ToolInvoker>) -> Self {
        Self { browser, fallback }
    }
}

#[async_trait]
impl ToolInvoker for RoutingToolInvoker {
    async fn invoke(&self, slug: &str, args: Value, conn: Option<&str>) -> Result<Value> {
        if slug == "browser" {
            self.browser.invoke(slug, args, conn).await
        } else {
            self.fallback.invoke(slug, args, conn).await
        }
    }
}

#[cfg(test)]
#[path = "routing_tests.rs"]
mod tests;

use serde::{Deserialize, Serialize};

/// Stable, host-derived identity for a channel session.
///
/// Optional fields are omitted when absent so older serialized origins retain
/// their existing wire representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelSessionIdentity {
    /// Tenant or profile owning the session.
    pub tenant_key: String,
    /// Agent associated with the session.
    pub agent_id: String,
    /// Stable key for the session in its target.
    pub session_key: String,
    /// Channel provider, such as `telegram` or `discord`.
    pub provider: String,
    /// Provider account that owns the session, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_id: Option<String>,
    /// Conversation coordinates within the session.
    pub conversation: ChannelConversationIdentity,
}

/// Provider-independent identity of a conversation within a channel session.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelConversationIdentity {
    /// Conversation category, such as `direct`, `group`, or `channel`.
    pub kind: String,
    /// Provider-local conversation identifier.
    pub id: String,
    /// Parent scope, such as a guild or workspace, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_id: Option<String>,
    /// Parent conversation identifier, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<String>,
    /// Provider thread identifier, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    /// Provider topic identifier, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_id: Option<String>,
}

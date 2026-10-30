use super::*;
use crate::schedule::{ChannelConversationIdentity, ChannelSessionIdentity};
use serde_json::json;

#[test]
fn channel_job_origin_roundtrips_session_and_conversation_identity() {
    let origin = JobOrigin::Channel {
        channel: "telegram".into(),
        reply_target: "42".into(),
        history_key: "telegram_alice_42".into(),
        sender: Some("alice".into()),
        thread_id: None,
        channel_session: Some(Box::new(ChannelSessionIdentity {
            tenant_key: "profile:alice".into(),
            agent_id: "orchestrator".into(),
            session_key: "main:telegram:bot-a:group:room-42".into(),
            provider: "telegram".into(),
            account_id: Some("telegram:bot-a".into()),
            conversation: ChannelConversationIdentity {
                kind: "group".into(),
                id: "room-42".into(),
                scope_id: Some("guild-1".into()),
                parent_id: Some("parent-1".into()),
                thread_id: Some("thread-1".into()),
                topic_id: Some("topic-1".into()),
            },
        })),
    };

    let wire = serde_json::to_value(&origin).unwrap();
    assert_eq!(wire["channel_session"]["tenant_key"], "profile:alice");
    assert_eq!(wire["channel_session"]["conversation"]["thread_id"], "thread-1");
    assert_eq!(serde_json::from_value::<JobOrigin>(wire).unwrap(), origin);
}

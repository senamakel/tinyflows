use super::*;
use serde_json::json;

#[test]
fn channel_job_origin_roundtrips_session_and_conversation_identity() {
    let wire = json!({
        "kind": "channel",
        "channel": "telegram",
        "reply_target": "42",
        "history_key": "telegram_alice_42",
        "channel_session": {
            "tenant_key": "profile:alice",
            "agent_id": "orchestrator",
            "session_key": "main:telegram:bot-a:group:room-42",
            "provider": "telegram",
            "account_id": "telegram:bot-a",
            "conversation": {
                "kind": "group",
                "id": "room-42",
                "scope_id": "guild-1",
                "parent_id": "parent-1",
                "thread_id": "thread-1",
                "topic_id": "topic-1"
            }
        }
    });

    let origin: JobOrigin = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(origin).unwrap(), wire);
}

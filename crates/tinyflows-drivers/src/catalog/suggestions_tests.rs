use super::*;
use crate::catalog::test_support::catalog;

fn suggestion(id: &str, created_at: &str, confidence: f64) -> FlowSuggestion {
    FlowSuggestion {
        id: id.into(),
        title: format!("title {id}"),
        one_liner: "one".into(),
        rationale: "why".into(),
        trigger_hint: Some("daily".into()),
        steps_outline: vec!["a".into(), "b".into()],
        suggested_connections: vec!["gmail".into()],
        suggested_slugs: vec!["GMAIL_SEND".into()],
        build_prompt: "build it".into(),
        confidence,
        status: SuggestionStatus::New,
        created_at: created_at.into(),
        source_run_id: None,
    }
}

#[tokio::test]
async fn suggestions_round_trip_newest_then_most_confident() {
    let store = catalog();
    assert_eq!(store.upsert_suggestions(&[]).await.unwrap(), 0);
    let written = store
        .upsert_suggestions(&[
            suggestion("old", "2026-01-01T00:00:00Z", 0.9),
            suggestion("low", "2026-01-02T00:00:00Z", 0.1),
            suggestion("high", "2026-01-02T00:00:00Z", 0.8),
        ])
        .await
        .unwrap();
    assert_eq!(written, 3);
    let listed = store.list_suggestions(None, 10).await.unwrap();
    assert_eq!(
        listed.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["high", "low", "old"]
    );
    assert_eq!(listed[0], suggestion("high", "2026-01-02T00:00:00Z", 0.8));
    assert_eq!(store.list_suggestions(None, 0).await.unwrap().len(), 1);
}

#[tokio::test]
async fn a_rerun_keeps_the_users_status_and_created_at() {
    let store = catalog();
    store
        .upsert_suggestions(&[suggestion("s", "2026-01-01T00:00:00Z", 0.5)])
        .await
        .unwrap();
    assert!(
        store
            .set_suggestion_status("s", SuggestionStatus::Dismissed)
            .await
            .unwrap()
    );
    assert!(
        !store
            .set_suggestion_status("missing", SuggestionStatus::Built)
            .await
            .unwrap()
    );
    let mut again = suggestion("s", "2026-03-01T00:00:00Z", 0.7);
    again.title = "refreshed".into();
    store.upsert_suggestions(&[again]).await.unwrap();
    let read = store
        .list_suggestions(Some(SuggestionStatus::Dismissed), 10)
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].title, "refreshed");
    assert_eq!(read[0].created_at, "2026-01-01T00:00:00Z");
    assert!(
        store
            .list_suggestions(Some(SuggestionStatus::New), 10)
            .await
            .unwrap()
            .is_empty()
    );
}

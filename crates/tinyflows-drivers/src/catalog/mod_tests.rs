use super::*;

#[test]
fn composite_ids_are_injective_and_bounded() {
    assert_ne!(composite_id(&["a/b", "c"]), composite_id(&["a", "b/c"]));
    let long = "x".repeat(600);
    let hashed = composite_id(&[&long, "k"]);
    assert!(hashed.starts_with("h:") && hashed.len() < MAX_KEY_LEN);
}

#[test]
fn instants_compare_as_instants_not_strings() {
    assert!(instant_before("2026-01-01T00:00:00Z", "2026-01-01T00:00:00.5+00:00"));
    assert!(instant_before("2026-01-01T01:00:00+02:00", "2026-01-01T00:00:00Z"));
    assert!(instant_before("a", "b"), "unparseable falls back to strings");
    assert!(instant_ns("2026-01-01T00:00:00Z") < instant_ns("2026-01-01T00:00:00.000000001Z"));
    assert_eq!(instant_ns("not a time"), 0);
}

#[tokio::test]
async fn ensure_declares_every_collection_once() {
    let catalog = test_support::catalog();
    catalog.ensure().await.unwrap();
    catalog.ensure().await.unwrap();
    assert_eq!(FlowCatalogDocuments::collections().len(), 7);
}

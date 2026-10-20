use super::*;

#[test]
fn composite_keys_escape_round_trip_and_hash_long_parts() {
    let key =
        Key::of(["flow/name", "%state", "~leading", "nul\0byte"]).expect("valid composite key");
    assert_eq!(
        key.split().expect("reversible key"),
        ["flow/name", "%state", "~leading", "nul\0byte"]
    );

    let long = Key::of(["x".repeat(600)]).expect("long components hash");
    assert!(long.is_hashed());
    assert_eq!(long.as_str().len(), 53);
    assert_eq!(long.split(), None);
}

#[test]
fn serialized_keys_revalidate_document_ids() {
    assert!(serde_json::from_str::<Key>("\"valid/id\"").is_ok());
    assert!(serde_json::from_str::<Key>("\"bad\\u0000id\"").is_err());
}

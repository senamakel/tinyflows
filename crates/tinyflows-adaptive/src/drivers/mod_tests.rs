use super::*;

#[test]
fn keys_are_unambiguous_and_long_ones_hashed() {
    assert_ne!(key(&["a/b", "c"]), key(&["a", "b/c"]));
    assert_ne!(key(&["", "x"]), key(&["x", ""]));
    let hashed = key(&[&"t".repeat(500)]);
    assert!(hashed.starts_with("h:") && hashed.len() == 66, "{hashed}");
    assert_ne!(hashed, key(&[&"u".repeat(500)]));
}

#[test]
fn only_a_conflict_is_a_race() {
    assert!(is_race(&StorageError::conflict("x")));
    assert!(!is_race(&StorageError::unavailable("x")));
}

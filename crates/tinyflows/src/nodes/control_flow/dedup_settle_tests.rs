use super::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
struct MemKv {
    map: Mutex<HashMap<String, Value>>,
    fail_set: bool,
}

impl DedupKv for MemKv {
    fn kv_get(&self, key: &str) -> Result<Option<Value>, String> {
        Ok(self.map.lock().unwrap().get(key).cloned())
    }
    fn kv_set(&self, key: &str, value: &Value) -> Result<(), String> {
        if self.fail_set {
            return Err("disk full".into());
        }
        self.map.lock().unwrap().insert(key.into(), value.clone());
        Ok(())
    }
    fn kv_delete(&self, key: &str) -> Result<(), String> {
        self.map.lock().unwrap().remove(key);
        Ok(())
    }
}

fn seeded() -> MemKv {
    let kv = MemKv::default();
    kv.map
        .lock()
        .unwrap()
        .insert("dedup:n1:tentative".into(), json!(["b", "c"]));
    kv.map
        .lock()
        .unwrap()
        .insert("dedup:n1:committed".into(), json!(["a", "b"]));
    kv
}

#[test]
fn success_unions_tentative_into_committed_and_clears_tentative() {
    let kv = seeded();
    let out = settle(&kv, "n1", true);
    assert_eq!(
        out,
        Settlement::Commit(CommitOutcome::Committed {
            added: 1,
            committed_len: 3,
            clear_error: None
        })
    );
    let map = kv.map.lock().unwrap();
    assert_eq!(map["dedup:n1:committed"], json!(["a", "b", "c"]));
    assert!(!map.contains_key("dedup:n1:tentative"));
}

#[test]
fn failure_releases_tentative_and_leaves_committed() {
    let kv = seeded();
    assert_eq!(settle(&kv, "n1", false), Settlement::Release(Ok(())));
    let map = kv.map.lock().unwrap();
    assert_eq!(map["dedup:n1:committed"], json!(["a", "b"]));
    assert!(!map.contains_key("dedup:n1:tentative"));
}

#[test]
fn nothing_tentative_is_a_no_op() {
    let kv = MemKv::default();
    assert_eq!(commit(&kv, "n1"), CommitOutcome::NothingTentative);
    assert!(kv.map.lock().unwrap().is_empty());
}

#[test]
fn failed_committed_write_keeps_tentative_for_retry() {
    let mut kv = seeded();
    kv.fail_set = true;
    assert_eq!(commit(&kv, "n1"), CommitOutcome::CommitFailed("disk full".into()));
    assert_eq!(kv.map.lock().unwrap()["dedup:n1:tentative"], json!(["b", "c"]));
}

#[test]
fn only_completed_statuses_count_as_success() {
    assert!(is_success_status("completed"));
    assert!(is_success_status("completed_with_warnings"));
    for s in ["failed", "cancelled", "interrupted", "", "COMPLETED", "weird"] {
        assert!(!is_success_status(s), "{s}");
    }
}

#[test]
fn malformed_stored_sets_degrade_to_empty() {
    let kv = MemKv::default();
    kv.map
        .lock()
        .unwrap()
        .insert("dedup:n1:tentative".into(), json!(["x", 1, null]));
    kv.map
        .lock()
        .unwrap()
        .insert("dedup:n1:committed".into(), json!("not-an-array"));
    assert!(matches!(
        commit(&kv, "n1"),
        CommitOutcome::Committed { added: 1, committed_len: 1, .. }
    ));
    assert_eq!(kv.map.lock().unwrap()["dedup:n1:committed"], json!(["x"]));
}

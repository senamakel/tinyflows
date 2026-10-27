//! Process-wide monotonic time and ULID ids for new entities.

use std::sync::atomic::{AtomicI64, Ordering};

use chrono::Utc;
use tinyflows_storage::Key;

/// Now in epoch milliseconds, never repeating within the process: creations
/// in quick succession still get distinct, increasing times, which is what
/// "oldest first" orders by.
pub(crate) fn monotonic_ms() -> i64 {
    static LAST: AtomicI64 = AtomicI64::new(0);
    let now = Utc::now().timestamp_millis();
    let mut last = LAST.load(Ordering::Relaxed);
    loop {
        let next = now.max(last + 1);
        match LAST.compare_exchange_weak(last, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return next,
            Err(seen) => last = seen,
        }
    }
}

/// A new entity id: a ULID stamped with [`monotonic_ms`], so ids sort in
/// creation order even within one millisecond of wall clock.
pub(crate) fn new_id() -> String {
    Key::ulid_at(u64::try_from(monotonic_ms()).unwrap_or_default())
        .as_str()
        .to_owned()
}

#[cfg(test)]
#[path = "ids_tests.rs"]
mod tests;

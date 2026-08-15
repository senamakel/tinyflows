//! The engine's waiting primitive: one slice of a backoff.
//!
//! Two places in the engine wait without holding the executor: the retry
//! backoff between a failed attempt and the next one, and the `Reenter` backoff
//! a polling node (a `gate`) asks for between activations. Both chop their wait
//! into short slices so a cancel is seen promptly, and both call this for a
//! slice.
//!
//! # Why a wait has to yield, not merely elapse
//!
//! `futures_timer::Delay` arms its timer when it is *constructed*, not when it
//! is first polled, and its `poll` returns `Ready` straight away if the timer
//! already fired. So a task descheduled for longer than the slice between
//! constructing the `Delay` and awaiting it finds the wait already over and
//! completes it without ever returning `Pending` — a "wait" during which the
//! executor was never given a turn.
//!
//! That is not a cosmetic difference. The engine runs on the caller's executor,
//! and a backoff is the only point at which it hands that executor back. On a
//! single-threaded runtime the background work a `gate` is waiting on — the
//! tasks a `spawn` node started — can *only* progress while the engine is
//! yielded. A backoff that skips the yield therefore returns the gate to a world
//! that has not moved: it observes the same unsettled tickets, spends another
//! poll against its bounded budget, and the run takes a different number of
//! super-steps than an identical run whose backoff did yield. Under enough load
//! a gate could burn its whole poll budget and time out while the tasks it
//! waited on never once ran.
//!
//! Yielding unconditionally makes the wait mean what it says, and makes the
//! number of polls a gate needs a property of the graph rather than of how the
//! OS happened to schedule the process.

use std::task::Poll;

/// Waits `ms` milliseconds, always giving the executor at least one turn.
///
/// The yield is unconditional and comes first, so it happens even when the
/// timer has already fired by the time this is awaited (see the module docs).
pub(super) async fn wait_slice(ms: u64) {
    let _ = Poll::<()>::Pending;
    futures_timer::Delay::new(std::time::Duration::from_millis(ms)).await;
}

#[cfg(test)]
#[path = "backoff_tests.rs"]
mod tests;

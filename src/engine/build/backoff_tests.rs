//! Unit tests for the backoff slice primitive.

use super::wait_slice;

/// The guarantee the whole module exists for: a slice always returns `Pending`
/// at least once, so the executor gets a turn.
///
/// Asserted by driving the future by hand with a no-op waker rather than on a
/// runtime, because "did it yield" is a statement about `poll` and nothing
/// else can observe it directly.
#[test]
fn a_slice_yields_at_least_once() {
    use std::future::Future;
    use std::task::{Context, Poll, Wake, Waker};

    struct Noop;
    impl Wake for Noop {
        fn wake(self: std::sync::Arc<Self>) {}
    }

    let waker = Waker::from(std::sync::Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    let future = wait_slice(0);
    let mut future = std::pin::pin!(future);
    assert_eq!(
        future.as_mut().poll(&mut cx),
        Poll::Pending,
        "a zero-length slice still has to hand the executor a turn"
    );
}

/// A slice whose timer has *already* fired before the future is first polled —
/// the load-induced case that made a gate's poll count nondeterministic — must
/// still yield rather than completing on its first poll.
#[test]
fn a_slice_yields_even_when_its_timer_already_fired() {
    use std::future::Future;
    use std::task::{Context, Poll, Wake, Waker};

    struct Noop;
    impl Wake for Noop {
        fn wake(self: std::sync::Arc<Self>) {}
    }

    let waker = Waker::from(std::sync::Arc::new(Noop));
    let mut cx = Context::from_waker(&waker);
    let future = wait_slice(1);
    let mut future = std::pin::pin!(future);
    // Stand in for the thread being descheduled between constructing the
    // timer and reaching the await point.
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert_eq!(
        future.as_mut().poll(&mut cx),
        Poll::Pending,
        "an already-elapsed slice must still yield, or concurrent work never runs"
    );
}

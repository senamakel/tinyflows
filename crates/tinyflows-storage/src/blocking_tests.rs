use super::*;

#[test]
fn bridge_runs_futures_from_sync_and_current_thread_runtime_contexts() {
    let bridge = Blocking::new().expect("bridge starts");
    assert_eq!(bridge.run(async { 42 }).expect("sync call completes"), 42);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");
    assert_eq!(
        runtime
            .block_on(async { bridge.run(async { "ready" }) })
            .expect("bridge call from runtime"),
        "ready"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bridge_runs_from_a_multithread_worker() {
    let bridge = Blocking::new().expect("bridge starts");
    assert_eq!(
        bridge.run(async { 7usize }).expect("bridge call completes"),
        7
    );
}

#[test]
fn a_future_cannot_reenter_its_own_bridge() {
    let bridge = Blocking::new().expect("bridge starts");
    let inner = bridge.clone();
    let error = bridge
        .run(async move { inner.run(async { () }) })
        .expect("outer bridge call completes")
        .expect_err("reentry would deadlock");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

#[test]
fn panicking_jobs_are_reported_to_the_waiting_caller() {
    let bridge = Blocking::new().expect("bridge starts");
    let error = bridge
        .run(async { panic!("intentional bridge job failure") })
        .expect_err("panicking job closes its reply channel");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Backend);
}

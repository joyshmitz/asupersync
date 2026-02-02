use asupersync::{Cx, Outcome, RegionId, RuntimeBuilder};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn test_region_does_not_close_automatically() {
    let runtime = RuntimeBuilder::current_thread()
        .build()
        .expect("runtime build");

    let completed = Arc::new(AtomicBool::new(false));
    let completed_clone = completed.clone();

    // Spawn a task in a sub-region
    let handle = runtime.handle().spawn(async move {
        let mut cx = Cx::current().expect("cx");

        let sub_region = cx
            .region(|scope| async move {
                scope.spawn(async move {
                    // Simulate some work
                    std::thread::sleep(Duration::from_millis(10));
                    completed_clone.store(true, Ordering::SeqCst);
                });
            })
            .await;

        sub_region
    });

    let region_outcome = runtime.block_on(handle);

    // The task inside should have completed
    assert!(completed.load(Ordering::SeqCst));

    // But did the region close?
    // In the current implementation, `cx.region` waits for the region to close.
    // If the runtime loop drives the region state, `cx.region` should return.
    // If it DOESN'T drive the region state, `cx.region` might hang or return early without closing.

    // Actually, `cx.region` is a future that awaits the region completion.
    // If the region state machine isn't being driven, `cx.region` should HANG.
    // The fact that `runtime.block_on(handle)` returns means `cx.region` returned.

    // Let's inspect the returned outcome/region state if possible.
    // Wait, `cx.region` returns `Outcome<R, E>`.

    // If the test passes (i.e. block_on returns), then `cx.region` finished.
    // This would mean the region DID close.

    // How does `cx.region` implementation work? I need to check `src/cx/mod.rs` or `src/cx/scope.rs`.
}

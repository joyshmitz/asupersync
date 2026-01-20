use asupersync::lab::LabConfig;
use asupersync::lab::LabRuntime;
use asupersync::types::Budget;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct YieldNow {
    yielded: bool,
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn yield_now() {
    YieldNow { yielded: false }.await
}

#[test]
fn test_lab_executor_runs_task() {
    let mut runtime = LabRuntime::new(LabConfig::default());

    // 1. Create root region
    let region = runtime.state.create_root_region(Budget::INFINITE);

    // 2. Create task
    let executed = Arc::new(AtomicBool::new(false));
    let executed_clone = executed.clone();

    let (task_id, _handle) = runtime
        .state
        .create_task(region, Budget::INFINITE, async move {
            executed_clone.store(true, Ordering::SeqCst);
        })
        .expect("create task");

    // 3. Schedule the task (RuntimeState.create_task doesn't schedule)
    runtime
        .scheduler
        .lock()
        .unwrap()
        .schedule(task_id, 0);

    // 4. Run until quiescent
    let steps = runtime.run_until_quiescent();

    // 5. Verify
    assert!(steps > 0, "Should have executed at least one step");
    assert!(
        executed.load(Ordering::SeqCst),
        "Task should have executed"
    );

    // Verify task is done using public API
    assert_eq!(runtime.state.live_task_count(), 0, "No live tasks should remain");
    assert!(runtime.is_quiescent(), "Runtime should be quiescent");
}

#[test]
fn test_lab_executor_wakes_task_yielding() {
    let mut runtime = LabRuntime::new(LabConfig::default());
    let region = runtime.state.create_root_region(Budget::INFINITE);

    let counter = Arc::new(AtomicBool::new(false));
    let counter_clone = counter.clone();

    // Create a task that yields once then sets flag
    let (task_id, _handle) = runtime
        .state
        .create_task(region, Budget::INFINITE, async move {
            // yield once
            yield_now().await;
            counter_clone.store(true, Ordering::SeqCst);
        })
        .expect("create task");

    runtime
        .scheduler
        .lock()
        .unwrap()
        .schedule(task_id, 0);

    runtime.run_until_quiescent();

    assert!(counter.load(Ordering::SeqCst), "Task should have completed after yield");
}

//! Reproduction test for spawn scheduling with manual runtime state.

#![allow(missing_docs)]

#[cfg(test)]
mod tests {
    use asupersync::cx::Cx;
    use asupersync::runtime::scheduler::ThreeLaneScheduler;
    use asupersync::runtime::RuntimeState;
    use asupersync::types::{Budget, TaskId};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn repro_spawn_registered_hangs_manual_state() {
        // 1. Setup RuntimeState and Scheduler (simulating a worker environment)
        let state = Arc::new(Mutex::new(RuntimeState::new()));
        let mut scheduler = ThreeLaneScheduler::new(1, &state); // 1 worker

        // 2. Create a root region
        let root_region = state.lock().unwrap().create_root_region(Budget::INFINITE);

        // 3. Create a scope manually
        // We need a Cx bound to root_region to create a Scope.
        let cx: Cx = Cx::new_with_observability(
            root_region,
            TaskId::new_for_test(0, 0),
            Budget::INFINITE,
            None,
            None,
            None,
        );
        let scope = cx.scope();

        let inner_ran = Arc::new(AtomicBool::new(false));
        let inner_ran_clone = inner_ran.clone();

        // 4. Spawn a task using spawn_registered
        // We simulate being inside a task where we have access to state (locked)
        let _handle = {
            let mut guard = state.lock().unwrap();
            let res = scope.spawn_registered(&mut guard, &cx, |_| async move {
                inner_ran_clone.store(true, Ordering::SeqCst);
                42
            });
            let handle = res.expect("spawn failed");
            drop(guard);
            handle
        };

        // 5. Run the worker loop on a thread to drive the task
        let mut worker = scheduler.take_workers().pop().unwrap();
        let shutdown = Arc::clone(&worker.shutdown);
        let parker = worker.parker.clone();
        let worker_thread = std::thread::spawn(move || worker.run_loop());

        // Wait briefly for the task to run
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_millis(100) {
            if inner_ran.load(Ordering::SeqCst) {
                break;
            }
            std::thread::yield_now();
        }

        // Signal shutdown and unpark to ensure the worker exits
        shutdown.store(true, Ordering::SeqCst);
        parker.unpark();
        let _ = worker_thread.join();

        // If the bug exists, the task was never scheduled, so run_once() always returned false (or didn't find the task),
        // and inner_ran is false.
        assert!(
            inner_ran.load(Ordering::SeqCst),
            "Inner task did not run - it was likely not scheduled!"
        );
    }
}

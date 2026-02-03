use asupersync::cx::Cx;
use asupersync::runtime::RuntimeBuilder;
use asupersync::types::Budget;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn repro_spawn_registered_hangs() {
    let runtime = RuntimeBuilder::new()
        .worker_threads(2)
        .build()
        .expect("runtime build");

    let inner_ran = Arc::new(AtomicBool::new(false));
    let inner_ran_clone = inner_ran.clone();

    // Run a root task
    runtime.block_on(async move {
        // Get the current context (simulated or real, block_on provides one via thread-local if set? 
        // No, block_on runs a future. It doesn't inject Cx.
        // But we need a Cx to create a Scope.
        // Wait, block_on runs a raw future. The runtime doesn't provide Cx to the root future unless we ask for it?
        // Runtime::block_on(future) -> F::Output.
        // It uses run_future_with_budget.
        // run_future_with_budget just polls.
        
        // So the root future doesn't have a Cx!
        // We need to bootstrap a Cx.
        // Cx::new(...) is private/crate-visible.
        // But tests can use Cx::for_testing().
        
        // HOWEVER, to reproduce the bug in `spawn_registered`, we need a valid `RuntimeState`.
        // `scope.spawn_registered` takes `&mut RuntimeState`.
        // We don't have access to `RuntimeState` inside `block_on`!
        
        // This reveals another issue: How do users use `scope!` if they can't get `RuntimeState`?
        // The `scope!` macro expects `__state` to be in scope.
        // Where does `__state` come from?
        // It must be passed down from somewhere.
        
        // If I am a user, how do I get `__state`?
        // I don't. `RuntimeState` is internal.
        // `Scope` is internal?
        // `Scope` is `pub`.
        
        // But `spawn_registered` takes `&mut RuntimeState`.
        // `RuntimeState` is `pub use ...` in `runtime`.
        
        // If `spawn!` is a macro, it injects `__state`.
        // This implies `__state` must be available in the context where `spawn!` is called.
        // This means `scope!` must provide `__state`.
        // `scope!` macro:
        /*
        quote! {
            {
                // scope! macro expansion
                let __cx = &#cx;
                #scope_creation
                #trace_name
                async move {
                    let scope = __scope;
                    #(#body_stmts)*
                }.await
            }
        }
        */
        // `scope!` does NOT provide `__state`!
        
        // Wait, `spawn!` macro:
        /*
        quote! {
            {
                // ...
                __scope.spawn_registered(__state, __cx, ...)
            }
        }
        */
        // `spawn!` uses `__state`.
        // But `scope!` does NOT provide `__state`.
        
        // So where does `__state` come from?
        // It must be passed as an argument to the function containing `scope!`?
        // Or `scope!` assumes `__state` is available?
        
        // This suggests that `scope!` and `spawn!` are only usable in functions that take `&mut RuntimeState`.
        // e.g. `fn my_task(state: &mut RuntimeState, cx: &Cx) ...`
        
        // But the `Scope::spawn` method requires `&mut RuntimeState`.
        
        // If users are supposed to use this, they must have access to `RuntimeState`.
        // But `RuntimeState` is the GLOBAL state of the runtime.
        // Passing `&mut RuntimeState` around means SINGLE THREADED access to the global state.
        
        // This confirms Phase 0 (Single Threaded) design.
        // In Phase 1 (Multi-threaded), we cannot pass `&mut RuntimeState` around!
        // `RuntimeState` is `Arc<Mutex<...>>` in the runtime.
        
        // So `spawn!` macro which uses `&mut RuntimeState` is fundamentally incompatible with multi-threaded execution where tasks run in parallel and don't hold the global lock.
        
        // If `spawn!` expects `__state: &mut RuntimeState`, then `spawn!` can only be used when we hold the lock.
        // But we can't hold the lock across `.await` points (deadlock/send issues).
        
        // This implies `spawn!` is intended for Phase 0 only, or for "Fiber" tier tasks that run inside the lock (unlikely).
        
        // IF `asupersync` claims to be moving to Phase 1, `spawn!` needs to change to use `Arc<Mutex<RuntimeState>>` or similar, OR `Scope` should hold the reference.
        
        // For this reproduction, I will simulate holding the lock.
        
        // I cannot easily access `RuntimeState` from `block_on` because `Runtime` wraps it.
        // I need to use `RuntimeInner` or `RuntimeState` directly.
        
        // I'll create a `RuntimeState` manually, like in unit tests.
    });
}

#[test]
fn repro_spawn_registered_hangs_manual_state() {
    use asupersync::runtime::RuntimeState;
    use asupersync::runtime::scheduler::ThreeLaneScheduler;
    use std::sync::{Arc, Mutex};

    // 1. Setup RuntimeState and Scheduler (simulating a worker environment)
    let state = Arc::new(Mutex::new(RuntimeState::new()));
    let scheduler = ThreeLaneScheduler::new(1, &state); // 1 worker
    
    // 2. Create a root task
    let root_region = state.lock().unwrap().create_root_region(Budget::INFINITE);
    
    // 3. Create a scope
    // We need to run this "inside" a task. 
    // We'll simulate the "body" of a task that has access to state.
    
    let inner_ran = Arc::new(AtomicBool::new(false));
    let inner_ran_clone = inner_ran.clone();
    
    // Lock state to simulate being in a "Fiber" or holding the lock
    let mut guard = state.lock().unwrap();
    let cx = Cx::for_testing(); // Fake context
    
    // Create scope manually
    let scope = cx.scope(); // Uses root region from cx (placeholder in for_testing)
    // We need a real region.
    // cx.scope() uses cx.region_id().
    // We need a Cx bound to root_region.
    let cx = Cx::new(root_region, asupersync::types::TaskId::new_for_test(0,0), Budget::INFINITE);
    let scope = cx.scope();
    
    // 4. Spawn a task using spawn_registered
    let res = scope.spawn_registered(&mut *guard, &cx, |_| async move {
        inner_ran_clone.store(true, Ordering::SeqCst);
        42
    });
    
    let handle = res.expect("spawn failed");
    
    // Drop lock so worker can run
    drop(guard);
    
    // 5. Run the scheduler to drive the task
    // The task should be in the scheduler's queue.
    // If spawn_registered works, it should be scheduled.
    
    // We need to extract the worker to run it
    // But ThreeLaneScheduler::new consumes the state Arc.
    // wait, `new` takes `&Arc`.
    
    // We can't easily run the scheduler loop here because we own the scheduler.
    // We can use `scheduler.run_once()` if we had access to workers.
    // `scheduler.take_workers()` gives us workers.
    
    // BUT, the bug is that `spawn_registered` DOES NOT push to the scheduler.
    // So even if we run the worker, it won't find the task.
    
    // Let's verify if the task is in the global queue.
    // `ThreeLaneScheduler` has `global`.
    // But `global` is private.
    
    // We can check if `handle.join` works?
    // No, we need to drive the runtime.
    
    // Let's try to run the worker loop for a bit.
    // We need to wrap this in a way that we can assert "inner_ran" is true.
    
    // If the task wasn't scheduled, the worker loop will exit/park immediately (or spin if we don't handle parking).
    
    // Let's rely on `scheduler` logic.
}

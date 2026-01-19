# Cancellation Injection Testing

Asupersync provides a deterministic framework for testing that your async code handles
cancellation correctly at every await point. This document explains how to use it.

## Overview

### The Problem: Cancellation Can Strike Anywhere

In async Rust, a future can be cancelled at any `.await` point. Consider this code:

```rust
async fn transfer_money(from: Account, to: Account, amount: u64) {
    from.debit(amount).await;          // await point 1
    to.credit(amount).await;           // await point 2
    log_transaction(from, to, amount).await;  // await point 3
}
```

If this future is cancelled after the debit but before the credit, money vanishes.
Traditional testing cannot catch this because:

- Unit tests run futures to completion
- Integration tests don't systematically test every cancellation point
- Production bugs are non-deterministic and hard to reproduce

### The Solution: Systematic Cancellation Injection

Asupersync's lab runtime enables deterministic cancellation testing:

1. **Recording Phase**: Run your async code once to discover all await points
2. **Injection Phase**: Re-run with cancellation injected at each await point
3. **Verification Phase**: Oracles check that invariants hold after cancellation

The lab runtime's virtual time and deterministic scheduling guarantee reproducibility:
same seed = same execution order = same test results.

## Quick Start

Add a cancellation injection test to your test suite:

```rust
use asupersync::lab::{lab, InjectionStrategy, InstrumentedFuture};

#[test]
fn my_async_code_is_cancel_safe() {
    let report = lab(42)  // seed for determinism
        .with_cancellation_injection(InjectionStrategy::AllPoints)
        .with_all_oracles()
        .run(|injector| {
            InstrumentedFuture::new(my_async_function(), injector)
        });

    assert!(report.all_passed(), "Cancellation failures:\n{}", report);
}
```

This will:
1. Run `my_async_function` once to record await points
2. Re-run it N times, injecting cancellation at each of the N await points
3. Verify oracles after each run
4. Report any failures with reproduction instructions

## Injection Strategies

Choose the right strategy for your testing needs:

### `AllPoints` - Exhaustive Testing

```rust
InjectionStrategy::AllPoints
```

Tests cancellation at every discovered await point. Use for:
- Critical code paths (payment processing, data persistence)
- Small tests with few await points
- Pre-release validation

**Trade-off**: O(N) test runs for N await points. Thorough but slow for large tests.

### `RandomSample(n)` - Probabilistic Coverage

```rust
InjectionStrategy::RandomSample(5)  // test 5 random points
```

Selects n random await points using a deterministic RNG seeded by the test seed.
Use for:
- Large tests with many await points
- CI pipelines with time constraints
- Fuzzing campaigns

**Trade-off**: May miss edge cases, but same seed = same points tested.

### `SpecificPoints(vec)` - Targeted Regression Testing

```rust
InjectionStrategy::SpecificPoints(vec![3, 7, 12])
```

Tests only the specified await points. Use for:
- Regression tests for known-problematic points
- Focused testing during development
- Reproducing specific failures

### `Probabilistic(p)` - Chaos Testing

```rust
InjectionStrategy::Probabilistic(0.1)  // 10% chance per await point
```

Each await point has probability p of being tested. Use for:
- Long-running chaos/soak tests
- Random exploration of the cancellation space
- Discovering unexpected interactions

### `FirstN(n)` - Early Await Points

```rust
InjectionStrategy::FirstN(3)  // test first 3 await points
```

Tests the first n await points. Use for:
- Testing initialization/setup code
- When early cancellation is most critical
- Quick smoke tests

### `EveryNth(n)` - Periodic Sampling

```rust
InjectionStrategy::EveryNth(5)  // test every 5th await point
```

Tests every nth await point. Use for:
- Systematic sampling of large futures
- When you expect periodic patterns

### `Never` - Recording Only

```rust
InjectionStrategy::Never
```

Records await points without testing. Use for:
- Discovering how many await points exist
- Debugging instrumentation
- Baseline measurements

## Understanding Failures

When a test fails, the report provides actionable information.

### Reading the Report

```
Cancellation Injection Test Report
==================================

Summary:
  Await points discovered: 15
  Points tested: 15 (strategy: AllPoints)
  Passed: 14
  Failed: 1
  Seed: 42
  Verdict: FAIL

Failures:

  [1] Await point 7
      Seed: 42
      Failed oracles:
        - ObligationLeak: Resource 'db_connection' was not released

      To reproduce:
        let config = LabInjectionConfig::new(42)
            .with_strategy(InjectionStrategy::AtSequence(7));
        let mut runner = LabInjectionRunner::new(config);
        let report = runner.run_simple(|injector| {
            InstrumentedFuture::new(your_future(), injector)
        });
        assert!(report.all_passed());
```

### Common Oracle Violations

#### ObligationLeak

**Symptom**: A resource was acquired but not released after cancellation.

**Common causes**:
- RAII guard not used (relying on explicit cleanup)
- Cleanup code after an await point that never runs
- Shared state not properly cleaned up on drop

**Fix**: Use RAII guards and register finalizers for critical cleanup.

#### TaskLeak

**Symptom**: Spawned tasks were not joined after the parent was cancelled.

**Common causes**:
- Spawning without storing the join handle
- Not cancelling child tasks when parent is cancelled
- Infinite loops in spawned tasks

**Fix**: Use structured concurrency - always join or cancel child tasks.

#### QuiescenceViolation

**Symptom**: The runtime did not reach quiescence after region close.

**Common causes**:
- Tasks still pending after cancellation
- Unbounded work queues
- Livelock between tasks

**Fix**: Ensure all work can complete or be cancelled.

#### LoserDrainViolation

**Symptom**: Race losers were not properly drained.

**Common causes**:
- `select!` without draining losing branches
- Incomplete cancellation protocol

**Fix**: Always await or cancel all select branches.

## Best Practices

### Pattern 1: Two-Phase Commit

For operations that modify multiple resources, use two-phase commit:

```rust
async fn transfer(cx: &Cx, from: &Account, to: &Account, amount: u64) -> Outcome<(), Error> {
    // Phase 1: Prepare (can be cancelled)
    let debit_voucher = from.prepare_debit(cx, amount).await?;
    let credit_voucher = to.prepare_credit(cx, amount).await?;

    // Phase 2: Commit (masked from cancellation)
    cx.mask_cancellation(|cx| async {
        debit_voucher.commit(cx).await?;
        credit_voucher.commit(cx).await?;
        Ok(())
    }).await
}
```

### Pattern 2: RAII Guards

Use guard types that clean up on drop:

```rust
struct ConnectionGuard {
    conn: Connection,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        // Cleanup happens even if cancelled
        self.conn.release();
    }
}

async fn use_connection(cx: &Cx) -> Outcome<(), Error> {
    let guard = ConnectionGuard { conn: acquire_connection().await? };

    do_work(&guard.conn).await?;

    // guard.drop() runs even if we're cancelled here
    Ok(())
}
```

### Pattern 3: Finalizer Registration

For cleanup that must run even after cancellation:

```rust
async fn process_batch(cx: &Cx, batch: Batch) -> Outcome<(), Error> {
    // Register finalizer before acquiring resources
    let finalizer_id = cx.register_finalizer(|| {
        batch.abort();
    });

    batch.process(cx).await?;

    // Deregister on success
    cx.deregister_finalizer(finalizer_id);
    batch.commit();
    Ok(())
}
```

### Pattern 4: Masking Critical Sections

For code that must not be cancelled:

```rust
async fn atomic_update(cx: &Cx, state: &State) -> Outcome<(), Error> {
    // This section will complete even if cancellation arrives
    cx.mask_cancellation(|cx| async {
        state.begin_transaction();
        state.update();
        state.commit_transaction();
    }).await
}
```

## CI Integration

### JSON Output for Parsing

```rust
let report = lab(seed).run(/* ... */);
let json = report.to_json();
println!("{}", serde_json::to_string_pretty(&json).unwrap());
```

Output:
```json
{
  "summary": {
    "total_await_points": 15,
    "tests_run": 15,
    "passed": 14,
    "failed": 1,
    "strategy": "AllPoints",
    "seed": 42,
    "verdict": "FAIL"
  },
  "failures": [
    {
      "index": 1,
      "injection_point": 7,
      "outcome": "Success",
      "await_points_before": 6,
      "oracle_violations": ["ObligationLeak: ..."],
      "reproduction_code": "..."
    }
  ]
}
```

### JUnit XML for Test Frameworks

```rust
let report = lab(seed).run(/* ... */);
std::fs::write("test-results.xml", report.to_junit_xml()).unwrap();
```

Most CI systems (Jenkins, GitHub Actions, GitLab CI) can parse JUnit XML for
test result visualization and failure tracking.

### Recommended CI Strategy

For CI pipelines, balance coverage with execution time:

```rust
#[test]
fn cancellation_injection_ci() {
    // Use environment variable for seed to enable reproduction
    let seed = std::env::var("CI_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());

    println!("Using seed: {}", seed);

    // In CI, use RandomSample for speed; locally, use AllPoints for thoroughness
    let strategy = if std::env::var("CI").is_ok() {
        InjectionStrategy::RandomSample(10)
    } else {
        InjectionStrategy::AllPoints
    };

    let report = lab(seed)
        .with_cancellation_injection(strategy)
        .with_all_oracles()
        .run(|injector| {
            InstrumentedFuture::new(my_critical_operation(), injector)
        });

    if !report.all_passed() {
        // Output JSON for CI parsing
        eprintln!("{}", serde_json::to_string_pretty(&report.to_json()).unwrap());
        panic!("Cancellation injection tests failed. Seed: {}", seed);
    }
}
```

## Troubleshooting

### Test Hangs

**Symptom**: Test never completes.

**Cause**: The instrumented future entered an infinite loop or deadlock.

**Fix**: Set `max_steps` on the lab builder:

```rust
lab(42)
    .max_steps(10000)  // Fail after 10000 steps
    .run(/* ... */);
```

### Non-Deterministic Failures

**Symptom**: Same seed gives different results.

**Cause**: Code uses real time, random numbers, or non-deterministic I/O.

**Fix**: Ensure all randomness and time comes from the lab runtime's virtual clock.

### Too Many Await Points

**Symptom**: Test takes too long with AllPoints.

**Cause**: Complex futures with many await points.

**Fix**: Use `RandomSample` or `FirstN` for routine CI, AllPoints for release validation.

## API Reference

See the rustdoc for:
- [`lab::injection`](../src/lab/injection.rs) - Lab integration
- [`lab::instrumented_future`](../src/lab/instrumented_future.rs) - Core instrumentation
- [`lab::oracle`](../src/lab/oracle.rs) - Oracle verification

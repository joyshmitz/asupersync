# Asupersync

A spec-first, cancel-correct, capability-secure async runtime for Rust.

## Vision

Asupersync is not "an executor plus helpers." It is a **semantic platform**:

- A **small kernel** of primitives with a **precise operational semantics**
- A **capability/effect boundary** (`Cx`) that prevents ambient authority and enables determinism + distribution
- **Structured concurrency by construction**: tasks are owned by regions; regions close to quiescence
- **Cancellation as a protocol**: request → drain → finalize (idempotent, budgeted, schedulable)
- **Two-phase effects by default**: reserve/commit + ack tokens + leases prevent silent data loss
- **Obligation tracking**: permits/acks/leases/finalizers make "futurelock" and effect leaks detectable
- A **lab runtime** with virtual time and deterministic scheduling for reproducible concurrency testing
- A **distributed story that is honest**: named computations + leases + idempotency, not "serialize closures across machines"

## Core Guarantees

If you use Asupersync primitives, you get:

- **Cancel correctness**: cancellation is explicit, bounded, and driven to completion
- **No orphan tasks**: every spawned task is owned by a region
- **Bounded cleanup**: cleanup budgets are sufficient conditions, not hopes
- **Predictable shutdown**: regions close to quiescence with all finalizers run
- **Replayable traces**: deterministic lab runtime enables trace capture and replay

## Execution Tiers

1. **Fibers**: borrow-friendly, region-pinned, same-thread execution
2. **Tasks**: parallel, `Send`, migrate across worker threads
3. **Actors**: long-lived supervised tasks (still region-owned)
4. **Remote**: named computations executed elsewhere with leases + idempotency

## Mathematical Foundations

Asupersync's design is grounded in:

- **Outcome lattice**: `Ok < Err < Cancelled < Panicked` (monotone aggregation)
- **Concurrency semiring**: `join` and `race` with algebraic laws
- **Linear obligations**: reserve/commit discipline from linear logic
- **Tropical budgets**: deadline/quota propagation via idempotent semirings
- **Mazurkiewicz traces**: true concurrency semantics (not just interleavings)
- **Game-theoretic cancellation**: System vs Task with quantitative bounds

## Documentation

- [`asupersync_plan_v4.md`](./asupersync_plan_v4.md) — Design Bible
- [`asupersync_v4_formal_semantics.md`](./asupersync_v4_formal_semantics.md) — Operational Semantics
- [`asupersync_v4_api_skeleton.rs`](./asupersync_v4_api_skeleton.rs) — Rust API Skeleton
- [`AGENTS.md`](./AGENTS.md) — Guidelines for AI coding agents

## Status

**Pre-implementation design phase.** The specification documents are complete; implementation has not yet begun.

## Roadmap

- Phase 0: Single-thread deterministic kernel
- Phase 1: Parallel scheduler + region heap
- Phase 2: I/O integration
- Phase 3: Actors + session types
- Phase 4: Distributed structured concurrency
- Phase 5: DPOR + TLA+ tooling

## License

TBD

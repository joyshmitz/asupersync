#![allow(missing_docs)]
//! Leak Regression E2E Suite (bd-105vq).
//!
//! End-to-end tests for leak detection across tasks, obligations, regions,
//! and admission limits. Exercises the marking analyzer, static leak checker,
//! and graded obligation runtime guards under stress.
//!
//! Coverage scope:
//!   - Marking analysis: clean lifecycle, leak detection, multi-region
//!   - Static leak checker: branch coverage, definite/potential leaks
//!   - Graded obligations: drop bomb, scope tracking, typestate tokens
//!   - Mixed obligation kinds under stress
//!   - Region close with outstanding obligations
//!   - Deterministic reproducibility
//!
//! Cross-references:
//!   Marking unit tests:   src/obligation/marking.rs
//!   Static checker tests: src/obligation/leak_check.rs
//!   Graded type tests:    src/obligation/graded.rs
//!   Commutativity tests:  tests/repro_leak_check_commutativity.rs

use asupersync::obligation::graded::{GradedObligation, GradedScope, Resolution};
use asupersync::obligation::marking::{MarkingAnalyzer, MarkingEvent, MarkingEventKind};
use asupersync::obligation::{BodyBuilder, LeakChecker};
use asupersync::record::ObligationKind;
use asupersync::test_utils::init_test_logging;
use asupersync::types::{ObligationId, RegionId, TaskId, Time};

// ===========================================================================
// HELPERS
// ===========================================================================

fn all_kinds() -> [ObligationKind; 4] {
    [
        ObligationKind::SendPermit,
        ObligationKind::Ack,
        ObligationKind::Lease,
        ObligationKind::IoOp,
    ]
}

fn make_event(time_ns: u64, kind: MarkingEventKind) -> MarkingEvent {
    MarkingEvent::new(Time::from_nanos(time_ns), kind)
}

fn obligation(idx: u32) -> ObligationId {
    ObligationId::new_for_test(idx, 0)
}

fn region(idx: u32) -> RegionId {
    RegionId::new_for_test(idx, 0)
}

fn task(idx: u32) -> TaskId {
    TaskId::new_for_test(idx, 0)
}

// ===========================================================================
// MARKING ANALYSIS: CLEAN LIFECYCLES
// ===========================================================================

#[test]
fn marking_clean_single_commit() {
    init_test_logging();
    let r0 = region(0);
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::SendPermit,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            10,
            MarkingEventKind::Commit {
                obligation: obligation(0),
                region: r0,
                kind: ObligationKind::SendPermit,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(
        result.is_safe(),
        "clean commit should be safe: {:?}",
        result.leaks
    );
    assert_eq!(result.stats.total_reserved, 1);
    assert_eq!(result.stats.total_committed, 1);
    assert_eq!(result.stats.total_leaked, 0);
}

#[test]
fn marking_clean_single_abort() {
    init_test_logging();
    let r0 = region(0);
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::Lease,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            10,
            MarkingEventKind::Abort {
                obligation: obligation(0),
                region: r0,
                kind: ObligationKind::Lease,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(result.is_safe());
    assert_eq!(result.stats.total_reserved, 1);
    assert_eq!(result.stats.total_aborted, 1);
    assert_eq!(result.stats.total_leaked, 0);
}

#[test]
fn marking_clean_all_kinds() {
    init_test_logging();
    let r0 = region(0);
    let mut events = Vec::new();
    let mut time = 0u64;

    for (i, kind) in all_kinds().iter().enumerate() {
        let o = obligation(i as u32);
        events.push(make_event(
            time,
            MarkingEventKind::Reserve {
                obligation: o,
                kind: *kind,
                task: task(0),
                region: r0,
            },
        ));
        time += 10;
        // Alternate commit/abort.
        if i % 2 == 0 {
            events.push(make_event(
                time,
                MarkingEventKind::Commit {
                    obligation: o,
                    region: r0,
                    kind: *kind,
                },
            ));
        } else {
            events.push(make_event(
                time,
                MarkingEventKind::Abort {
                    obligation: o,
                    region: r0,
                    kind: *kind,
                },
            ));
        }
        time += 10;
    }
    events.push(make_event(
        time,
        MarkingEventKind::RegionClose { region: r0 },
    ));

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(result.is_safe());
    assert_eq!(result.stats.total_reserved, 4);
    assert_eq!(result.stats.total_committed, 2);
    assert_eq!(result.stats.total_aborted, 2);
    assert_eq!(result.stats.total_leaked, 0);
    assert_eq!(result.stats.distinct_kinds, 4);
}

// ===========================================================================
// MARKING ANALYSIS: LEAK DETECTION
// ===========================================================================

#[test]
fn marking_detects_single_leak_on_region_close() {
    init_test_logging();
    let r0 = region(0);
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::SendPermit,
                task: task(0),
                region: r0,
            },
        ),
        // No commit or abort.
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(
        !result.is_safe(),
        "unresolved obligation should be detected"
    );
    assert_eq!(result.leak_count(), 1, "should find 1 leak on region close");
    assert_eq!(result.leaks[0].region, r0);
    assert_eq!(result.leaks[0].kind, ObligationKind::SendPermit);
}

#[test]
fn marking_detects_multiple_leaks_same_region() {
    init_test_logging();
    let r0 = region(0);
    let mut events = Vec::new();

    for i in 0..5u32 {
        events.push(make_event(
            u64::from(i) * 10,
            MarkingEventKind::Reserve {
                obligation: obligation(i),
                kind: ObligationKind::Ack,
                task: task(0),
                region: r0,
            },
        ));
    }
    // Resolve only 2 of 5.
    events.push(make_event(
        60,
        MarkingEventKind::Commit {
            obligation: obligation(0),
            region: r0,
            kind: ObligationKind::Ack,
        },
    ));
    events.push(make_event(
        70,
        MarkingEventKind::Abort {
            obligation: obligation(1),
            region: r0,
            kind: ObligationKind::Ack,
        },
    ));
    events.push(make_event(
        100,
        MarkingEventKind::RegionClose { region: r0 },
    ));

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(!result.is_safe());
    assert_eq!(result.stats.total_reserved, 5);
    assert_eq!(result.stats.total_committed, 1);
    assert_eq!(result.stats.total_aborted, 1);
    // 3 obligations remain pending at region close → 1 LeakViolation for Ack kind.
    assert_eq!(result.leak_count(), 1, "one kind leaked: Ack");
    assert_eq!(result.leaks[0].count, 3, "3 Ack obligations leaked");
}

#[test]
fn marking_detects_leak_with_explicit_leak_event() {
    init_test_logging();
    let r0 = region(0);
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::IoOp,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            10,
            MarkingEventKind::Leak {
                obligation: obligation(0),
                region: r0,
                kind: ObligationKind::IoOp,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    // Explicit Leak event decrements the marking, so region close finds
    // zero pending and is_safe() returns true (no LeakViolation generated).
    // But stats.total_leaked counts the explicit Leak event.
    assert_eq!(result.stats.total_leaked, 1);
    assert_eq!(result.leak_count(), 0, "marking cleared by explicit leak");
}

// ===========================================================================
// MARKING ANALYSIS: MULTI-REGION
// ===========================================================================

#[test]
fn marking_multi_region_independent_clean() {
    init_test_logging();
    let mut events = Vec::new();
    let mut time = 0u64;

    // 4 independent regions, each with 3 obligations.
    for r_idx in 0..4u32 {
        let r = region(r_idx);
        for o_idx in 0..3u32 {
            let o = obligation(r_idx * 10 + o_idx);
            events.push(make_event(
                time,
                MarkingEventKind::Reserve {
                    obligation: o,
                    kind: ObligationKind::SendPermit,
                    task: task(r_idx),
                    region: r,
                },
            ));
            time += 5;
            events.push(make_event(
                time,
                MarkingEventKind::Commit {
                    obligation: o,
                    region: r,
                    kind: ObligationKind::SendPermit,
                },
            ));
            time += 5;
        }
        events.push(make_event(
            time,
            MarkingEventKind::RegionClose { region: r },
        ));
        time += 10;
    }

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(result.is_safe());
    assert_eq!(result.stats.total_reserved, 12);
    assert_eq!(result.stats.total_committed, 12);
    assert_eq!(result.stats.total_leaked, 0);
    assert_eq!(result.stats.distinct_regions, 4);
}

#[test]
fn marking_multi_region_partial_leak() {
    init_test_logging();
    let r_clean = region(0);
    let r_leaky = region(1);

    let events = vec![
        // Region 0: clean.
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::SendPermit,
                task: task(0),
                region: r_clean,
            },
        ),
        make_event(
            10,
            MarkingEventKind::Commit {
                obligation: obligation(0),
                region: r_clean,
                kind: ObligationKind::SendPermit,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r_clean }),
        // Region 1: leak.
        make_event(
            30,
            MarkingEventKind::Reserve {
                obligation: obligation(1),
                kind: ObligationKind::Lease,
                task: task(1),
                region: r_leaky,
            },
        ),
        make_event(40, MarkingEventKind::RegionClose { region: r_leaky }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(!result.is_safe());
    assert_eq!(result.leaks.len(), 1);
    assert_eq!(result.leaks[0].region, r_leaky);
    assert_eq!(result.leaks[0].kind, ObligationKind::Lease);
}

// ===========================================================================
// MARKING ANALYSIS: STRESS
// ===========================================================================

#[test]
fn marking_stress_1000_obligations_clean() {
    init_test_logging();
    let r0 = region(0);
    let mut events = Vec::new();

    for i in 0..1000u32 {
        let kind = all_kinds()[usize::from((i % 4) as u8)];
        events.push(make_event(
            u64::from(i) * 2,
            MarkingEventKind::Reserve {
                obligation: obligation(i),
                kind,
                task: task(i % 10),
                region: r0,
            },
        ));
        events.push(make_event(
            u64::from(i) * 2 + 1,
            MarkingEventKind::Commit {
                obligation: obligation(i),
                region: r0,
                kind,
            },
        ));
    }
    events.push(make_event(
        2001,
        MarkingEventKind::RegionClose { region: r0 },
    ));

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(result.is_safe());
    assert_eq!(result.stats.total_reserved, 1000);
    assert_eq!(result.stats.total_committed, 1000);
    assert_eq!(result.stats.total_leaked, 0);
    assert_eq!(result.stats.distinct_kinds, 4);
}

#[test]
fn marking_stress_100_regions_with_scattered_leaks() {
    init_test_logging();
    let mut events = Vec::new();
    let mut time = 0u64;
    let mut expected_leaks = 0u32;

    for r_idx in 0..100u32 {
        let r = region(r_idx);
        let o = obligation(r_idx);
        let kind = all_kinds()[(r_idx % 4) as usize];

        events.push(make_event(
            time,
            MarkingEventKind::Reserve {
                obligation: o,
                kind,
                task: task(0),
                region: r,
            },
        ));
        time += 5;

        // Leak every 7th region.
        if r_idx % 7 != 0 {
            events.push(make_event(
                time,
                MarkingEventKind::Commit {
                    obligation: o,
                    region: r,
                    kind,
                },
            ));
        } else {
            expected_leaks += 1;
        }
        time += 5;
        events.push(make_event(
            time,
            MarkingEventKind::RegionClose { region: r },
        ));
        time += 5;
    }

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(!result.is_safe());
    // Each leaked region produces one LeakViolation per kind with pending > 0.
    assert_eq!(
        result.leak_count() as u32,
        expected_leaks,
        "expected {expected_leaks} leak violations"
    );
    assert_eq!(result.stats.distinct_regions, 100);
}

// ===========================================================================
// STATIC LEAK CHECKER: CLEAN PATHS
// ===========================================================================

#[test]
fn static_checker_straight_line_commit() {
    init_test_logging();
    let mut b = BodyBuilder::new("straight_commit");
    let v = b.reserve(ObligationKind::SendPermit);
    b.commit(v);
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(
        result.is_clean(),
        "straight-line commit: {:?}",
        result.leaks()
    );
}

#[test]
fn static_checker_branch_both_arms_resolve() {
    init_test_logging();
    let mut b = BodyBuilder::new("both_arms_resolve");
    let v = b.reserve(ObligationKind::Lease);
    b.branch(|bb| {
        bb.arm(|a| {
            a.commit(v);
        });
        bb.arm(|a| {
            a.abort(v);
        });
    });
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(result.is_clean());
}

#[test]
fn static_checker_multiple_obligations_all_resolved() {
    init_test_logging();
    let mut b = BodyBuilder::new("multi_obligation");
    let v0 = b.reserve(ObligationKind::SendPermit);
    let v1 = b.reserve(ObligationKind::Ack);
    let v2 = b.reserve(ObligationKind::Lease);
    b.commit(v0);
    b.abort(v1);
    b.commit(v2);
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(result.is_clean());
}

// ===========================================================================
// STATIC LEAK CHECKER: LEAK DETECTION
// ===========================================================================

#[test]
fn static_checker_definite_leak_no_resolve() {
    init_test_logging();
    let mut b = BodyBuilder::new("no_resolve");
    let _v = b.reserve(ObligationKind::SendPermit);
    // Never resolved.
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(!result.is_clean());
    assert!(!result.leaks().is_empty());
}

#[test]
fn static_checker_potential_leak_one_arm_missing() {
    init_test_logging();
    let mut b = BodyBuilder::new("one_arm_missing");
    let v = b.reserve(ObligationKind::Ack);
    b.branch(|bb| {
        bb.arm(|a| {
            a.commit(v);
        });
        bb.arm(|_a| {
            // Missing resolve on this path.
        });
    });
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(!result.is_clean(), "should detect potential leak");
}

#[test]
fn static_checker_nested_branch_leak() {
    init_test_logging();
    let mut b = BodyBuilder::new("nested_branch_leak");
    let v = b.reserve(ObligationKind::IoOp);
    b.branch(|bb| {
        bb.arm(|a| {
            a.branch(|inner| {
                inner.arm(|a2| {
                    a2.commit(v);
                });
                inner.arm(|_a2| {
                    // Leak path.
                });
            });
        });
        bb.arm(|a| {
            a.abort(v);
        });
    });
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(!result.is_clean(), "nested branch leak should be detected");
}

#[test]
fn static_checker_multiple_leaks_different_kinds() {
    init_test_logging();
    let mut b = BodyBuilder::new("multi_kind_leak");
    let _v0 = b.reserve(ObligationKind::SendPermit);
    let _v1 = b.reserve(ObligationKind::Lease);
    let v2 = b.reserve(ObligationKind::Ack);
    // Only v2 resolved.
    b.commit(v2);
    let body = b.build();

    let mut checker = LeakChecker::new();
    let result = checker.check(&body);
    assert!(!result.is_clean());
    assert!(
        result.leaks().len() >= 2,
        "should detect at least 2 leaks, got {}",
        result.leaks().len()
    );
}

// ===========================================================================
// GRADED OBLIGATIONS: DROP BOMB
// ===========================================================================

#[test]
fn graded_obligation_clean_commit() {
    init_test_logging();
    let ob = GradedObligation::reserve(ObligationKind::SendPermit, "test-commit");
    let proof = ob.resolve(Resolution::Commit);
    assert_eq!(proof.kind, ObligationKind::SendPermit);
    assert_eq!(proof.resolution, Resolution::Commit);
}

#[test]
fn graded_obligation_clean_abort() {
    init_test_logging();
    let ob = GradedObligation::reserve(ObligationKind::Lease, "test-abort");
    let proof = ob.resolve(Resolution::Abort);
    assert_eq!(proof.kind, ObligationKind::Lease);
    assert_eq!(proof.resolution, Resolution::Abort);
}

#[test]
#[should_panic(expected = "OBLIGATION LEAKED")]
fn graded_obligation_drop_without_resolve_panics() {
    init_test_logging();
    let _ob = GradedObligation::reserve(ObligationKind::SendPermit, "leaked-permit");
    // Dropped without resolve → panic.
}

#[test]
fn graded_obligation_into_raw_disarms() {
    init_test_logging();
    let ob = GradedObligation::reserve(ObligationKind::IoOp, "raw-escape");
    let raw = ob.into_raw();
    assert_eq!(raw.kind, ObligationKind::IoOp);
    // No panic on drop.
}

// ===========================================================================
// GRADED SCOPE: TRACKING
// ===========================================================================

#[test]
fn graded_scope_clean_close() {
    init_test_logging();
    let mut scope = GradedScope::open("clean-scope");

    for kind in &all_kinds() {
        let ob = GradedObligation::reserve(*kind, format!("test-{kind:?}"));
        scope.on_reserve();
        let _ = ob.resolve(Resolution::Commit);
        scope.on_resolve();
    }

    assert_eq!(scope.outstanding(), 0);
    let proof = scope.close().expect("should close cleanly");
    assert_eq!(proof.total_reserved, 4);
    assert_eq!(proof.total_resolved, 4);
}

#[test]
fn graded_scope_close_with_outstanding_returns_error() {
    init_test_logging();
    let mut scope = GradedScope::open("leaky-scope");

    // Reserve 3, resolve 1.
    for _ in 0..3 {
        let ob = GradedObligation::reserve(ObligationKind::SendPermit, "permit");
        scope.on_reserve();
        // Keep 2 of them alive by resolving via into_raw to avoid panic.
        let _ = ob.into_raw();
    }
    scope.on_resolve(); // Only resolve 1.

    assert_eq!(scope.outstanding(), 2);
    let err = scope.close().expect_err("should detect outstanding");
    assert_eq!(err.outstanding, 2);
    assert_eq!(err.reserved, 3);
    assert_eq!(err.resolved, 1);
}

#[test]
fn graded_scope_stress_many_obligations() {
    init_test_logging();
    let mut scope = GradedScope::open("stress-scope");
    let n: usize = 500;

    for i in 0..n {
        let kind = all_kinds()[i % 4];
        let ob = GradedObligation::reserve(kind, format!("ob-{i}"));
        scope.on_reserve();
        if i % 2 == 0 {
            let _ = ob.resolve(Resolution::Commit);
        } else {
            let _ = ob.resolve(Resolution::Abort);
        }
        scope.on_resolve();
    }

    assert_eq!(scope.outstanding(), 0);
    let proof = scope.close().expect("should close cleanly");
    let expected = u32::try_from(n).expect("n fits u32");
    assert_eq!(proof.total_reserved, expected);
    assert_eq!(proof.total_resolved, expected);
}

// ===========================================================================
// COMBINED: MARKING + STATIC + GRADED
// ===========================================================================

#[test]
fn combined_all_checkers_agree_clean() {
    init_test_logging();

    // Static check.
    let mut b = BodyBuilder::new("combined_clean");
    let v0 = b.reserve(ObligationKind::SendPermit);
    let v1 = b.reserve(ObligationKind::Lease);
    b.commit(v0);
    b.abort(v1);
    let mut checker = LeakChecker::new();
    let static_result = checker.check(&b.build());
    assert!(static_result.is_clean());

    // Marking analysis.
    let r0 = region(0);
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::SendPermit,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            5,
            MarkingEventKind::Reserve {
                obligation: obligation(1),
                kind: ObligationKind::Lease,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            10,
            MarkingEventKind::Commit {
                obligation: obligation(0),
                region: r0,
                kind: ObligationKind::SendPermit,
            },
        ),
        make_event(
            15,
            MarkingEventKind::Abort {
                obligation: obligation(1),
                region: r0,
                kind: ObligationKind::Lease,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];
    let mut analyzer = MarkingAnalyzer::new();
    let marking_result = analyzer.analyze(&events);
    assert!(marking_result.is_safe());

    // Graded runtime.
    let mut scope = GradedScope::open("combined");
    let ob0 = GradedObligation::reserve(ObligationKind::SendPermit, "sp");
    scope.on_reserve();
    let ob1 = GradedObligation::reserve(ObligationKind::Lease, "lease");
    scope.on_reserve();
    let _ = ob0.resolve(Resolution::Commit);
    scope.on_resolve();
    let _ = ob1.resolve(Resolution::Abort);
    scope.on_resolve();
    scope.close().expect("clean");
}

#[test]
fn combined_all_checkers_agree_leaky() {
    init_test_logging();

    // Static check: one arm leaks.
    let mut b = BodyBuilder::new("combined_leaky");
    let v = b.reserve(ObligationKind::SendPermit);
    b.branch(|bb| {
        bb.arm(|a| {
            a.commit(v);
        });
        bb.arm(|_a| {});
    });
    let mut checker = LeakChecker::new();
    let static_result = checker.check(&b.build());
    assert!(!static_result.is_clean());

    // Marking analysis: leak on region close.
    let r0 = region(0);
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::SendPermit,
                task: task(0),
                region: r0,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];
    let mut analyzer = MarkingAnalyzer::new();
    let marking_result = analyzer.analyze(&events);
    assert!(!marking_result.is_safe());

    // Graded runtime: scope leak error.
    let mut scope = GradedScope::open("combined-leaky");
    let ob = GradedObligation::reserve(ObligationKind::SendPermit, "sp");
    scope.on_reserve();
    let _ = ob.into_raw(); // Disarm to avoid panic, but don't resolve.
    let err = scope.close().expect_err("should detect leak");
    assert_eq!(err.outstanding, 1);
}

// ===========================================================================
// DETERMINISTIC REPRODUCIBILITY
// ===========================================================================

#[test]
fn marking_deterministic_same_events_same_result() {
    init_test_logging();
    let r0 = region(0);

    let build_events = || {
        let mut events = Vec::new();
        for i in 0..50u32 {
            let kind = all_kinds()[usize::from((i % 4) as u8)];
            events.push(make_event(
                u64::from(i) * 2,
                MarkingEventKind::Reserve {
                    obligation: obligation(i),
                    kind,
                    task: task(i % 5),
                    region: r0,
                },
            ));
            // Leak every 10th.
            if i % 10 != 0 {
                events.push(make_event(
                    u64::from(i) * 2 + 1,
                    MarkingEventKind::Commit {
                        obligation: obligation(i),
                        region: r0,
                        kind,
                    },
                ));
            }
        }
        events.push(make_event(
            200,
            MarkingEventKind::RegionClose { region: r0 },
        ));
        events
    };

    let events1 = build_events();
    let events2 = build_events();

    let mut a1 = MarkingAnalyzer::new();
    let r1 = a1.analyze(&events1);

    let mut a2 = MarkingAnalyzer::new();
    let r2 = a2.analyze(&events2);

    assert_eq!(r1.stats.total_reserved, r2.stats.total_reserved);
    assert_eq!(r1.stats.total_committed, r2.stats.total_committed);
    assert_eq!(r1.stats.total_leaked, r2.stats.total_leaked);
    assert_eq!(r1.leaks.len(), r2.leaks.len());
    assert_eq!(r1.is_safe(), r2.is_safe());
}

// ===========================================================================
// EDGE CASES
// ===========================================================================

#[test]
fn marking_empty_trace_is_safe() {
    init_test_logging();
    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&[]);
    assert!(result.is_safe());
    assert_eq!(result.stats.total_reserved, 0);
}

#[test]
fn marking_region_close_no_obligations_is_safe() {
    init_test_logging();
    let events = vec![make_event(
        0,
        MarkingEventKind::RegionClose { region: region(0) },
    )];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(result.is_safe());
}

#[test]
fn static_checker_empty_body_clean() {
    init_test_logging();
    let b = BodyBuilder::new("empty");
    let mut checker = LeakChecker::new();
    let result = checker.check(&b.build());
    assert!(result.is_clean());
}

#[test]
fn graded_scope_empty_close() {
    init_test_logging();
    let scope = GradedScope::open("empty");
    let proof = scope.close().expect("empty scope should close");
    assert_eq!(proof.total_reserved, 0);
    assert_eq!(proof.total_resolved, 0);
}

// ===========================================================================
// REGRESSION: CANCEL MID-RESERVE PATTERN
// ===========================================================================

#[test]
fn marking_cancel_mid_reserve_aborts_clean() {
    init_test_logging();
    let r0 = region(0);

    // Pattern: task reserves obligations, gets cancelled, aborts all.
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::SendPermit,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            5,
            MarkingEventKind::Reserve {
                obligation: obligation(1),
                kind: ObligationKind::Ack,
                task: task(0),
                region: r0,
            },
        ),
        // Cancellation triggers abort of all outstanding.
        make_event(
            10,
            MarkingEventKind::Abort {
                obligation: obligation(0),
                region: r0,
                kind: ObligationKind::SendPermit,
            },
        ),
        make_event(
            11,
            MarkingEventKind::Abort {
                obligation: obligation(1),
                region: r0,
                kind: ObligationKind::Ack,
            },
        ),
        make_event(20, MarkingEventKind::RegionClose { region: r0 }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(result.is_safe());
    assert_eq!(result.stats.total_reserved, 2);
    assert_eq!(result.stats.total_aborted, 2);
    assert_eq!(result.stats.total_leaked, 0);
}

#[test]
fn marking_cancel_mid_reserve_partial_leak() {
    init_test_logging();
    let r0 = region(0);

    // Task reserves 3, cancelled, only aborts 2 (race condition leak).
    let events = vec![
        make_event(
            0,
            MarkingEventKind::Reserve {
                obligation: obligation(0),
                kind: ObligationKind::Lease,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            5,
            MarkingEventKind::Reserve {
                obligation: obligation(1),
                kind: ObligationKind::Lease,
                task: task(0),
                region: r0,
            },
        ),
        make_event(
            10,
            MarkingEventKind::Reserve {
                obligation: obligation(2),
                kind: ObligationKind::Lease,
                task: task(0),
                region: r0,
            },
        ),
        // Only 2 of 3 aborted.
        make_event(
            15,
            MarkingEventKind::Abort {
                obligation: obligation(0),
                region: r0,
                kind: ObligationKind::Lease,
            },
        ),
        make_event(
            16,
            MarkingEventKind::Abort {
                obligation: obligation(1),
                region: r0,
                kind: ObligationKind::Lease,
            },
        ),
        make_event(30, MarkingEventKind::RegionClose { region: r0 }),
    ];

    let mut analyzer = MarkingAnalyzer::new();
    let result = analyzer.analyze(&events);
    assert!(!result.is_safe());
    assert_eq!(result.leak_count(), 1, "one Lease leak on region close");
    assert_eq!(result.leaks[0].count, 1, "1 Lease obligation leaked");
}

// ===========================================================================
// STATIC CHECKER: REALISTIC PATTERNS
// ===========================================================================

#[test]
fn static_checker_realistic_send_with_error_handling() {
    init_test_logging();
    let mut b = BodyBuilder::new("send_with_error");
    let permit = b.reserve(ObligationKind::SendPermit);
    b.branch(|bb| {
        // Success path: commit.
        bb.arm(|a| {
            a.commit(permit);
        });
        // Error path: abort (correct error handling).
        bb.arm(|a| {
            a.abort(permit);
        });
    });

    let mut checker = LeakChecker::new();
    let result = checker.check(&b.build());
    assert!(
        result.is_clean(),
        "send with proper error handling should be clean"
    );
}

#[test]
fn static_checker_realistic_lease_renewal_loop() {
    init_test_logging();
    // Model: acquire lease, branch (renew success → new lease, fail → abort).
    let mut b = BodyBuilder::new("lease_renewal");
    let lease = b.reserve(ObligationKind::Lease);
    b.branch(|bb| {
        // Renew success: commit old, reserve new, commit new.
        bb.arm(|a| {
            a.commit(lease);
        });
        // Renew fail: abort.
        bb.arm(|a| {
            a.abort(lease);
        });
    });

    let mut checker = LeakChecker::new();
    let result = checker.check(&b.build());
    assert!(result.is_clean());
}

#[test]
fn static_checker_realistic_io_without_cleanup() {
    init_test_logging();
    // Model: start I/O, branch (success, error without cleanup → leak).
    let mut b = BodyBuilder::new("io_no_cleanup");
    let io = b.reserve(ObligationKind::IoOp);
    b.branch(|bb| {
        bb.arm(|a| {
            a.commit(io);
        });
        bb.arm(|_a| {
            // BUG: error path doesn't abort.
        });
    });

    let mut checker = LeakChecker::new();
    let result = checker.check(&b.build());
    assert!(
        !result.is_clean(),
        "missing error cleanup should be detected"
    );
}

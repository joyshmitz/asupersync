//! Task explanation E2E tests.

use crate::console_e2e::util::init_console_test;
use asupersync::cx::Cx;
use asupersync::observability::{BlockReason, Diagnostics};
use asupersync::types::TaskId;

#[test]
fn e2e_diagnostics_explain_task_not_found() {
    init_console_test("e2e_diagnostics_explain_task_not_found");

    let cx = Cx::for_testing();
    let state = cx.runtime_state().clone();
    let diagnostics = Diagnostics::new(state);

    // Query a non-existent task
    let fake_id = TaskId::from_raw(88888);
    let explanation = diagnostics.explain_task_blocked(fake_id);

    let is_not_found = matches!(explanation.block_reason, BlockReason::TaskNotFound);
    crate::assert_with_log!(
        is_not_found,
        "task not found",
        true,
        is_not_found
    );

    crate::test_complete!("e2e_diagnostics_explain_task_not_found");
}

#[test]
fn e2e_diagnostics_explain_task_display() {
    init_console_test("e2e_diagnostics_explain_task_display");

    let cx = Cx::for_testing();
    let state = cx.runtime_state().clone();
    let diagnostics = Diagnostics::new(state);

    // Get an explanation and verify it can be displayed
    let fake_id = TaskId::from_raw(77777);
    let explanation = diagnostics.explain_task_blocked(fake_id);

    let display = format!("{explanation}");
    crate::assert_with_log!(
        display.contains("Task"),
        "display has task",
        true,
        display.contains("Task")
    );
    crate::assert_with_log!(
        display.contains("blocked"),
        "display has blocked",
        true,
        display.contains("blocked")
    );

    crate::test_complete!("e2e_diagnostics_explain_task_display");
}

#[test]
fn e2e_diagnostics_block_reason_display() {
    init_console_test("e2e_diagnostics_block_reason_display");

    // Test that each BlockReason variant has meaningful Display
    let variants = [
        (BlockReason::TaskNotFound, "not found"),
        (BlockReason::NotStarted, "not started"),
        (BlockReason::AwaitingSchedule, "schedule"),
        (BlockReason::Completed, "completed"),
        (
            BlockReason::AwaitingFuture {
                description: "test".to_string(),
            },
            "future",
        ),
    ];

    for (reason, expected_substring) in variants {
        let display = format!("{reason}");
        crate::assert_with_log!(
            display.to_lowercase().contains(expected_substring),
            "block reason display",
            true,
            display.to_lowercase().contains(expected_substring)
        );
    }

    crate::test_complete!("e2e_diagnostics_block_reason_display");
}

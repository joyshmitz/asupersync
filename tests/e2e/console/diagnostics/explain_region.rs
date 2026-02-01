//! Region explanation E2E tests.

use crate::console_e2e::util::init_console_test;
use asupersync::cx::Cx;
use asupersync::observability::{Diagnostics, Reason};
use asupersync::types::RegionId;

#[test]
fn e2e_diagnostics_explain_region_not_found() {
    init_console_test("e2e_diagnostics_explain_region_not_found");

    let cx = Cx::for_testing();
    let state = cx.runtime_state().clone();
    let diagnostics = Diagnostics::new(state);

    // Query a non-existent region
    let fake_id = RegionId::from_raw(99999);
    let explanation = diagnostics.explain_region_open(fake_id);

    crate::assert_with_log!(
        explanation.region_state.is_none(),
        "no region state",
        true,
        explanation.region_state.is_none()
    );

    let has_not_found = explanation
        .reasons
        .iter()
        .any(|r| matches!(r, Reason::RegionNotFound));
    crate::assert_with_log!(
        has_not_found,
        "reason is not found",
        true,
        has_not_found
    );

    crate::test_complete!("e2e_diagnostics_explain_region_not_found");
}

#[test]
fn e2e_diagnostics_explain_region_display() {
    init_console_test("e2e_diagnostics_explain_region_display");

    let cx = Cx::for_testing();
    let state = cx.runtime_state().clone();
    let diagnostics = Diagnostics::new(state);

    // Get an explanation and verify it can be displayed
    let fake_id = RegionId::from_raw(12345);
    let explanation = diagnostics.explain_region_open(fake_id);

    // The Display impl should produce readable output
    let display = format!("{explanation}");

    crate::assert_with_log!(
        display.contains("Region"),
        "display has region",
        true,
        display.contains("Region")
    );
    crate::assert_with_log!(
        display.contains("12345") || display.contains("RegionId"),
        "display has id",
        true,
        display.contains("12345") || display.contains("RegionId")
    );

    crate::test_complete!("e2e_diagnostics_explain_region_display");
}

#[test]
fn e2e_diagnostics_reason_display() {
    init_console_test("e2e_diagnostics_reason_display");

    // Test that each Reason variant has a meaningful Display
    let not_found = Reason::RegionNotFound;
    let display = format!("{not_found}");
    crate::assert_with_log!(
        display.contains("not found"),
        "not found display",
        true,
        display.contains("not found")
    );

    crate::test_complete!("e2e_diagnostics_reason_display");
}

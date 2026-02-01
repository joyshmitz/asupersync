//! Common utilities and fixtures for console E2E tests.

use asupersync::cx::Cx;
use asupersync::runtime::state::RuntimeState;
use std::sync::Arc;

/// Create a minimal runtime state for diagnostics testing.
pub fn create_test_runtime_state() -> Arc<RuntimeState> {
    // For now, create via Cx::for_testing() which sets up a minimal state
    let cx = Cx::for_testing();
    cx.runtime_state().clone()
}

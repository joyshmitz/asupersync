//! Tracing infrastructure for deterministic replay.
//!
//! This module provides structured tracing for the runtime, enabling:
//!
//! - Deterministic replay of executions
//! - Debugging and analysis of concurrent behavior
//! - Mazurkiewicz trace semantics for DPOR
//!
//! # Submodules
//!
//! - [`event`]: Observability trace events for debugging and analysis
//! - [`replay`]: Compact replay events for deterministic record/replay
//! - [`buffer`]: Ring buffer for recent events
//! - [`format`]: Output formatting utilities

pub mod buffer;
pub mod distributed;
pub mod event;
pub mod format;
pub mod replay;

pub use buffer::TraceBuffer;
pub use event::{TraceData, TraceEvent, TraceEventKind};
pub use replay::{
    CompactRegionId, CompactTaskId, ReplayEvent, ReplayTrace, ReplayTraceError, TraceMetadata,
    REPLAY_SCHEMA_VERSION,
};

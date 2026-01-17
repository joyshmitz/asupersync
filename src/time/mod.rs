//! Time primitives: sleep and timeout operations.
//!
//! This module provides core time-based operations for async programming:
//! - [`Sleep`]: A future that completes after a deadline
//! - [`TimeoutFuture`]: A wrapper that adds a timeout to any future
//!
//! # Virtual vs Wall Time
//!
//! These primitives work with both production (wall clock) time and
//! virtual time in the lab runtime. The time source is determined by
//! the runtime context.
//!
//! # Cancel Safety
//!
//! Both `Sleep` and `TimeoutFuture` are cancel-safe:
//! - `Sleep`: Can be dropped and recreated without side effects
//! - `TimeoutFuture`: The inner future may have side effects on cancellation
//!
//! # Example
//!
//! ```ignore
//! use asupersync::time::{sleep, timeout};
//! use std::time::Duration;
//!
//! // Sleep for 100 milliseconds
//! sleep(Duration::from_millis(100)).await;
//!
//! // Wrap an operation with a timeout
//! match timeout(Duration::from_secs(5), async { expensive_operation() }).await {
//!     Ok(result) => println!("Completed: {result}"),
//!     Err(_) => println!("Timed out!"),
//! }
//! ```

mod driver;
mod elapsed;
mod sleep;
mod timeout_future;

pub use driver::{TimerDriver, TimerHandle, TimeSource, VirtualClock, WallClock};
pub use elapsed::Elapsed;
pub use sleep::{sleep, sleep_until, Sleep};
pub use timeout_future::{timeout, timeout_at, TimeoutFuture};

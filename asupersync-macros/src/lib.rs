//! Proc macros for asupersync structured concurrency runtime.
//!
//! This crate provides procedural macros that simplify working with the asupersync
//! async runtime's structured concurrency primitives. The macros handle the boilerplate
//! for creating scopes, spawning tasks, joining results, and racing computations.
//!
//! # Available Macros
//!
//! - [`scope!`] - Create a structured concurrency scope
//! - [`spawn!`] - Spawn a task within the current scope
//! - [`join!`] - Join multiple futures, waiting for all to complete
//! - [`race!`] - Race multiple futures, returning the first to complete
//!
//! # Example
//!
//! ```ignore
//! use asupersync_macros::{scope, spawn, join, race};
//!
//! async fn example(cx: &mut Cx) {
//!     scope!(cx, {
//!         let handle1 = spawn!(async { compute_a().await });
//!         let handle2 = spawn!(async { compute_b().await });
//!
//!         // Wait for both
//!         let (result_a, result_b) = join!(handle1, handle2);
//!     });
//! }
//! ```

mod join;
mod race;
mod scope;
mod spawn;
mod util;

use proc_macro::TokenStream;

/// Creates a structured concurrency scope.
///
/// The `scope!` macro creates a region that owns spawned tasks and guarantees
/// quiescence on exit. All tasks spawned within the scope are cancelled and
/// drained before the scope completes.
///
/// # Syntax
///
/// ```ignore
/// scope!(cx, {
///     // body with spawned tasks
/// })
/// ```
///
/// # Arguments
///
/// - `cx` - The capability context (`&mut Cx`)
/// - `body` - A block containing the scope's work
///
/// # Returns
///
/// The result of the scope body.
///
/// # Example
///
/// ```ignore
/// scope!(cx, {
///     spawn!(async { work_a().await });
///     spawn!(async { work_b().await });
///     // Both tasks are awaited before scope exits
/// })
/// ```
#[proc_macro]
pub fn scope(input: TokenStream) -> TokenStream {
    scope::scope_impl(input)
}

/// Spawns a task within the current scope.
///
/// The `spawn!` macro spawns an async task that is owned by the enclosing region.
/// The task cannot orphan - it will be cancelled and drained when the region closes.
///
/// # Syntax
///
/// ```ignore
/// spawn!(async { /* work */ })
/// spawn!(async move { /* work with captured values */ })
/// ```
///
/// # Returns
///
/// A `TaskHandle` that can be awaited to get the task's result.
///
/// # Example
///
/// ```ignore
/// let handle = spawn!(async {
///     expensive_computation().await
/// });
/// let result = handle.await;
/// ```
#[proc_macro]
pub fn spawn(input: TokenStream) -> TokenStream {
    spawn::spawn_impl(input)
}

/// Joins multiple futures, waiting for all to complete.
///
/// The `join!` macro runs multiple futures concurrently and waits for all of them
/// to complete. If any future is cancelled or panics, the others continue running
/// and the final outcome reflects the most severe result.
///
/// # Syntax
///
/// ```ignore
/// join!(future1, future2, ...)
/// ```
///
/// # Returns
///
/// A tuple of all the futures' results in the order they were specified.
///
/// # Outcome Semantics
///
/// The combined outcome follows the severity lattice:
/// - If all succeed: `Outcome::Ok((r1, r2, ...))`
/// - If any fails: the most severe outcome is propagated
///
/// # Example
///
/// ```ignore
/// let (a, b, c) = join!(
///     fetch_user().await,
///     fetch_profile().await,
///     fetch_settings().await
/// );
/// ```
#[proc_macro]
pub fn join(input: TokenStream) -> TokenStream {
    join::join_impl(input)
}

/// Races multiple futures, returning the first to complete.
///
/// The `race!` macro runs multiple futures concurrently and returns when the first
/// one completes. The losing futures are automatically cancelled and drained,
/// ensuring no orphaned work.
///
/// # Syntax
///
/// ```ignore
/// race!(future1, future2, ...)
/// ```
///
/// # Returns
///
/// The result of the winning future.
///
/// # Loser Cleanup
///
/// All non-winning futures are cancelled via the cancellation protocol:
/// 1. Cancel request sent
/// 2. Futures drain to cleanup points
/// 3. Finalizers run
/// 4. Outcomes discarded
///
/// # Example
///
/// ```ignore
/// let result = race!(
///     primary_service.fetch().await,
///     backup_service.fetch().await
/// );
/// // One completed, the other was cancelled and drained
/// ```
#[proc_macro]
pub fn race(input: TokenStream) -> TokenStream {
    race::race_impl(input)
}

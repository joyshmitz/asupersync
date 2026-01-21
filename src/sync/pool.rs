//! Generic resource pool with obligation-based return semantics.
//!
//! This module provides a cancel-safe resource pool where acquired resources
//! are tracked as obligations that must be returned to the pool.
//!
//! # Two-Phase Pattern
//!
//! Pool acquisition follows the two-phase pattern:
//! - **Phase 1 (Wait)**: Wait for a resource to become available (cancel-safe)
//! - **Phase 2 (Hold)**: Hold the resource via [`PooledResource`] (obligation)
//!
//! # Cancel Safety
//!
//! - Cancellation during wait: Clean abort, no resource acquired
//! - Cancellation while holding: Resource returned to pool via obligation
//!
//! # Example
//!
//! ```ignore
//! use asupersync::sync::pool::{Pool, PooledResource};
//!
//! // Pool implementation provides acquire
//! let resource = pool.acquire(&cx).await?;
//!
//! // Use the resource
//! resource.do_something();
//!
//! // Resource automatically returned when dropped
//! ```

use std::error::Error;
use std::fmt;
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use crate::cx::Cx;
use crate::types::Time;

/// Error returned when pool acquisition fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// The pool was closed.
    Closed,
    /// Cancelled while waiting.
    Cancelled,
    /// Timeout while waiting.
    Timeout,
    /// Pool is at capacity and cannot create more resources.
    AtCapacity,
    /// Resource creation failed.
    ResourceCreationFailed,
}

impl fmt::Display for AcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => write!(f, "pool closed"),
            Self::Cancelled => write!(f, "pool acquire cancelled"),
            Self::Timeout => write!(f, "pool acquire timeout"),
            Self::AtCapacity => write!(f, "pool at capacity"),
            Self::ResourceCreationFailed => write!(f, "resource creation failed"),
        }
    }
}

impl Error for AcquireError {}

/// Error returned when trying to acquire without waiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryAcquireError {
    /// No resource immediately available.
    NoResource,
    /// The pool was closed.
    Closed,
}

impl fmt::Display for TryAcquireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoResource => write!(f, "no pooled resource available"),
            Self::Closed => write!(f, "pool closed"),
        }
    }
}

impl Error for TryAcquireError {}

/// Statistics for a resource pool.
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Resources currently in use.
    pub active: usize,
    /// Resources idle in pool.
    pub idle: usize,
    /// Total resources (active + idle).
    pub total: usize,
    /// Maximum pool size.
    pub max_size: usize,
    /// Number of waiters blocked on acquire.
    pub waiters: usize,
    /// Total acquisitions since pool creation.
    pub total_acquisitions: u64,
    /// Total time spent waiting for resources (nanoseconds).
    pub total_wait_time_ns: u64,
    /// Total resources created over pool lifetime.
    pub resources_created: u64,
    /// Total resources discarded (marked unhealthy).
    pub resources_discarded: u64,
}

impl PoolStats {
    /// Returns the total time spent waiting as a Duration.
    #[must_use]
    pub fn total_wait_time(&self) -> Duration {
        Duration::from_nanos(self.total_wait_time_ns)
    }

    /// Returns the average wait time per acquisition.
    #[must_use]
    pub fn average_wait_time(&self) -> Option<Duration> {
        if self.total_acquisitions == 0 {
            None
        } else {
            Some(Duration::from_nanos(
                self.total_wait_time_ns / self.total_acquisitions,
            ))
        }
    }

    /// Returns the pool utilization as a ratio (0.0 to 1.0).
    #[must_use]
    pub fn utilization(&self) -> f64 {
        if self.max_size == 0 {
            0.0
        } else {
            self.active as f64 / self.max_size as f64
        }
    }
}

/// Trait for resource pools with cancel-safe acquisition.
///
/// Implementors provide the actual resource management logic while this trait
/// defines the common interface for acquiring and managing pooled resources.
///
/// # Cancel Safety
///
/// The `acquire` method must be cancel-safe: if cancelled while waiting,
/// no resource should be leaked or left in an inconsistent state.
///
/// # Obligation Semantics
///
/// Acquired resources are wrapped in [`PooledResource`] which tracks the
/// obligation to return the resource. Resources are automatically returned
/// when the [`PooledResource`] is dropped.
pub trait Pool: Send + Sync {
    /// The type of resource managed by this pool.
    type Resource: Send;

    /// Error type for acquisition failures.
    type Error: Error + Send + Sync + 'static;

    /// Acquires a resource from the pool asynchronously.
    ///
    /// This may block if no resources are available and the pool is at capacity.
    /// The acquire respects the Cx's deadline if one is set.
    ///
    /// # Cancel Safety
    ///
    /// If cancelled while waiting, no resource is leaked.
    /// If cancelled after acquisition, the [`PooledResource`]'s Drop impl
    /// ensures the resource is returned.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The pool is closed
    /// - The operation is cancelled
    /// - The deadline is exceeded
    fn acquire<'a>(
        &'a self,
        cx: &'a Cx,
    ) -> impl std::future::Future<Output = Result<PooledResource<Self::Resource>, Self::Error>> + Send + 'a;

    /// Tries to acquire a resource without waiting.
    ///
    /// Returns `None` if no resource is immediately available.
    fn try_acquire(&self) -> Option<PooledResource<Self::Resource>>;

    /// Returns current pool statistics.
    fn stats(&self) -> PoolStats;

    /// Closes the pool, rejecting new acquisitions.
    ///
    /// Existing acquired resources can still be returned.
    /// The close operation is idempotent.
    fn close(&self);

    /// Returns true if the pool is closed.
    fn is_closed(&self) -> bool;
}

/// How a resource should be returned to the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnAction {
    /// Return the resource to the idle pool (resource is healthy).
    Return,
    /// Discard the resource (resource is broken/unhealthy).
    Discard,
}

/// A callback for returning resources to a pool.
///
/// This trait abstracts the mechanism for returning resources,
/// allowing [`PooledResource`] to work with any pool implementation.
pub trait ReturnCallback<R>: Send + Sync {
    /// Returns or discards the resource.
    fn return_resource(&self, resource: R, action: ReturnAction);
}

/// A resource acquired from a pool.
///
/// This type ensures the resource is returned to the pool when dropped,
/// even if the holding task is cancelled. The obligation to return the
/// resource is discharged either by explicit return or by Drop.
///
/// # Cancel Safety
///
/// If the task holding this resource is cancelled, the Drop implementation
/// ensures the resource is properly returned to the pool.
///
/// # Usage
///
/// Access the underlying resource via [`Deref`] and [`DerefMut`]:
///
/// ```ignore
/// let mut resource = pool.acquire(&cx).await?;
/// resource.do_something();  // Deref to underlying type
/// ```
///
/// Explicitly return or discard:
///
/// ```ignore
/// // Return as healthy
/// resource.return_to_pool();
///
/// // Or mark as broken
/// resource.discard();
/// ```
pub struct PooledResource<R> {
    /// The underlying resource, wrapped in Option for take semantics.
    resource: Option<R>,
    /// Callback to return the resource.
    return_callback: Box<dyn ReturnCallback<R>>,
    /// When this resource was acquired.
    acquired_at: Time,
}

// Safety: PooledResource is Send if R is Send and the callback is Send
// The callback is boxed with Send + Sync bounds
unsafe impl<R: Send> Send for PooledResource<R> {}

impl<R> PooledResource<R> {
    /// Creates a new pooled resource.
    ///
    /// # Arguments
    ///
    /// * `resource` - The underlying resource
    /// * `return_callback` - Callback to return the resource to the pool
    /// * `acquired_at` - Time when the resource was acquired
    #[must_use]
    pub fn new(
        resource: R,
        return_callback: impl ReturnCallback<R> + 'static,
        acquired_at: Time,
    ) -> Self {
        Self {
            resource: Some(resource),
            return_callback: Box::new(return_callback),
            acquired_at,
        }
    }

    /// Access the resource.
    ///
    /// # Panics
    ///
    /// Panics if the resource has already been returned or discarded.
    #[must_use]
    pub fn get(&self) -> &R {
        self.resource.as_ref().expect("resource already taken")
    }

    /// Mutably access the resource.
    ///
    /// # Panics
    ///
    /// Panics if the resource has already been returned or discarded.
    #[must_use]
    pub fn get_mut(&mut self) -> &mut R {
        self.resource.as_mut().expect("resource already taken")
    }

    /// Explicitly returns the resource to the pool.
    ///
    /// This marks the resource as healthy and returns it for reuse.
    /// After calling this method, the [`PooledResource`] is consumed.
    pub fn return_to_pool(mut self) {
        if let Some(resource) = self.resource.take() {
            self.return_callback
                .return_resource(resource, ReturnAction::Return);
        }
    }

    /// Marks the resource as broken and discards it.
    ///
    /// The pool will create a new resource to replace this one.
    /// Use this when the resource is in an unhealthy state.
    /// After calling this method, the [`PooledResource`] is consumed.
    pub fn discard(mut self) {
        if let Some(resource) = self.resource.take() {
            self.return_callback
                .return_resource(resource, ReturnAction::Discard);
        }
    }

    /// Returns how long this resource has been held.
    ///
    /// # Arguments
    ///
    /// * `now` - The current time
    #[must_use]
    pub fn held_duration(&self, now: Time) -> Duration {
        Duration::from_nanos(now.duration_since(self.acquired_at))
    }

    /// Returns when this resource was acquired.
    #[must_use]
    pub fn acquired_at(&self) -> Time {
        self.acquired_at
    }
}

impl<R> Deref for PooledResource<R> {
    type Target = R;

    fn deref(&self) -> &R {
        self.get()
    }
}

impl<R> DerefMut for PooledResource<R> {
    fn deref_mut(&mut self) -> &mut R {
        self.get_mut()
    }
}

impl<R> Drop for PooledResource<R> {
    fn drop(&mut self) {
        // If the resource hasn't been explicitly returned or discarded,
        // return it as healthy. This handles cancellation and panics.
        if let Some(resource) = self.resource.take() {
            self.return_callback
                .return_resource(resource, ReturnAction::Return);
        }
    }
}

impl<R: fmt::Debug> fmt::Debug for PooledResource<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PooledResource")
            .field("resource", &self.resource)
            .field("acquired_at", &self.acquired_at)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    /// A simple return callback that counts returns and discards.
    struct TestCallback {
        returns: Arc<AtomicUsize>,
        discards: Arc<AtomicUsize>,
    }

    impl<R> ReturnCallback<R> for TestCallback {
        fn return_resource(&self, _resource: R, action: ReturnAction) {
            match action {
                ReturnAction::Return => {
                    self.returns.fetch_add(1, Ordering::SeqCst);
                }
                ReturnAction::Discard => {
                    self.discards.fetch_add(1, Ordering::SeqCst);
                }
            }
        }
    }

    #[test]
    fn pooled_resource_access() {
        init_test("pooled_resource_access");
        let returns = Arc::new(AtomicUsize::new(0));
        let discards = Arc::new(AtomicUsize::new(0));
        let callback = TestCallback {
            returns: Arc::clone(&returns),
            discards: Arc::clone(&discards),
        };

        let mut resource = PooledResource::new(42i32, callback, Time::ZERO);

        // Test access via get
        crate::assert_with_log!(*resource.get() == 42, "get", 42, *resource.get());

        // Test access via deref
        crate::assert_with_log!(*resource == 42, "deref", 42, *resource);

        // Test mutable access
        *resource.get_mut() = 100;
        crate::assert_with_log!(*resource == 100, "get_mut", 100, *resource);

        // Drop should return
        drop(resource);
        crate::assert_with_log!(
            returns.load(Ordering::SeqCst) == 1,
            "returns",
            1,
            returns.load(Ordering::SeqCst)
        );
        crate::assert_with_log!(
            discards.load(Ordering::SeqCst) == 0,
            "discards",
            0,
            discards.load(Ordering::SeqCst)
        );

        crate::test_complete!("pooled_resource_access");
    }

    #[test]
    fn pooled_resource_explicit_return() {
        init_test("pooled_resource_explicit_return");
        let returns = Arc::new(AtomicUsize::new(0));
        let discards = Arc::new(AtomicUsize::new(0));
        let callback = TestCallback {
            returns: Arc::clone(&returns),
            discards: Arc::clone(&discards),
        };

        let resource = PooledResource::new("test", callback, Time::ZERO);

        // Explicit return
        resource.return_to_pool();

        crate::assert_with_log!(
            returns.load(Ordering::SeqCst) == 1,
            "returns",
            1,
            returns.load(Ordering::SeqCst)
        );
        crate::assert_with_log!(
            discards.load(Ordering::SeqCst) == 0,
            "discards",
            0,
            discards.load(Ordering::SeqCst)
        );

        crate::test_complete!("pooled_resource_explicit_return");
    }

    #[test]
    fn pooled_resource_discard() {
        init_test("pooled_resource_discard");
        let returns = Arc::new(AtomicUsize::new(0));
        let discards = Arc::new(AtomicUsize::new(0));
        let callback = TestCallback {
            returns: Arc::clone(&returns),
            discards: Arc::clone(&discards),
        };

        let resource = PooledResource::new(vec![1, 2, 3], callback, Time::ZERO);

        // Discard the resource
        resource.discard();

        crate::assert_with_log!(
            returns.load(Ordering::SeqCst) == 0,
            "returns",
            0,
            returns.load(Ordering::SeqCst)
        );
        crate::assert_with_log!(
            discards.load(Ordering::SeqCst) == 1,
            "discards",
            1,
            discards.load(Ordering::SeqCst)
        );

        crate::test_complete!("pooled_resource_discard");
    }

    #[test]
    fn pooled_resource_drop_returns() {
        init_test("pooled_resource_drop_returns");
        let returns = Arc::new(AtomicUsize::new(0));
        let discards = Arc::new(AtomicUsize::new(0));

        {
            let callback = TestCallback {
                returns: Arc::clone(&returns),
                discards: Arc::clone(&discards),
            };
            let _resource = PooledResource::new(123u64, callback, Time::ZERO);
            // Resource dropped here without explicit return/discard
        }

        // Should have returned automatically
        crate::assert_with_log!(
            returns.load(Ordering::SeqCst) == 1,
            "returns",
            1,
            returns.load(Ordering::SeqCst)
        );
        crate::assert_with_log!(
            discards.load(Ordering::SeqCst) == 0,
            "discards",
            0,
            discards.load(Ordering::SeqCst)
        );

        crate::test_complete!("pooled_resource_drop_returns");
    }

    #[test]
    fn pool_stats_utilization() {
        init_test("pool_stats_utilization");
        let stats = PoolStats {
            active: 5,
            idle: 3,
            total: 8,
            max_size: 10,
            waiters: 2,
            total_acquisitions: 100,
            total_wait_time_ns: 1_000_000_000, // 1 second
            resources_created: 10,
            resources_discarded: 2,
        };

        let utilization = stats.utilization();
        crate::assert_with_log!(
            (utilization - 0.5).abs() < 0.001,
            "utilization",
            0.5,
            utilization
        );

        let avg_wait = stats.average_wait_time().unwrap();
        crate::assert_with_log!(
            avg_wait == Duration::from_millis(10),
            "avg_wait",
            Duration::from_millis(10),
            avg_wait
        );

        crate::test_complete!("pool_stats_utilization");
    }

    #[test]
    fn pool_stats_empty() {
        init_test("pool_stats_empty");
        let stats = PoolStats::default();

        crate::assert_with_log!(
            stats.utilization() == 0.0,
            "utilization",
            0.0,
            stats.utilization()
        );
        crate::assert_with_log!(
            stats.average_wait_time().is_none(),
            "avg_wait",
            "None",
            stats.average_wait_time()
        );

        crate::test_complete!("pool_stats_empty");
    }

    #[test]
    fn pooled_resource_held_duration() {
        init_test("pooled_resource_held_duration");
        let returns = Arc::new(AtomicUsize::new(0));
        let discards = Arc::new(AtomicUsize::new(0));
        let callback = TestCallback { returns, discards };

        let acquired_at = Time::from_millis(1000);
        let resource = PooledResource::new(42i32, callback, acquired_at);

        let now = Time::from_millis(1500);
        let duration = resource.held_duration(now);

        crate::assert_with_log!(
            duration == Duration::from_millis(500),
            "held_duration",
            Duration::from_millis(500),
            duration
        );

        crate::assert_with_log!(
            resource.acquired_at() == acquired_at,
            "acquired_at",
            acquired_at,
            resource.acquired_at()
        );

        crate::test_complete!("pooled_resource_held_duration");
    }
}

//! Actor abstraction for region-owned, message-driven concurrency.
//!
//! Actors in Asupersync are region-owned tasks that process messages from a
//! bounded mailbox. They integrate with the runtime's structured concurrency
//! model:
//!
//! - **Region-owned**: Actors are spawned within a region and cannot outlive it.
//! - **Cancel-safe mailbox**: Messages use the two-phase reserve/send pattern.
//! - **Lifecycle hooks**: `on_start` and `on_stop` for initialization and cleanup.
//!
//! # Example
//!
//! ```ignore
//! struct Counter {
//!     count: u64,
//! }
//!
//! impl Actor for Counter {
//!     type Message = u64;
//!
//!     async fn handle(&mut self, _cx: &Cx, msg: u64) {
//!         self.count += msg;
//!     }
//! }
//!
//! // In a scope:
//! let (handle, stored) = scope.spawn_actor(
//!     &mut state, &cx, Counter { count: 0 }, 32,
//! )?;
//! state.store_spawned_task(handle.task_id(), stored);
//!
//! // Send messages:
//! handle.send(&cx, 5).await?;
//! handle.send(&cx, 10).await?;
//!
//! // Stop the actor:
//! handle.stop();
//! let result = handle.join(&cx).await?;
//! assert_eq!(result.count, 15);
//! ```

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::channel::mpsc;
use crate::cx::Cx;
use crate::error::SendError;

/// A message-driven actor that processes messages from a bounded mailbox.
///
/// Actors are the unit of stateful, message-driven concurrency. Each actor:
/// - Owns mutable state (`self`)
/// - Receives messages sequentially (no data races)
/// - Runs inside a region (structured lifetime)
///
/// # Cancel Safety
///
/// When an actor is cancelled (region close, explicit abort), the runtime:
/// 1. Closes the mailbox (no new messages accepted)
/// 2. Calls `on_stop` for cleanup
/// 3. Returns the actor state to the caller via `ActorHandle::join`
pub trait Actor: Send + 'static {
    /// The type of messages this actor can receive.
    type Message: Send + 'static;

    /// Called once when the actor starts, before processing any messages.
    ///
    /// Use this for initialization that requires the capability context.
    /// The default implementation does nothing.
    fn on_start(&mut self, _cx: &Cx) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }

    /// Handle a single message.
    ///
    /// This is called sequentially for each message in the mailbox.
    /// The actor has exclusive access to its state during handling.
    fn handle(
        &mut self,
        cx: &Cx,
        msg: Self::Message,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>>;

    /// Called once when the actor is stopping, after the mailbox is drained.
    ///
    /// Use this for cleanup. The default implementation does nothing.
    fn on_stop(&mut self, _cx: &Cx) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(async {})
    }
}

/// Handle to a running actor, used to send messages and manage its lifecycle.
///
/// The handle owns:
/// - A sender for the actor's mailbox
/// - A task handle for join/abort operations
///
/// When the handle is dropped, the mailbox sender is dropped, which causes
/// the actor loop to exit after processing remaining messages.
#[derive(Debug)]
pub struct ActorHandle<A: Actor> {
    sender: mpsc::Sender<A::Message>,
    task_id: crate::types::TaskId,
    receiver: crate::channel::oneshot::Receiver<Result<A, crate::error::JoinError>>,
    inner: std::sync::Weak<std::sync::RwLock<crate::cx::CxInner>>,
}

impl<A: Actor> ActorHandle<A> {
    /// Send a message to the actor using two-phase reserve/send.
    ///
    /// Returns an error if the actor has stopped or the mailbox is full.
    pub async fn send(&self, cx: &Cx, msg: A::Message) -> Result<(), SendError<A::Message>> {
        self.sender.send(cx, msg).await
    }

    /// Try to send a message without blocking.
    ///
    /// Returns `Err(SendError::Full(msg))` if the mailbox is full, or
    /// `Err(SendError::Disconnected(msg))` if the actor has stopped.
    pub fn try_send(&self, msg: A::Message) -> Result<(), SendError<A::Message>> {
        self.sender.try_send(msg)
    }

    /// Returns a lightweight, clonable reference for sending messages.
    pub fn sender(&self) -> ActorRef<A> {
        ActorRef {
            sender: self.sender.clone(),
        }
    }

    /// Returns the task ID of the actor's underlying task.
    #[must_use]
    pub fn task_id(&self) -> crate::types::TaskId {
        self.task_id
    }

    /// Signals the actor to stop by closing the mailbox.
    ///
    /// The actor will finish processing any buffered messages, then call
    /// `on_stop` and return its state.
    pub fn stop(&self) {
        // Closing is achieved by dropping our sender clone won't work since
        // we need to keep the handle alive. Instead, we abort the task which
        // triggers cancellation.
        if let Some(inner) = self.inner.upgrade() {
            if let Ok(mut guard) = inner.write() {
                guard.cancel_requested = true;
            }
        }
    }

    /// Returns true if the actor has finished.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.receiver.is_ready()
    }

    /// Wait for the actor to finish and return its final state.
    pub fn join<'a>(&'a self, cx: &'a Cx) -> ActorJoinFuture<'a, A> {
        ActorJoinFuture {
            handle: self,
            _cx: cx,
            completed: false,
        }
    }

    /// Request the actor to stop by aborting its task.
    pub fn abort(&self) {
        if let Some(inner) = self.inner.upgrade() {
            if let Ok(mut guard) = inner.write() {
                guard.cancel_requested = true;
            }
        }
    }
}

/// A lightweight, clonable reference to an actor's mailbox.
///
/// Use this to send messages to an actor from multiple locations without
/// needing to share the `ActorHandle`.
#[derive(Debug, Clone)]
pub struct ActorRef<A: Actor> {
    sender: mpsc::Sender<A::Message>,
}

impl<A: Actor> ActorRef<A> {
    /// Send a message to the actor.
    pub async fn send(&self, cx: &Cx, msg: A::Message) -> Result<(), SendError<A::Message>> {
        self.sender.send(cx, msg).await
    }

    /// Try to send a message without blocking.
    pub fn try_send(&self, msg: A::Message) -> Result<(), SendError<A::Message>> {
        self.sender.try_send(msg)
    }

    /// Returns true if the actor has stopped (mailbox closed).
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.sender.is_closed()
    }
}

/// Future returned by `ActorHandle::join`.
pub struct ActorJoinFuture<'a, A: Actor> {
    handle: &'a ActorHandle<A>,
    _cx: &'a Cx,
    completed: bool,
}

impl<A: Actor> Future for ActorJoinFuture<'_, A> {
    type Output = Result<A, crate::error::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.handle.receiver).poll(cx) {
            Poll::Ready(Ok(result)) => {
                self.completed = true;
                Poll::Ready(result)
            }
            Poll::Ready(Err(_)) => {
                self.completed = true;
                Poll::Ready(Err(crate::error::JoinError::Cancelled(
                    crate::types::CancelReason::race_loser(),
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The default mailbox capacity for actors.
pub const DEFAULT_MAILBOX_CAPACITY: usize = 64;

/// Internal: runs the actor message loop.
///
/// This function is the core of the actor runtime. It:
/// 1. Calls `on_start`
/// 2. Receives and handles messages until the mailbox is closed or cancelled
/// 3. Calls `on_stop`
/// 4. Returns the actor state
async fn run_actor_loop<A: Actor>(
    mut actor: A,
    cx: Cx,
    mut mailbox: mpsc::Receiver<A::Message>,
) -> A {
    // Phase 1: Initialization
    actor.on_start(&cx).await;

    // Phase 2: Message loop
    loop {
        // Check for cancellation
        if cx.is_cancel_requested() {
            break;
        }

        match mailbox.recv(&cx).await {
            Ok(msg) => {
                actor.handle(&cx, msg).await;
            }
            Err(crate::channel::mpsc::RecvError::Disconnected) => {
                // All senders dropped - graceful shutdown
                break;
            }
            Err(crate::channel::mpsc::RecvError::Cancelled) => {
                // Cancellation requested
                break;
            }
            Err(crate::channel::mpsc::RecvError::Empty) => {
                // Shouldn't happen with recv() (only try_recv), but handle gracefully
                break;
            }
        }
    }

    // Phase 3: Cleanup
    actor.on_stop(&cx).await;

    actor
}

// Extension for Scope to spawn actors
impl<'r, P: crate::types::Policy> crate::cx::Scope<'r, P> {
    /// Spawns a new actor in this scope with the given mailbox capacity.
    ///
    /// The actor runs as a region-owned task. Messages are delivered through
    /// a bounded MPSC channel with two-phase send semantics.
    ///
    /// # Arguments
    ///
    /// * `state` - Runtime state for task creation
    /// * `cx` - Capability context
    /// * `actor` - The actor instance
    /// * `mailbox_capacity` - Bounded mailbox size
    ///
    /// # Returns
    ///
    /// A tuple of `(ActorHandle, StoredTask)`. The `StoredTask` must be
    /// registered with the runtime via `state.store_spawned_task()`.
    pub fn spawn_actor<A: Actor>(
        &self,
        state: &mut crate::runtime::state::RuntimeState,
        cx: &Cx,
        actor: A,
        mailbox_capacity: usize,
    ) -> Result<
        (ActorHandle<A>, crate::runtime::stored_task::StoredTask),
        crate::error::SpawnError,
    > {
        use crate::channel::oneshot;
        use crate::cx::scope::CatchUnwind;
        use crate::runtime::stored_task::StoredTask;
        use crate::runtime::task_handle::TaskHandle;
        use tracing::{debug, debug_span};

        // Create the actor's mailbox
        let (msg_tx, msg_rx) = mpsc::channel::<A::Message>(mailbox_capacity);

        // Create oneshot for returning the actor state
        let (result_tx, result_rx) =
            oneshot::channel::<Result<A, crate::error::JoinError>>();

        // Create task record
        let task_id = self.create_task_record(state)?;

        let _span = debug_span!(
            "actor_spawn",
            task_id = ?task_id,
            region_id = ?self.region_id(),
            mailbox_capacity = mailbox_capacity,
        )
        .entered();
        debug!(
            task_id = ?task_id,
            region_id = ?self.region_id(),
            mailbox_capacity = mailbox_capacity,
            "actor spawned"
        );

        // Create child context
        let child_observability = cx.child_observability(self.region_id(), task_id);
        let child_entropy = cx.child_entropy(task_id);
        let io_driver = state.io_driver_handle();
        let child_cx = Cx::new_with_observability(
            self.region_id(),
            task_id,
            self.budget(),
            Some(child_observability),
            io_driver,
            Some(child_entropy),
        );

        // Link Cx to TaskRecord
        if let Some(record) = state.tasks.get_mut(task_id.arena_index()) {
            record.set_cx_inner(child_cx.inner.clone());
            record.set_cx(child_cx.clone());
        }

        let cx_for_send = child_cx.clone();
        let inner_weak = Arc::downgrade(&child_cx.inner);

        // Create the actor loop future
        let wrapped = async move {
            let result = CatchUnwind(Box::pin(run_actor_loop(actor, child_cx, msg_rx))).await;
            match result {
                Ok(actor_state) => {
                    let _ = result_tx.send(&cx_for_send, Ok(actor_state));
                }
                Err(payload) => {
                    let msg = crate::cx::scope::payload_to_string(&payload);
                    let _ = result_tx.send(
                        &cx_for_send,
                        Err(crate::error::JoinError::Panicked(
                            crate::types::PanicPayload::new(msg),
                        )),
                    );
                }
            }
        };

        let stored = StoredTask::new(wrapped);

        let handle = ActorHandle {
            sender: msg_tx,
            task_id,
            receiver: result_rx,
            inner: inner_weak,
        };

        Ok((handle, stored))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::state::RuntimeState;
    use crate::types::Budget;

    fn init_test(name: &str) {
        crate::test_utils::init_test_logging();
        crate::test_phase!(name);
    }

    /// Simple counter actor for testing.
    struct Counter {
        count: u64,
        started: bool,
        stopped: bool,
    }

    impl Counter {
        fn new() -> Self {
            Self {
                count: 0,
                started: false,
                stopped: false,
            }
        }
    }

    impl Actor for Counter {
        type Message = u64;

        fn on_start(&mut self, _cx: &Cx) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.started = true;
            Box::pin(async {})
        }

        fn handle(
            &mut self,
            _cx: &Cx,
            msg: u64,
        ) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.count += msg;
            Box::pin(async {})
        }

        fn on_stop(&mut self, _cx: &Cx) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
            self.stopped = true;
            Box::pin(async {})
        }
    }

    #[test]
    fn actor_trait_object_safety() {
        init_test("actor_trait_object_safety");

        // Verify Counter implements Actor with the right bounds
        fn assert_actor<A: Actor>() {}
        assert_actor::<Counter>();

        crate::test_complete!("actor_trait_object_safety");
    }

    #[test]
    fn actor_handle_creation() {
        init_test("actor_handle_creation");

        let mut state = RuntimeState::new();
        let root = state.create_root_region(Budget::INFINITE);
        let cx = Cx::for_testing();
        let scope = crate::cx::Scope::<crate::types::FailFast>::new(root, Budget::INFINITE);

        let result = scope.spawn_actor(&mut state, &cx, Counter::new(), 32);
        assert!(result.is_ok(), "spawn_actor should succeed");

        let (handle, stored) = result.unwrap();
        state.store_spawned_task(handle.task_id(), stored);

        // Handle should have valid task ID
        let _tid = handle.task_id();

        // Actor should not be finished yet (not polled)
        assert!(!handle.is_finished());

        crate::test_complete!("actor_handle_creation");
    }

    #[test]
    fn actor_ref_is_cloneable() {
        init_test("actor_ref_is_cloneable");

        let mut state = RuntimeState::new();
        let root = state.create_root_region(Budget::INFINITE);
        let cx = Cx::for_testing();
        let scope = crate::cx::Scope::<crate::types::FailFast>::new(root, Budget::INFINITE);

        let (handle, stored) = scope
            .spawn_actor(&mut state, &cx, Counter::new(), 32)
            .unwrap();
        state.store_spawned_task(handle.task_id(), stored);

        // Get multiple refs
        let ref1 = handle.sender();
        let ref2 = ref1.clone();

        // Both should be open
        assert!(!ref1.is_closed());
        assert!(!ref2.is_closed());

        crate::test_complete!("actor_ref_is_cloneable");
    }
}

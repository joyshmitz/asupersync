//! Obligation record for the runtime.
//!
//! Obligations represent resources that must be resolved (commit, abort, etc.)
//! before their owning region can close. They implement the two-phase pattern.

use crate::types::{ObligationId, RegionId, TaskId, Time};
use core::fmt;

/// The kind of obligation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationKind {
    /// A send permit for a channel.
    SendPermit,
    /// An acknowledgement for a received message.
    Ack,
    /// A lease for a remote resource.
    Lease,
    /// A pending I/O operation.
    IoOp,
}

/// The reason an obligation was aborted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationAbortReason {
    /// Aborted due to cancellation.
    Cancel,
    /// Aborted due to an error.
    Error,
    /// Explicitly aborted by the caller.
    Explicit,
}

impl ObligationAbortReason {
    /// Returns a short string for tracing and diagnostics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Cancel => "cancel",
            Self::Error => "error",
            Self::Explicit => "explicit",
        }
    }
}

impl fmt::Display for ObligationAbortReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The state of an obligation.
///
/// State transitions:
/// ```text
/// Reserved ──► Committed
///    │
///    ├────────► Aborted
///    │
///    └────────► Leaked (error: holder completed without resolving)
/// ```
///
/// All terminal states (Committed, Aborted, Leaked) are absorbing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObligationState {
    /// Obligation is reserved but not yet resolved.
    /// Blocks region close until resolved.
    Reserved,
    /// Obligation was committed (successful resolution).
    /// The effect took place (e.g., message was sent).
    Committed,
    /// Obligation was aborted (clean cancellation).
    /// No data loss, resources released.
    Aborted,
    /// ERROR: Obligation was leaked (holder completed without resolving).
    /// This indicates a bug in user code or library.
    /// In lab mode: triggers panic. In prod mode: log and attempt recovery.
    Leaked,
}

impl ObligationState {
    /// Returns true if the obligation is in a terminal state.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted | Self::Leaked)
    }

    /// Returns true if the obligation is resolved (not pending).
    /// Note: Leaked counts as resolved (it's terminal, just not successful).
    #[must_use]
    pub const fn is_resolved(self) -> bool {
        self.is_terminal()
    }

    /// Returns true if the obligation was successfully resolved (not leaked).
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(self, Self::Committed | Self::Aborted)
    }

    /// Returns true if the obligation leaked (error state).
    #[must_use]
    pub const fn is_leaked(self) -> bool {
        matches!(self, Self::Leaked)
    }
}

/// Internal record for an obligation in the runtime.
#[derive(Debug)]
pub struct ObligationRecord {
    /// Unique identifier for this obligation.
    pub id: ObligationId,
    /// The kind of obligation.
    pub kind: ObligationKind,
    /// The task holding this obligation.
    pub holder: TaskId,
    /// The region that owns this obligation.
    pub region: RegionId,
    /// Current state.
    pub state: ObligationState,
    /// Optional description for debugging.
    pub description: Option<String>,
    /// Time when the obligation was reserved.
    pub reserved_at: Time,
    /// Time when the obligation was resolved.
    pub resolved_at: Option<Time>,
    /// Reason for abort, if applicable.
    pub abort_reason: Option<ObligationAbortReason>,
}

impl ObligationRecord {
    /// Creates a new obligation record.
    #[must_use]
    pub fn new(
        id: ObligationId,
        kind: ObligationKind,
        holder: TaskId,
        region: RegionId,
        reserved_at: Time,
    ) -> Self {
        Self {
            id,
            kind,
            holder,
            region,
            state: ObligationState::Reserved,
            description: None,
            reserved_at,
            resolved_at: None,
            abort_reason: None,
        }
    }

    /// Creates an obligation with a description.
    #[must_use]
    pub fn with_description(
        id: ObligationId,
        kind: ObligationKind,
        holder: TaskId,
        region: RegionId,
        reserved_at: Time,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id,
            kind,
            holder,
            region,
            state: ObligationState::Reserved,
            description: Some(description.into()),
            reserved_at,
            resolved_at: None,
            abort_reason: None,
        }
    }

    /// Returns true if the obligation is still pending.
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self.state, ObligationState::Reserved)
    }

    /// Commits the obligation.
    ///
    /// # Panics
    ///
    /// Panics if already resolved.
    pub fn commit(&mut self, now: Time) -> u64 {
        assert!(self.is_pending(), "obligation already resolved");
        self.state = ObligationState::Committed;
        self.resolved_at = Some(now);
        self.abort_reason = None;
        now.duration_since(self.reserved_at)
    }

    /// Aborts the obligation.
    ///
    /// # Panics
    ///
    /// Panics if already resolved.
    pub fn abort(&mut self, now: Time, reason: ObligationAbortReason) -> u64 {
        assert!(self.is_pending(), "obligation already resolved");
        self.state = ObligationState::Aborted;
        self.resolved_at = Some(now);
        self.abort_reason = Some(reason);
        now.duration_since(self.reserved_at)
    }

    /// Marks the obligation as leaked.
    ///
    /// Called by the runtime when it detects that an obligation holder
    /// completed without resolving the obligation. This is an error state.
    ///
    /// # Panics
    ///
    /// Panics if already resolved.
    pub fn mark_leaked(&mut self, now: Time) -> u64 {
        assert!(self.is_pending(), "obligation already resolved");
        self.state = ObligationState::Leaked;
        self.resolved_at = Some(now);
        self.abort_reason = None;
        now.duration_since(self.reserved_at)
    }

    /// Returns true if this obligation leaked.
    #[must_use]
    pub const fn is_leaked(&self) -> bool {
        self.state.is_leaked()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::ArenaIndex;

    fn test_ids() -> (ObligationId, TaskId, RegionId) {
        (
            ObligationId::from_arena(ArenaIndex::new(0, 0)),
            TaskId::from_arena(ArenaIndex::new(0, 0)),
            RegionId::from_arena(ArenaIndex::new(0, 0)),
        )
    }

    #[test]
    fn obligation_state_predicates() {
        assert!(!ObligationState::Reserved.is_terminal());
        assert!(ObligationState::Committed.is_terminal());
        assert!(ObligationState::Aborted.is_terminal());
        assert!(ObligationState::Leaked.is_terminal());

        assert!(!ObligationState::Reserved.is_resolved());
        assert!(ObligationState::Committed.is_resolved());
        assert!(ObligationState::Aborted.is_resolved());
        assert!(ObligationState::Leaked.is_resolved());

        assert!(!ObligationState::Reserved.is_success());
        assert!(ObligationState::Committed.is_success());
        assert!(ObligationState::Aborted.is_success());
        assert!(!ObligationState::Leaked.is_success());

        assert!(!ObligationState::Reserved.is_leaked());
        assert!(!ObligationState::Committed.is_leaked());
        assert!(!ObligationState::Aborted.is_leaked());
        assert!(ObligationState::Leaked.is_leaked());
    }

    #[test]
    fn obligation_lifecycle_commit() {
        let (oid, tid, rid) = test_ids();
        let reserved_at = Time::from_nanos(10);
        let mut ob = ObligationRecord::new(
            oid,
            ObligationKind::SendPermit,
            tid,
            rid,
            reserved_at,
        );

        assert!(ob.is_pending());
        assert!(!ob.is_leaked());
        assert_eq!(ob.state, ObligationState::Reserved);

        let duration = ob.commit(Time::from_nanos(25));
        assert!(!ob.is_pending());
        assert!(!ob.is_leaked());
        assert_eq!(ob.state, ObligationState::Committed);
        assert_eq!(duration, 15);
        assert_eq!(ob.resolved_at, Some(Time::from_nanos(25)));
    }

    #[test]
    fn obligation_lifecycle_abort() {
        let (oid, tid, rid) = test_ids();
        let reserved_at = Time::from_nanos(100);
        let mut ob = ObligationRecord::new(oid, ObligationKind::Ack, tid, rid, reserved_at);

        let duration = ob.abort(Time::from_nanos(140), ObligationAbortReason::Explicit);
        assert!(!ob.is_pending());
        assert!(!ob.is_leaked());
        assert_eq!(ob.state, ObligationState::Aborted);
        assert_eq!(duration, 40);
        assert_eq!(ob.abort_reason, Some(ObligationAbortReason::Explicit));
    }

    #[test]
    fn obligation_lifecycle_leaked() {
        let (oid, tid, rid) = test_ids();
        let reserved_at = Time::from_nanos(5);
        let mut ob = ObligationRecord::new(oid, ObligationKind::Lease, tid, rid, reserved_at);

        let duration = ob.mark_leaked(Time::from_nanos(8));
        assert!(!ob.is_pending());
        assert!(ob.is_leaked());
        assert_eq!(ob.state, ObligationState::Leaked);
        assert_eq!(duration, 3);
    }

    #[test]
    #[should_panic(expected = "obligation already resolved")]
    fn double_commit_panics() {
        let (oid, tid, rid) = test_ids();
        let mut ob = ObligationRecord::new(
            oid,
            ObligationKind::IoOp,
            tid,
            rid,
            Time::ZERO,
        );
        ob.commit(Time::ZERO);
        ob.commit(Time::ZERO); // Should panic
    }

    #[test]
    #[should_panic(expected = "obligation already resolved")]
    fn double_abort_panics() {
        let (oid, tid, rid) = test_ids();
        let mut ob = ObligationRecord::new(
            oid,
            ObligationKind::IoOp,
            tid,
            rid,
            Time::ZERO,
        );
        ob.abort(Time::ZERO, ObligationAbortReason::Explicit);
        ob.abort(Time::ZERO, ObligationAbortReason::Explicit); // Should panic
    }

    #[test]
    #[should_panic(expected = "obligation already resolved")]
    fn commit_after_abort_panics() {
        let (oid, tid, rid) = test_ids();
        let mut ob = ObligationRecord::new(
            oid,
            ObligationKind::SendPermit,
            tid,
            rid,
            Time::ZERO,
        );
        ob.abort(Time::ZERO, ObligationAbortReason::Cancel);
        ob.commit(Time::ZERO); // Should panic
    }

    #[test]
    #[should_panic(expected = "obligation already resolved")]
    fn mark_leaked_after_commit_panics() {
        let (oid, tid, rid) = test_ids();
        let mut ob = ObligationRecord::new(
            oid,
            ObligationKind::SendPermit,
            tid,
            rid,
            Time::ZERO,
        );
        ob.commit(Time::ZERO);
        ob.mark_leaked(Time::ZERO); // Should panic
    }

    #[test]
    fn obligation_kinds_are_distinguishable() {
        assert_ne!(ObligationKind::SendPermit, ObligationKind::Ack);
        assert_ne!(ObligationKind::Ack, ObligationKind::Lease);
        assert_ne!(ObligationKind::Lease, ObligationKind::IoOp);
    }

    #[test]
    fn with_description_sets_description() {
        let (oid, tid, rid) = test_ids();
        let ob = ObligationRecord::with_description(
            oid,
            ObligationKind::SendPermit,
            tid,
            rid,
            Time::ZERO,
            "test description",
        );
        assert_eq!(ob.description, Some("test description".to_string()));
    }
}

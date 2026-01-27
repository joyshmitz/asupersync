//! Plan DAG IR for concurrency combinators.
//!
//! This module defines a minimal DAG representation for join/race/timeout
//! structures. It is intentionally lightweight and uses safe Rust only.

use std::collections::HashSet;
use std::time::Duration;

/// Node identifier for a plan DAG.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PlanId(usize);

impl PlanId {
    /// Creates a new plan id from a raw index.
    #[must_use]
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    /// Returns the underlying index.
    #[must_use]
    pub const fn index(self) -> usize {
        self.0
    }
}

/// Plan node describing a combinator and its dependencies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanNode {
    /// Leaf computation (e.g., an opaque task).
    Leaf {
        /// Human-readable label for debugging.
        label: String,
    },
    /// Join of all children.
    Join {
        /// Child nodes that must all complete.
        children: Vec<PlanId>,
    },
    /// Race of children.
    Race {
        /// Child nodes that race for first completion.
        children: Vec<PlanId>,
    },
    /// Timeout applied to a child computation.
    Timeout {
        /// Child node being timed.
        child: PlanId,
        /// Timeout duration.
        duration: Duration,
    },
}

impl PlanNode {
    fn children(&self) -> Box<dyn Iterator<Item = PlanId> + '_> {
        match self {
            Self::Leaf { .. } => Box::new(std::iter::empty()),
            Self::Join { children } | Self::Race { children } => Box::new(children.iter().copied()),
            Self::Timeout { child, .. } => Box::new(std::iter::once(*child)),
        }
    }
}

/// Errors returned when validating a plan DAG.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanError {
    /// A node referenced a missing child id.
    MissingNode {
        /// Parent node id.
        parent: PlanId,
        /// Missing child id.
        child: PlanId,
    },
    /// A join/race node had no children.
    EmptyChildren {
        /// Node with empty child list.
        parent: PlanId,
    },
    /// A cycle was detected in the graph.
    Cycle {
        /// Node where cycle detection occurred.
        at: PlanId,
    },
}

/// Plan DAG builder and container.
#[derive(Debug, Default)]
pub struct PlanDag {
    nodes: Vec<PlanNode>,
    root: Option<PlanId>,
}

impl PlanDag {
    /// Creates an empty plan DAG.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a leaf node and returns its id.
    pub fn leaf(&mut self, label: impl Into<String>) -> PlanId {
        let id = PlanId::new(self.nodes.len());
        self.nodes.push(PlanNode::Leaf {
            label: label.into(),
        });
        id
    }

    /// Adds a join node and returns its id.
    pub fn join(&mut self, children: Vec<PlanId>) -> PlanId {
        let id = PlanId::new(self.nodes.len());
        self.nodes.push(PlanNode::Join { children });
        id
    }

    /// Adds a race node and returns its id.
    pub fn race(&mut self, children: Vec<PlanId>) -> PlanId {
        let id = PlanId::new(self.nodes.len());
        self.nodes.push(PlanNode::Race { children });
        id
    }

    /// Adds a timeout node and returns its id.
    pub fn timeout(&mut self, child: PlanId, duration: Duration) -> PlanId {
        let id = PlanId::new(self.nodes.len());
        self.nodes.push(PlanNode::Timeout { child, duration });
        id
    }

    /// Sets the root node for this plan.
    pub fn set_root(&mut self, root: PlanId) {
        self.root = Some(root);
    }

    /// Returns the root node, if set.
    #[must_use]
    pub const fn root(&self) -> Option<PlanId> {
        self.root
    }

    /// Returns a reference to a node by id.
    #[must_use]
    pub fn node(&self, id: PlanId) -> Option<&PlanNode> {
        self.nodes.get(id.index())
    }

    /// Validates the DAG for structural correctness.
    pub fn validate(&self) -> Result<(), PlanError> {
        let Some(root) = self.root else {
            return Ok(());
        };

        let mut visiting = HashSet::new();
        let mut visited = HashSet::new();
        self.validate_from(root, &mut visiting, &mut visited)?;
        Ok(())
    }

    fn validate_from(
        &self,
        id: PlanId,
        visiting: &mut HashSet<PlanId>,
        visited: &mut HashSet<PlanId>,
    ) -> Result<(), PlanError> {
        if visited.contains(&id) {
            return Ok(());
        }
        if !visiting.insert(id) {
            return Err(PlanError::Cycle { at: id });
        }

        let node = self.node(id).ok_or(PlanError::MissingNode {
            parent: id,
            child: id,
        })?;

        match node {
            PlanNode::Join { children } | PlanNode::Race { children } => {
                if children.is_empty() {
                    return Err(PlanError::EmptyChildren { parent: id });
                }
            }
            PlanNode::Leaf { .. } | PlanNode::Timeout { .. } => {}
        }

        for child in node.children() {
            if self.node(child).is_none() {
                return Err(PlanError::MissingNode { parent: id, child });
            }
            self.validate_from(child, visiting, visited)?;
        }

        visiting.remove(&id);
        visited.insert(id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::init_test_logging;

    fn init_test(name: &str) {
        init_test_logging();
        crate::test_phase!(name);
    }

    #[test]
    fn build_join_race_timeout_plan() {
        init_test("build_join_race_timeout_plan");
        let mut dag = PlanDag::new();
        let a = dag.leaf("a");
        let b = dag.leaf("b");
        let join = dag.join(vec![a, b]);
        let raced = dag.race(vec![join]);
        let timed = dag.timeout(raced, Duration::from_secs(1));
        dag.set_root(timed);

        assert!(dag.validate().is_ok());
        crate::test_complete!("build_join_race_timeout_plan");
    }

    #[test]
    fn invalid_missing_child_is_reported() {
        init_test("invalid_missing_child_is_reported");
        let mut dag = PlanDag::new();
        let bad = PlanId::new(99);
        let join = dag.join(vec![bad]);
        dag.set_root(join);
        let err = dag.validate().expect_err("missing child should fail");
        assert_eq!(
            err,
            PlanError::MissingNode {
                parent: join,
                child: bad
            }
        );
        crate::test_complete!("invalid_missing_child_is_reported");
    }

    #[test]
    fn empty_children_is_reported() {
        init_test("empty_children_is_reported");
        let mut dag = PlanDag::new();
        let join = dag.join(Vec::new());
        dag.set_root(join);
        let err = dag.validate().expect_err("empty children should fail");
        assert_eq!(err, PlanError::EmptyChildren { parent: join });
        crate::test_complete!("empty_children_is_reported");
    }

    #[test]
    fn cycle_is_reported() {
        init_test("cycle_is_reported");
        let mut dag = PlanDag::new();
        let a = dag.leaf("a");
        let timeout = dag.timeout(a, Duration::from_millis(5));
        dag.nodes[a.index()] = PlanNode::Timeout {
            child: timeout,
            duration: Duration::from_millis(1),
        };
        dag.set_root(timeout);

        let err = dag.validate().expect_err("cycle should fail");
        assert_eq!(err, PlanError::Cycle { at: timeout });
        crate::test_complete!("cycle_is_reported");
    }
}

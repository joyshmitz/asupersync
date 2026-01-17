//! Buffer traits for reading and writing bytes.
//!
//! This module provides the [`Buf`] and [`BufMut`] traits which define
//! abstract interfaces for reading from and writing to byte buffers.
//!
//! # Overview
//!
//! - [`Buf`]: Trait for reading bytes from a buffer (cursor-like interface)
//! - [`BufMut`]: Trait for writing bytes to a buffer
//!
//! These traits enable generic codec implementations, zero-copy buffer
//! chaining, and efficient protocol parsing.

mod buf_trait;
mod buf_mut_trait;
mod chain;
mod take;
mod limit;

pub use buf_trait::Buf;
pub use buf_mut_trait::BufMut;
pub use chain::Chain;
pub use take::Take;
pub use limit::Limit;

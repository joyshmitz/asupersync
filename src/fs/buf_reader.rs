//! Buffered async reader for filesystem I/O.
//!
//! This is a thin wrapper around the core `io::BufReader` to provide
//! a convenient `fs`-scoped type for file operations.

pub use crate::io::BufReader;

//! Buffered async writer for filesystem I/O.
//!
//! This is a thin wrapper around the core `io::BufWriter` to provide
//! a convenient `fs`-scoped type for file operations.

pub use crate::io::BufWriter;

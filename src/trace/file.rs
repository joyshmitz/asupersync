//! Trace file format for persisting and loading replay traces.
//!
//! This module provides a binary file format for saving traces to disk and
//! loading them for replay. The format is designed for:
//!
//! - **Compactness**: Uses MessagePack for efficient binary encoding
//! - **Versioning**: Format version in header for forward compatibility
//! - **Streaming**: Events can be read incrementally without loading all into memory
//!
//! # File Format
//!
//! ```text
//! +-------------------+
//! | Magic (11 bytes)  |  "ASUPERTRACE"
//! +-------------------+
//! | Version (2 bytes) |  u16 little-endian
//! +-------------------+
//! | Flags (2 bytes)   |  u16 little-endian (reserved for compression, etc.)
//! +-------------------+
//! | Meta len (4 bytes)|  u32 little-endian
//! +-------------------+
//! | Metadata (msgpack)|  TraceMetadata
//! +-------------------+
//! | Event count (8 b) |  u64 little-endian
//! +-------------------+
//! | Events (msgpack)  |  [ReplayEvent] length-prefixed
//! +-------------------+
//! ```
//!
//! # Example
//!
//! ```ignore
//! use asupersync::trace::file::{TraceWriter, TraceReader};
//! use asupersync::trace::replay::{ReplayEvent, TraceMetadata};
//!
//! // Writing a trace
//! let mut writer = TraceWriter::create("trace.bin")?;
//! writer.write_metadata(&TraceMetadata::new(42))?;
//! writer.write_event(&ReplayEvent::RngSeed { seed: 42 })?;
//! writer.finish()?;
//!
//! // Reading a trace
//! let reader = TraceReader::open("trace.bin")?;
//! println!("Seed: {}", reader.metadata().seed);
//! for event in reader.events() {
//!     let event = event?;
//!     println!("{:?}", event);
//! }
//! ```

use super::replay::{ReplayEvent, TraceMetadata, REPLAY_SCHEMA_VERSION};
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

// =============================================================================
// Constants
// =============================================================================

/// Magic bytes at the start of every trace file.
pub const TRACE_MAGIC: &[u8; 11] = b"ASUPERTRACE";

/// Current file format version.
pub const TRACE_FILE_VERSION: u16 = 1;

/// Flag: Events are LZ4 compressed (not yet implemented).
pub const FLAG_COMPRESSED: u16 = 0x0001;

/// Header size (magic + version + flags + meta_len).
pub const HEADER_SIZE: usize = 11 + 2 + 2 + 4;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur when working with trace files.
#[derive(Debug, thiserror::Error)]
pub enum TraceFileError {
    /// I/O error during file operations.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Invalid magic bytes in file header.
    #[error("invalid magic bytes: not a trace file")]
    InvalidMagic,

    /// Unsupported file format version.
    #[error("unsupported file version: expected <= {expected}, found {found}")]
    UnsupportedVersion {
        /// Maximum supported version.
        expected: u16,
        /// Found version.
        found: u16,
    },

    /// Unsupported flags in header.
    #[error("unsupported flags: {0:#06x}")]
    UnsupportedFlags(u16),

    /// Error serializing data.
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Error deserializing data.
    #[error("deserialization error: {0}")]
    Deserialize(String),

    /// Metadata mismatch (schema version).
    #[error("schema version mismatch: expected {expected}, found {found}")]
    SchemaMismatch {
        /// Expected schema version.
        expected: u32,
        /// Found schema version.
        found: u32,
    },

    /// Writer already finished.
    #[error("writer already finished")]
    AlreadyFinished,

    /// File is truncated or corrupt.
    #[error("file truncated or corrupt")]
    Truncated,
}

impl From<rmp_serde::encode::Error> for TraceFileError {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::Serialize(e.to_string())
    }
}

impl From<rmp_serde::decode::Error> for TraceFileError {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::Deserialize(e.to_string())
    }
}

/// Result type for trace file operations.
pub type TraceFileResult<T> = Result<T, TraceFileError>;

// =============================================================================
// TraceWriter
// =============================================================================

/// Writer for streaming trace events to a file.
///
/// Events are written incrementally, allowing large traces to be written
/// without holding all events in memory.
///
/// # Example
///
/// ```ignore
/// let mut writer = TraceWriter::create("trace.bin")?;
/// writer.write_metadata(&TraceMetadata::new(42))?;
/// for event in events {
///     writer.write_event(&event)?;
/// }
/// writer.finish()?;
/// ```
pub struct TraceWriter {
    writer: BufWriter<File>,
    event_count: u64,
    event_count_pos: u64,
    finished: bool,
}

impl TraceWriter {
    /// Creates a new trace file for writing.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be created.
    pub fn create(path: impl AsRef<Path>) -> TraceFileResult<Self> {
        let file = File::create(path)?;
        let writer = BufWriter::new(file);

        Ok(Self {
            writer,
            event_count: 0,
            event_count_pos: 0,
            finished: false,
        })
    }

    /// Writes the trace metadata (must be called first).
    ///
    /// This writes the file header including magic bytes, version,
    /// flags, and the serialized metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if writing fails or the writer was already finished.
    pub fn write_metadata(&mut self, metadata: &TraceMetadata) -> TraceFileResult<()> {
        if self.finished {
            return Err(TraceFileError::AlreadyFinished);
        }

        // Serialize metadata to get its length
        let meta_bytes = rmp_serde::to_vec(metadata)?;

        // Write header
        self.writer.write_all(TRACE_MAGIC)?;
        self.writer.write_all(&TRACE_FILE_VERSION.to_le_bytes())?;
        self.writer.write_all(&0u16.to_le_bytes())?; // flags (none set)

        // Write metadata length and data
        let meta_len = meta_bytes.len() as u32;
        self.writer.write_all(&meta_len.to_le_bytes())?;
        self.writer.write_all(&meta_bytes)?;

        // Write placeholder for event count (we'll update this in finish())
        self.event_count_pos = HEADER_SIZE as u64 + meta_len as u64;
        self.writer.write_all(&0u64.to_le_bytes())?;

        Ok(())
    }

    /// Writes a single replay event.
    ///
    /// Events are length-prefixed for streaming reads.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization or writing fails.
    pub fn write_event(&mut self, event: &ReplayEvent) -> TraceFileResult<()> {
        if self.finished {
            return Err(TraceFileError::AlreadyFinished);
        }

        // Serialize event
        let event_bytes = rmp_serde::to_vec(event)?;

        // Write length-prefixed event
        let len = event_bytes.len() as u32;
        self.writer.write_all(&len.to_le_bytes())?;
        self.writer.write_all(&event_bytes)?;

        self.event_count += 1;
        Ok(())
    }

    /// Finishes writing the trace file.
    ///
    /// This updates the event count in the header and flushes all data.
    /// Must be called to complete the file properly.
    ///
    /// # Errors
    ///
    /// Returns an error if flushing or seeking fails.
    pub fn finish(mut self) -> TraceFileResult<()> {
        self.finished = true;

        // Flush buffered data
        self.writer.flush()?;

        // Seek back and update event count
        let file = self.writer.get_mut();
        file.seek(SeekFrom::Start(self.event_count_pos))?;
        file.write_all(&self.event_count.to_le_bytes())?;
        file.flush()?;

        Ok(())
    }

    /// Returns the number of events written so far.
    #[must_use]
    pub fn event_count(&self) -> u64 {
        self.event_count
    }
}

impl Drop for TraceWriter {
    fn drop(&mut self) {
        if !self.finished {
            // Best-effort: try to flush but don't panic
            let _ = self.writer.flush();
        }
    }
}

// =============================================================================
// TraceReader
// =============================================================================

/// Reader for loading trace files.
///
/// Supports streaming reads where events are loaded incrementally.
///
/// # Example
///
/// ```ignore
/// let reader = TraceReader::open("trace.bin")?;
/// println!("Seed: {}", reader.metadata().seed);
/// println!("Events: {}", reader.event_count());
///
/// for event in reader.events() {
///     let event = event?;
///     println!("{:?}", event);
/// }
/// ```
pub struct TraceReader {
    reader: BufReader<File>,
    metadata: TraceMetadata,
    event_count: u64,
    events_read: u64,
    events_start_pos: u64,
}

impl TraceReader {
    /// Opens a trace file for reading.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be opened
    /// - The file has invalid magic bytes
    /// - The file version is unsupported
    /// - The metadata is corrupt
    pub fn open(path: impl AsRef<Path>) -> TraceFileResult<Self> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and validate magic
        let mut magic = [0u8; 11];
        reader.read_exact(&mut magic)?;
        if &magic != TRACE_MAGIC {
            return Err(TraceFileError::InvalidMagic);
        }

        // Read version
        let mut version_bytes = [0u8; 2];
        reader.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);
        if version > TRACE_FILE_VERSION {
            return Err(TraceFileError::UnsupportedVersion {
                expected: TRACE_FILE_VERSION,
                found: version,
            });
        }

        // Read flags
        let mut flags_bytes = [0u8; 2];
        reader.read_exact(&mut flags_bytes)?;
        let flags = u16::from_le_bytes(flags_bytes);
        // Check for unsupported flags (only compression flag is defined)
        if flags & FLAG_COMPRESSED != 0 {
            return Err(TraceFileError::UnsupportedFlags(flags));
        }

        // Read metadata length
        let mut meta_len_bytes = [0u8; 4];
        reader.read_exact(&mut meta_len_bytes)?;
        let meta_len = u32::from_le_bytes(meta_len_bytes) as usize;

        // Read metadata
        let mut meta_bytes = vec![0u8; meta_len];
        reader.read_exact(&mut meta_bytes)?;
        let metadata: TraceMetadata = rmp_serde::from_slice(&meta_bytes)?;

        // Validate schema version
        if metadata.version != REPLAY_SCHEMA_VERSION {
            return Err(TraceFileError::SchemaMismatch {
                expected: REPLAY_SCHEMA_VERSION,
                found: metadata.version,
            });
        }

        // Read event count
        let mut event_count_bytes = [0u8; 8];
        reader.read_exact(&mut event_count_bytes)?;
        let event_count = u64::from_le_bytes(event_count_bytes);

        // Calculate events start position
        let events_start_pos = HEADER_SIZE as u64 + meta_len as u64 + 8;

        Ok(Self {
            reader,
            metadata,
            event_count,
            events_read: 0,
            events_start_pos,
        })
    }

    /// Returns the trace metadata.
    #[must_use]
    pub fn metadata(&self) -> &TraceMetadata {
        &self.metadata
    }

    /// Returns the total number of events in the trace.
    #[must_use]
    pub fn event_count(&self) -> u64 {
        self.event_count
    }

    /// Returns the number of events read so far.
    #[must_use]
    pub fn events_read(&self) -> u64 {
        self.events_read
    }

    /// Returns an iterator over the events in the trace.
    ///
    /// Events are read incrementally from the file.
    pub fn events(self) -> TraceEventIterator {
        TraceEventIterator {
            reader: self.reader,
            remaining: self.event_count,
        }
    }

    /// Reads the next event from the trace.
    ///
    /// Returns `None` when all events have been read.
    ///
    /// # Errors
    ///
    /// Returns an error if reading or deserialization fails.
    pub fn read_event(&mut self) -> TraceFileResult<Option<ReplayEvent>> {
        if self.events_read >= self.event_count {
            return Ok(None);
        }

        // Read event length
        let mut len_bytes = [0u8; 4];
        if self.reader.read_exact(&mut len_bytes).is_err() {
            return Ok(None);
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read event data
        let mut event_bytes = vec![0u8; len];
        self.reader.read_exact(&mut event_bytes)?;

        let event: ReplayEvent = rmp_serde::from_slice(&event_bytes)?;
        self.events_read += 1;

        Ok(Some(event))
    }

    /// Resets the reader to the beginning of the events section.
    ///
    /// # Errors
    ///
    /// Returns an error if seeking fails.
    pub fn rewind(&mut self) -> TraceFileResult<()> {
        self.reader.seek(SeekFrom::Start(self.events_start_pos))?;
        self.events_read = 0;
        Ok(())
    }

    /// Loads all events into memory.
    ///
    /// This is convenient for small traces but may use significant memory
    /// for large traces. Use [`events()`][Self::events] for streaming.
    ///
    /// # Errors
    ///
    /// Returns an error if reading fails.
    pub fn load_all(mut self) -> TraceFileResult<Vec<ReplayEvent>> {
        let mut events = Vec::with_capacity(self.event_count as usize);
        while let Some(event) = self.read_event()? {
            events.push(event);
        }
        Ok(events)
    }
}

// =============================================================================
// Iterator
// =============================================================================

/// Iterator over trace events.
pub struct TraceEventIterator {
    reader: BufReader<File>,
    remaining: u64,
}

impl Iterator for TraceEventIterator {
    type Item = TraceFileResult<ReplayEvent>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        // Read event length
        let mut len_bytes = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut len_bytes) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return None;
            }
            return Some(Err(TraceFileError::Io(e)));
        }
        let len = u32::from_le_bytes(len_bytes) as usize;

        // Read event data
        let mut event_bytes = vec![0u8; len];
        if let Err(e) = self.reader.read_exact(&mut event_bytes) {
            return Some(Err(TraceFileError::Io(e)));
        }

        match rmp_serde::from_slice(&event_bytes) {
            Ok(event) => {
                self.remaining -= 1;
                Some(Ok(event))
            }
            Err(e) => Some(Err(TraceFileError::from(e))),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining as usize;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for TraceEventIterator {}

// =============================================================================
// Convenience Functions
// =============================================================================

/// Writes a complete trace to a file.
///
/// This is a convenience function for writing small traces.
/// For large traces, use [`TraceWriter`] for streaming writes.
///
/// # Errors
///
/// Returns an error if file creation or writing fails.
pub fn write_trace(
    path: impl AsRef<Path>,
    metadata: &TraceMetadata,
    events: &[ReplayEvent],
) -> TraceFileResult<()> {
    let mut writer = TraceWriter::create(path)?;
    writer.write_metadata(metadata)?;
    for event in events {
        writer.write_event(event)?;
    }
    writer.finish()
}

/// Reads a complete trace from a file.
///
/// This is a convenience function for reading small traces.
/// For large traces, use [`TraceReader`] for streaming reads.
///
/// # Errors
///
/// Returns an error if file opening or reading fails.
pub fn read_trace(path: impl AsRef<Path>) -> TraceFileResult<(TraceMetadata, Vec<ReplayEvent>)> {
    let reader = TraceReader::open(path)?;
    let metadata = reader.metadata().clone();
    let events = reader.load_all()?;
    Ok((metadata, events))
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::replay::CompactTaskId;
    use tempfile::NamedTempFile;

    fn sample_events() -> Vec<ReplayEvent> {
        vec![
            ReplayEvent::RngSeed { seed: 42 },
            ReplayEvent::TaskScheduled {
                task: CompactTaskId(1),
                at_tick: 0,
            },
            ReplayEvent::TimeAdvanced {
                from_nanos: 0,
                to_nanos: 1_000_000,
            },
            ReplayEvent::TaskYielded {
                task: CompactTaskId(1),
            },
            ReplayEvent::TaskScheduled {
                task: CompactTaskId(1),
                at_tick: 1,
            },
            ReplayEvent::TaskCompleted {
                task: CompactTaskId(1),
                outcome: 0,
            },
        ]
    }

    #[test]
    fn write_and_read_roundtrip() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let metadata = TraceMetadata::new(42).with_description("test trace");
        let events = sample_events();

        // Write
        write_trace(path, &metadata, &events).expect("write trace");

        // Read
        let (read_meta, read_events) = read_trace(path).expect("read trace");

        assert_eq!(read_meta.seed, metadata.seed);
        assert_eq!(read_meta.description, metadata.description);
        assert_eq!(read_events.len(), events.len());

        for (orig, read) in events.iter().zip(read_events.iter()) {
            assert_eq!(orig, read);
        }
    }

    #[test]
    fn streaming_write_and_read() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let metadata = TraceMetadata::new(123);
        let events = sample_events();

        // Streaming write
        {
            let mut writer = TraceWriter::create(path).expect("create writer");
            writer.write_metadata(&metadata).expect("write metadata");
            for event in &events {
                writer.write_event(event).expect("write event");
            }
            assert_eq!(writer.event_count(), events.len() as u64);
            writer.finish().expect("finish");
        }

        // Streaming read
        {
            let reader = TraceReader::open(path).expect("open reader");
            assert_eq!(reader.metadata().seed, 123);
            assert_eq!(reader.event_count(), events.len() as u64);

            let mut count = 0;
            for result in reader.events() {
                let event = result.expect("read event");
                assert_eq!(event, events[count]);
                count += 1;
            }
            assert_eq!(count, events.len());
        }
    }

    #[test]
    fn reader_rewind() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let metadata = TraceMetadata::new(42);
        let events = sample_events();
        write_trace(path, &metadata, &events).expect("write trace");

        let mut reader = TraceReader::open(path).expect("open reader");

        // Read first two events
        let e1 = reader.read_event().expect("read").expect("event");
        let e2 = reader.read_event().expect("read").expect("event");
        assert_eq!(reader.events_read(), 2);

        // Rewind and verify we get the same events
        reader.rewind().expect("rewind");
        assert_eq!(reader.events_read(), 0);

        let e1_again = reader.read_event().expect("read").expect("event");
        let e2_again = reader.read_event().expect("read").expect("event");
        assert_eq!(e1, e1_again);
        assert_eq!(e2, e2_again);
    }

    #[test]
    fn empty_trace() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let metadata = TraceMetadata::new(0);
        write_trace(path, &metadata, &[]).expect("write empty trace");

        let (read_meta, read_events) = read_trace(path).expect("read empty trace");
        assert_eq!(read_meta.seed, 0);
        assert!(read_events.is_empty());
    }

    #[test]
    fn large_trace() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let metadata = TraceMetadata::new(42);
        let event_count = 10_000;

        // Generate large trace
        let events: Vec<_> = (0..event_count)
            .map(|i| ReplayEvent::TaskScheduled {
                task: CompactTaskId(i),
                at_tick: i,
            })
            .collect();

        write_trace(path, &metadata, &events).expect("write large trace");

        // Read with streaming
        let reader = TraceReader::open(path).expect("open reader");
        assert_eq!(reader.event_count(), event_count);

        let mut count = 0u64;
        for result in reader.events() {
            let event = result.expect("read event");
            if let ReplayEvent::TaskScheduled { task, at_tick } = event {
                assert_eq!(task.0, count);
                assert_eq!(at_tick, count);
            } else {
                panic!("unexpected event type");
            }
            count += 1;
        }
        assert_eq!(count, event_count);
    }

    #[test]
    fn invalid_magic() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        // Write garbage
        std::fs::write(path, b"NOT A TRACE FILE").expect("write garbage");

        let result = TraceReader::open(path);
        assert!(matches!(result, Err(TraceFileError::InvalidMagic)));
    }

    #[test]
    fn file_size_reasonable() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let metadata = TraceMetadata::new(42);
        let events: Vec<_> = (0..1000)
            .map(|i| ReplayEvent::TaskScheduled {
                task: CompactTaskId(i),
                at_tick: i,
            })
            .collect();

        write_trace(path, &metadata, &events).expect("write trace");

        let file_size = std::fs::metadata(path).expect("metadata").len();
        let bytes_per_event = file_size as f64 / 1000.0;

        // Should be well under 64 bytes per event
        assert!(
            bytes_per_event < 40.0,
            "File size too large: {:.1} bytes/event",
            bytes_per_event
        );
    }

    #[test]
    fn writer_already_finished_error() {
        let temp = NamedTempFile::new().expect("create temp file");
        let path = temp.path();

        let mut writer = TraceWriter::create(path).expect("create writer");
        writer
            .write_metadata(&TraceMetadata::new(42))
            .expect("write metadata");
        writer.finish().expect("finish");

        // Attempting to use a finished writer should not be possible
        // because finish() consumes self, so this is compile-time safety
    }
}

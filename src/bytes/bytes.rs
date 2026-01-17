//! Immutable, reference-counted byte slice.

use super::buf::Buf;
use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

/// Immutable byte slice with cheap cloning.
///
/// Cloning a `Bytes` is O(1) - it just increments a reference count.
/// Slicing is also O(1) - no data is copied, just the view is adjusted.
///
/// # Implementation
///
/// This implementation uses `Arc<Vec<u8>>` for shared ownership rather than
/// raw pointers, ensuring memory safety without unsafe code.
///
/// # Examples
///
/// ```
/// use asupersync::bytes::Bytes;
///
/// // Create from static data (no allocation)
/// let b = Bytes::from_static(b"hello world");
/// assert_eq!(&b[..], b"hello world");
///
/// // Clone is cheap (reference counting)
/// let b2 = b.clone();
/// assert_eq!(&b2[..], b"hello world");
///
/// // Slicing is O(1)
/// let hello = b.slice(0..5);
/// assert_eq!(&hello[..], b"hello");
/// ```
#[derive(Clone)]
pub struct Bytes {
    /// The backing storage.
    data: BytesInner,
    /// Start offset within the backing storage.
    start: usize,
    /// Length of this view.
    len: usize,
}

#[derive(Clone)]
enum BytesInner {
    /// Static data (no allocation, 'static lifetime).
    Static(&'static [u8]),
    /// Heap-allocated, reference-counted data.
    Shared(Arc<Vec<u8>>),
    /// Empty bytes (no allocation).
    Empty,
}

impl Bytes {
    /// Create an empty `Bytes`.
    ///
    /// No allocation occurs.
    #[must_use]
    pub const fn new() -> Self {
        Bytes {
            data: BytesInner::Empty,
            start: 0,
            len: 0,
        }
    }

    /// Create `Bytes` from a static byte slice.
    ///
    /// No allocation occurs - the bytes point directly to static memory.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::Bytes;
    ///
    /// let b = Bytes::from_static(b"hello");
    /// assert_eq!(&b[..], b"hello");
    /// ```
    #[must_use]
    pub const fn from_static(bytes: &'static [u8]) -> Self {
        Bytes {
            data: BytesInner::Static(bytes),
            start: 0,
            len: bytes.len(),
        }
    }

    /// Copy data from a slice into a new `Bytes`.
    ///
    /// This allocates and copies the data.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::Bytes;
    ///
    /// let data = vec![1, 2, 3, 4, 5];
    /// let b = Bytes::copy_from_slice(&data);
    /// assert_eq!(&b[..], &[1, 2, 3, 4, 5]);
    /// ```
    #[must_use]
    pub fn copy_from_slice(data: &[u8]) -> Self {
        if data.is_empty() {
            return Self::new();
        }
        let vec = data.to_vec();
        let len = vec.len();
        Bytes {
            data: BytesInner::Shared(Arc::new(vec)),
            start: 0,
            len,
        }
    }

    /// Returns the number of bytes.
    #[inline]
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true if empty.
    #[inline]
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns a slice of self for the given range.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::Bytes;
    ///
    /// let b = Bytes::from_static(b"hello world");
    /// let hello = b.slice(0..5);
    /// assert_eq!(&hello[..], b"hello");
    /// ```
    #[must_use]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> Self {
        use std::ops::Bound;

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("range start overflow"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("range end overflow"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len,
        };

        assert!(
            start <= end && end <= self.len,
            "slice bounds out of range: start={start}, end={end}, len={}",
            self.len
        );

        Bytes {
            data: self.data.clone(),
            start: self.start + start,
            len: end - start,
        }
    }

    /// Split off the bytes from `at` to the end.
    ///
    /// Self becomes `[0, at)`, returns `[at, len)`.
    ///
    /// # Panics
    ///
    /// Panics if `at > len`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::Bytes;
    ///
    /// let mut b = Bytes::from_static(b"hello world");
    /// let world = b.split_off(6);
    /// assert_eq!(&b[..], b"hello ");
    /// assert_eq!(&world[..], b"world");
    /// ```
    pub fn split_off(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_off out of bounds: at={at}, len={}",
            self.len
        );

        let other = Bytes {
            data: self.data.clone(),
            start: self.start + at,
            len: self.len - at,
        };

        self.len = at;
        other
    }

    /// Split off bytes from the beginning.
    ///
    /// Self becomes `[at, len)`, returns `[0, at)`.
    ///
    /// # Panics
    ///
    /// Panics if `at > len`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::Bytes;
    ///
    /// let mut b = Bytes::from_static(b"hello world");
    /// let hello = b.split_to(6);
    /// assert_eq!(&hello[..], b"hello ");
    /// assert_eq!(&b[..], b"world");
    /// ```
    pub fn split_to(&mut self, at: usize) -> Self {
        assert!(
            at <= self.len,
            "split_to out of bounds: at={at}, len={}",
            self.len
        );

        let other = Bytes {
            data: self.data.clone(),
            start: self.start,
            len: at,
        };

        self.start += at;
        self.len -= at;
        other
    }

    /// Truncate the buffer to `len` bytes.
    ///
    /// If `len` is greater than the current length, this has no effect.
    pub fn truncate(&mut self, len: usize) {
        if len < self.len {
            self.len = len;
        }
    }

    /// Clear the buffer, making it empty.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Get the underlying byte slice.
    fn as_slice(&self) -> &[u8] {
        match &self.data {
            BytesInner::Empty => &[],
            BytesInner::Static(s) => &s[self.start..self.start + self.len],
            BytesInner::Shared(arc) => &arc[self.start..self.start + self.len],
        }
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        if vec.is_empty() {
            return Self::new();
        }
        let len = vec.len();
        Bytes {
            data: BytesInner::Shared(Arc::new(vec)),
            start: 0,
            len,
        }
    }
}

impl From<&'static [u8]> for Bytes {
    fn from(slice: &'static [u8]) -> Self {
        Self::from_static(slice)
    }
}

impl From<&'static str> for Bytes {
    fn from(s: &'static str) -> Self {
        Self::from_static(s.as_bytes())
    }
}

impl From<String> for Bytes {
    fn from(s: String) -> Self {
        Self::from(s.into_bytes())
    }
}

impl std::fmt::Debug for Bytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Bytes")
            .field("len", &self.len)
            .field("data", &self.as_slice())
            .finish()
    }
}

impl PartialEq for Bytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Eq for Bytes {}

impl PartialEq<[u8]> for Bytes {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<Bytes> for [u8] {
    fn eq(&self, other: &Bytes) -> bool {
        self == other.as_slice()
    }
}

impl PartialEq<Vec<u8>> for Bytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl std::hash::Hash for Bytes {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

// === Buf trait implementation ===

/// A cursor for reading from Bytes.
///
/// This wrapper tracks the read position, allowing Bytes to implement Buf.
#[derive(Clone, Debug)]
pub struct BytesCursor {
    inner: Bytes,
    pos: usize,
}

impl BytesCursor {
    /// Create a new cursor at position 0.
    #[must_use]
    pub fn new(bytes: Bytes) -> Self {
        BytesCursor {
            inner: bytes,
            pos: 0,
        }
    }

    /// Get a reference to the underlying Bytes.
    #[must_use]
    pub fn get_ref(&self) -> &Bytes {
        &self.inner
    }

    /// Consume the cursor, returning the underlying Bytes.
    #[must_use]
    pub fn into_inner(self) -> Bytes {
        self.inner
    }

    /// Get the current position.
    #[must_use]
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Set the position.
    pub fn set_position(&mut self, pos: usize) {
        self.pos = pos;
    }
}

impl Buf for BytesCursor {
    fn remaining(&self) -> usize {
        self.inner.len().saturating_sub(self.pos)
    }

    fn chunk(&self) -> &[u8] {
        let slice = self.inner.as_slice();
        if self.pos >= slice.len() {
            &[]
        } else {
            &slice[self.pos..]
        }
    }

    fn advance(&mut self, cnt: usize) {
        assert!(
            cnt <= self.remaining(),
            "advance out of bounds: cnt={cnt}, remaining={}",
            self.remaining()
        );
        self.pos += cnt;
    }
}

impl Bytes {
    /// Create a cursor for reading from this Bytes.
    ///
    /// The cursor implements `Buf` and tracks the read position.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::{Bytes, Buf};
    ///
    /// let b = Bytes::from_static(b"\x00\x01\x02\x03");
    /// let mut cursor = b.reader();
    /// assert_eq!(cursor.get_u8(), 0);
    /// assert_eq!(cursor.get_u8(), 1);
    /// assert_eq!(cursor.remaining(), 2);
    /// ```
    #[must_use]
    pub fn reader(self) -> BytesCursor {
        BytesCursor::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_new() {
        let b = Bytes::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn test_bytes_from_static() {
        let b = Bytes::from_static(b"hello world");
        assert_eq!(b.len(), 11);
        assert_eq!(&b[..], b"hello world");
    }

    #[test]
    fn test_bytes_copy_from_slice() {
        let data = vec![1u8, 2, 3, 4, 5];
        let b = Bytes::copy_from_slice(&data);
        assert_eq!(b.len(), 5);
        assert_eq!(&b[..], &data[..]);
    }

    #[test]
    fn test_bytes_clone_is_cheap() {
        let b1 = Bytes::copy_from_slice(&vec![0u8; 1_000_000]);
        let start = std::time::Instant::now();
        for _ in 0..1000 {
            let _b2 = b1.clone();
        }
        let elapsed = start.elapsed();
        // Should be very fast (reference counting)
        assert!(
            elapsed.as_millis() < 50,
            "Clone should be O(1), took {elapsed:?}"
        );
    }

    #[test]
    fn test_bytes_slice() {
        let b = Bytes::from_static(b"hello world");

        let hello = b.slice(0..5);
        assert_eq!(&hello[..], b"hello");

        let world = b.slice(6..);
        assert_eq!(&world[..], b"world");

        let middle = b.slice(3..8);
        assert_eq!(&middle[..], b"lo wo");
    }

    #[test]
    fn test_bytes_split_off() {
        let mut b = Bytes::from_static(b"hello world");
        let world = b.split_off(6);

        assert_eq!(&b[..], b"hello ");
        assert_eq!(&world[..], b"world");
    }

    #[test]
    fn test_bytes_split_to() {
        let mut b = Bytes::from_static(b"hello world");
        let hello = b.split_to(6);

        assert_eq!(&hello[..], b"hello ");
        assert_eq!(&b[..], b"world");
    }

    #[test]
    fn test_bytes_truncate() {
        let mut b = Bytes::from_static(b"hello world");
        b.truncate(5);
        assert_eq!(&b[..], b"hello");

        // Truncate to larger has no effect
        b.truncate(100);
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_bytes_clear() {
        let mut b = Bytes::from_static(b"hello world");
        b.clear();
        assert!(b.is_empty());
    }

    #[test]
    fn test_bytes_from_vec() {
        let v = vec![1u8, 2, 3];
        let b: Bytes = v.into();
        assert_eq!(&b[..], &[1, 2, 3]);
    }

    #[test]
    fn test_bytes_from_string() {
        let s = String::from("hello");
        let b: Bytes = s.into();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_bytes_slice_panic() {
        let b = Bytes::from_static(b"hello");
        let _bad = b.slice(0..100);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_bytes_split_off_panic() {
        let mut b = Bytes::from_static(b"hello");
        let _bad = b.split_off(100);
    }

    #[test]
    fn test_bytes_equality() {
        let b1 = Bytes::from_static(b"hello");
        let b2 = Bytes::copy_from_slice(b"hello");
        assert_eq!(b1, b2);
        assert_eq!(b1, b"hello"[..]);
        assert_eq!(b"hello"[..], b1);
    }
}

//! Mutable buffer with efficient growth and splitting.

use super::buf::BufMut;
use super::Bytes;
use std::ops::{Deref, DerefMut, RangeBounds};

/// Mutable buffer that can be frozen into `Bytes`.
///
/// `BytesMut` provides a mutable buffer with efficient growth, splitting,
/// and the ability to freeze into an immutable `Bytes`.
///
/// # Implementation
///
/// This implementation uses `Vec<u8>` as the backing storage, ensuring
/// safety without unsafe code. For small buffers, inline storage could
/// be added as an optimization in the future.
///
/// # Examples
///
/// ```
/// use asupersync::bytes::BytesMut;
///
/// let mut buf = BytesMut::with_capacity(100);
/// buf.put_slice(b"hello");
/// buf.put_slice(b" world");
///
/// let frozen = buf.freeze();
/// assert_eq!(&frozen[..], b"hello world");
/// ```
pub struct BytesMut {
    /// The backing storage.
    data: Vec<u8>,
}

impl BytesMut {
    /// Create an empty `BytesMut`.
    #[must_use]
    pub fn new() -> Self {
        BytesMut { data: Vec::new() }
    }

    /// Create a `BytesMut` with the given capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::BytesMut;
    ///
    /// let buf = BytesMut::with_capacity(100);
    /// assert!(buf.is_empty());
    /// assert!(buf.capacity() >= 100);
    /// ```
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        BytesMut {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of bytes.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if empty.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns the capacity.
    #[inline]
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }

    /// Freeze into an immutable `Bytes`.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::BytesMut;
    ///
    /// let mut buf = BytesMut::new();
    /// buf.put_slice(b"hello world");
    ///
    /// let frozen = buf.freeze();
    /// assert_eq!(&frozen[..], b"hello world");
    /// ```
    #[must_use]
    pub fn freeze(self) -> Bytes {
        Bytes::from(self.data)
    }

    /// Reserve at least `additional` more bytes of capacity.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::BytesMut;
    ///
    /// let mut buf = BytesMut::new();
    /// buf.reserve(100);
    /// assert!(buf.capacity() >= 100);
    /// ```
    pub fn reserve(&mut self, additional: usize) {
        self.data.reserve(additional);
    }

    /// Append bytes to the buffer.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::BytesMut;
    ///
    /// let mut buf = BytesMut::new();
    /// buf.put_slice(b"hello");
    /// buf.put_slice(b" world");
    /// assert_eq!(&buf[..], b"hello world");
    /// ```
    pub fn put_slice(&mut self, src: &[u8]) {
        self.data.extend_from_slice(src);
    }

    /// Extend from slice (alias for `put_slice`).
    pub fn extend_from_slice(&mut self, src: &[u8]) {
        self.put_slice(src);
    }

    /// Put a single byte.
    pub fn put_u8(&mut self, n: u8) {
        self.data.push(n);
    }

    /// Split off bytes from `at` to end.
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
    /// use asupersync::bytes::BytesMut;
    ///
    /// let mut buf = BytesMut::new();
    /// buf.put_slice(b"hello world");
    ///
    /// let world = buf.split_off(6);
    /// assert_eq!(&buf[..], b"hello ");
    /// assert_eq!(&world[..], b"world");
    /// ```
    pub fn split_off(&mut self, at: usize) -> BytesMut {
        assert!(
            at <= self.len(),
            "split_off out of bounds: at={at}, len={}",
            self.len()
        );

        let tail = self.data.split_off(at);
        BytesMut { data: tail }
    }

    /// Split off bytes from beginning to `at`.
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
    /// use asupersync::bytes::BytesMut;
    ///
    /// let mut buf = BytesMut::new();
    /// buf.put_slice(b"hello world");
    ///
    /// let hello = buf.split_to(6);
    /// assert_eq!(&hello[..], b"hello ");
    /// assert_eq!(&buf[..], b"world");
    /// ```
    pub fn split_to(&mut self, at: usize) -> BytesMut {
        assert!(
            at <= self.len(),
            "split_to out of bounds: at={at}, len={}",
            self.len()
        );

        let mut head = Vec::with_capacity(at);
        head.extend_from_slice(&self.data[..at]);

        // Remove the head from our data
        self.data.drain(..at);

        BytesMut { data: head }
    }

    /// Truncate to `len` bytes.
    ///
    /// If `len` is greater than the current length, this has no effect.
    pub fn truncate(&mut self, len: usize) {
        self.data.truncate(len);
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Resize to `new_len`, filling with `value` if growing.
    ///
    /// # Examples
    ///
    /// ```
    /// use asupersync::bytes::BytesMut;
    ///
    /// let mut buf = BytesMut::new();
    /// buf.put_slice(b"hello");
    ///
    /// // Grow
    /// buf.resize(10, b'!');
    /// assert_eq!(&buf[..], b"hello!!!!!");
    ///
    /// // Shrink
    /// buf.resize(5, 0);
    /// assert_eq!(&buf[..], b"hello");
    /// ```
    pub fn resize(&mut self, new_len: usize, value: u8) {
        self.data.resize(new_len, value);
    }

    /// Returns a slice of self for the given range.
    ///
    /// # Panics
    ///
    /// Panics if the range is out of bounds.
    #[must_use]
    pub fn slice(&self, range: impl RangeBounds<usize>) -> &[u8] {
        use std::ops::Bound;

        let start = match range.start_bound() {
            Bound::Included(&n) => n,
            Bound::Excluded(&n) => n.checked_add(1).expect("range start overflow"),
            Bound::Unbounded => 0,
        };

        let end = match range.end_bound() {
            Bound::Included(&n) => n.checked_add(1).expect("range end overflow"),
            Bound::Excluded(&n) => n,
            Bound::Unbounded => self.len(),
        };

        &self.data[start..end]
    }

    /// Returns the remaining spare capacity as a mutable slice.
    #[must_use]
    pub fn spare_capacity_mut(&mut self) -> &mut [std::mem::MaybeUninit<u8>] {
        self.data.spare_capacity_mut()
    }

    /// Set the length of the buffer.
    ///
    /// # Safety
    ///
    /// This is safe because we only allow setting length to values within
    /// capacity, and we use safe Vec operations. However, the caller must
    /// ensure that all bytes up to `len` have been initialized.
    ///
    /// # Panics
    ///
    /// Panics if `len > capacity`.
    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity(),
            "set_len out of bounds: len={len}, capacity={}",
            self.capacity()
        );
        // This is safe because we're within capacity bounds
        // and Vec tracks initialization
        self.data.resize(len, 0);
    }
}

impl Default for BytesMut {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for BytesMut {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.data
    }
}

impl DerefMut for BytesMut {
    fn deref_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl AsRef<[u8]> for BytesMut {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for BytesMut {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl From<Vec<u8>> for BytesMut {
    fn from(vec: Vec<u8>) -> Self {
        BytesMut { data: vec }
    }
}

impl From<&[u8]> for BytesMut {
    fn from(slice: &[u8]) -> Self {
        BytesMut {
            data: slice.to_vec(),
        }
    }
}

impl From<&str> for BytesMut {
    fn from(s: &str) -> Self {
        BytesMut {
            data: s.as_bytes().to_vec(),
        }
    }
}

impl From<String> for BytesMut {
    fn from(s: String) -> Self {
        BytesMut {
            data: s.into_bytes(),
        }
    }
}

impl std::fmt::Debug for BytesMut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesMut")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .field("data", &self.data.as_slice())
            .finish()
    }
}

impl PartialEq for BytesMut {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for BytesMut {}

impl PartialEq<[u8]> for BytesMut {
    fn eq(&self, other: &[u8]) -> bool {
        self.data.as_slice() == other
    }
}

impl PartialEq<BytesMut> for [u8] {
    fn eq(&self, other: &BytesMut) -> bool {
        self == other.data.as_slice()
    }
}

impl PartialEq<Vec<u8>> for BytesMut {
    fn eq(&self, other: &Vec<u8>) -> bool {
        &self.data == other
    }
}

impl std::hash::Hash for BytesMut {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

// === BufMut trait implementation ===

impl BufMut for BytesMut {
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.len()
    }

    fn chunk_mut(&mut self) -> &mut [u8] {
        // For BytesMut, we grow dynamically via put_slice
        // Return an empty slice since we handle growth in put_slice
        &mut []
    }

    fn advance_mut(&mut self, _cnt: usize) {
        // For BytesMut, advance is handled implicitly in put_slice
    }

    // Override put_slice for efficient BytesMut implementation
    fn put_slice(&mut self, src: &[u8]) {
        self.data.extend_from_slice(src);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_mut_new() {
        let b = BytesMut::new();
        assert!(b.is_empty());
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn test_bytes_mut_with_capacity() {
        let b = BytesMut::with_capacity(100);
        assert!(b.is_empty());
        assert!(b.capacity() >= 100);
    }

    #[test]
    fn test_bytes_mut_put_slice() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello");
        b.put_slice(b" ");
        b.put_slice(b"world");

        assert_eq!(&b[..], b"hello world");
    }

    #[test]
    fn test_bytes_mut_reserve_and_grow() {
        let mut b = BytesMut::new();

        // Small write
        b.put_slice(b"hello");
        assert_eq!(b.len(), 5);

        // Reserve more
        b.reserve(1000);
        assert!(b.capacity() >= 1005);

        // Data should be preserved
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_freeze() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello world");

        let frozen = b.freeze();
        assert_eq!(&frozen[..], b"hello world");

        // Should be able to clone cheaply
        let clone = frozen.clone();
        assert_eq!(&clone[..], b"hello world");
    }

    #[test]
    fn test_bytes_mut_split_off() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello world");

        let world = b.split_off(6);

        assert_eq!(&b[..], b"hello ");
        assert_eq!(&world[..], b"world");
    }

    #[test]
    fn test_bytes_mut_split_to() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello world");

        let hello = b.split_to(6);

        assert_eq!(&hello[..], b"hello ");
        assert_eq!(&b[..], b"world");
    }

    #[test]
    fn test_bytes_mut_resize() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello");

        // Grow
        b.resize(10, b'!');
        assert_eq!(&b[..], b"hello!!!!!");

        // Shrink
        b.resize(5, 0);
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_truncate() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello world");
        b.truncate(5);
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_clear() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello world");
        b.clear();
        assert!(b.is_empty());
    }

    #[test]
    fn test_bytes_mut_from_vec() {
        let v = vec![1u8, 2, 3];
        let b: BytesMut = v.into();
        assert_eq!(&b[..], &[1, 2, 3]);
    }

    #[test]
    fn test_bytes_mut_from_slice() {
        let b: BytesMut = b"hello".as_slice().into();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    fn test_bytes_mut_from_string() {
        let s = String::from("hello");
        let b: BytesMut = s.into();
        assert_eq!(&b[..], b"hello");
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_bytes_mut_split_off_panic() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello");
        let _bad = b.split_off(100);
    }

    #[test]
    #[should_panic(expected = "out of bounds")]
    fn test_bytes_mut_split_to_panic() {
        let mut b = BytesMut::new();
        b.put_slice(b"hello");
        let _bad = b.split_to(100);
    }
}

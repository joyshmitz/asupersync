//! Take adapter for limiting bytes read from a Buf.

use super::Buf;

/// A `Buf` adapter that limits the bytes read.
///
/// Created by [`Buf::take()`].
///
/// # Examples
///
/// ```
/// use asupersync::bytes::Buf;
///
/// let buf: &[u8] = &[1, 2, 3, 4, 5];
/// let mut take = buf.take(3);
///
/// assert_eq!(take.remaining(), 3);
///
/// let mut dst = [0u8; 3];
/// take.copy_to_slice(&mut dst);
/// assert_eq!(dst, [1, 2, 3]);
/// ```
#[derive(Debug)]
pub struct Take<T> {
    inner: T,
    limit: usize,
}

impl<T> Take<T> {
    /// Create a new `Take`.
    pub(crate) fn new(inner: T, limit: usize) -> Self {
        Take { inner, limit }
    }

    /// Consumes this `Take`, returning the underlying buffer.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Gets a reference to the underlying buffer.
    ///
    /// The reader position of the returned reference may not be the same
    /// as that of the buffer passed to [`Buf::take()`].
    #[must_use]
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Gets a mutable reference to the underlying buffer.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Returns the maximum number of bytes that can be read.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Sets the maximum number of bytes that can be read.
    ///
    /// Note: this does not reset the position of the inner buffer.
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }
}

impl<T: Buf> Buf for Take<T> {
    fn remaining(&self) -> usize {
        std::cmp::min(self.inner.remaining(), self.limit)
    }

    fn chunk(&self) -> &[u8] {
        let chunk = self.inner.chunk();
        let len = std::cmp::min(chunk.len(), self.limit);
        &chunk[..len]
    }

    fn advance(&mut self, cnt: usize) {
        assert!(
            cnt <= self.limit,
            "advance out of bounds: cnt={cnt}, limit={}",
            self.limit
        );
        self.inner.advance(cnt);
        self.limit -= cnt;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_take_remaining() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let take = Take::new(buf, 3);
        assert_eq!(take.remaining(), 3);
    }

    #[test]
    fn test_take_remaining_when_inner_smaller() {
        let buf: &[u8] = &[1, 2];
        let take = Take::new(buf, 10);
        assert_eq!(take.remaining(), 2);
    }

    #[test]
    fn test_take_chunk() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let take = Take::new(buf, 3);
        assert_eq!(take.chunk(), &[1, 2, 3]);
    }

    #[test]
    fn test_take_advance() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let mut take = Take::new(buf, 3);

        take.advance(2);
        assert_eq!(take.remaining(), 1);
        assert_eq!(take.chunk(), &[3]);
    }

    #[test]
    fn test_take_copy_to_slice() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let mut take = Take::new(buf, 3);

        let mut dst = [0u8; 3];
        take.copy_to_slice(&mut dst);
        assert_eq!(dst, [1, 2, 3]);
        assert_eq!(take.remaining(), 0);
    }

    #[test]
    fn test_take_limit() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let mut take = Take::new(buf, 3);
        assert_eq!(take.limit(), 3);

        take.set_limit(5);
        assert_eq!(take.limit(), 5);
        assert_eq!(take.remaining(), 5);
    }

    #[test]
    fn test_take_into_inner() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let take = Take::new(buf, 3);
        let inner = take.into_inner();
        assert_eq!(inner, &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_take_get_ref() {
        let buf: &[u8] = &[1, 2, 3, 4, 5];
        let take = Take::new(buf, 3);
        assert_eq!(*take.get_ref(), &[1, 2, 3, 4, 5][..]);
    }
}

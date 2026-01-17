//! Limit adapter for limiting bytes written to a BufMut.

use super::BufMut;

/// A `BufMut` adapter that limits the bytes written.
///
/// Created by [`BufMut::limit()`].
///
/// # Examples
///
/// ```
/// use asupersync::bytes::BufMut;
///
/// let mut buf = Vec::new();
/// let mut limit = (&mut buf as &mut Vec<u8>).limit(3);
///
/// // This would panic without the limit adapter on an infinite buffer
/// // With limit, we can only write 3 bytes
/// limit.put_slice(&[1, 2, 3]);
///
/// assert_eq!(buf, vec![1, 2, 3]);
/// ```
#[derive(Debug)]
pub struct Limit<T> {
    inner: T,
    limit: usize,
}

impl<T> Limit<T> {
    /// Create a new `Limit`.
    pub(crate) fn new(inner: T, limit: usize) -> Self {
        Limit { inner, limit }
    }

    /// Consumes this `Limit`, returning the underlying buffer.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Gets a reference to the underlying buffer.
    #[must_use]
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Gets a mutable reference to the underlying buffer.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Returns the maximum number of bytes that can be written.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Sets the maximum number of bytes that can be written.
    pub fn set_limit(&mut self, limit: usize) {
        self.limit = limit;
    }
}

impl<T: BufMut> BufMut for Limit<T> {
    fn remaining_mut(&self) -> usize {
        std::cmp::min(self.inner.remaining_mut(), self.limit)
    }

    fn chunk_mut(&mut self) -> &mut [u8] {
        let chunk = self.inner.chunk_mut();
        let len = std::cmp::min(chunk.len(), self.limit);
        &mut chunk[..len]
    }

    fn advance_mut(&mut self, cnt: usize) {
        assert!(
            cnt <= self.limit,
            "advance_mut out of bounds: cnt={cnt}, limit={}",
            self.limit
        );
        self.inner.advance_mut(cnt);
        self.limit -= cnt;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_remaining_mut() {
        let mut data = [0u8; 10];
        let buf: &mut [u8] = &mut data;
        let limit = Limit::new(buf, 5);
        assert_eq!(limit.remaining_mut(), 5);
    }

    #[test]
    fn test_limit_remaining_mut_when_inner_smaller() {
        let mut data = [0u8; 3];
        let buf: &mut [u8] = &mut data;
        let limit = Limit::new(buf, 10);
        assert_eq!(limit.remaining_mut(), 3);
    }

    #[test]
    fn test_limit_put_slice() {
        let mut data = [0u8; 10];
        {
            let buf: &mut [u8] = &mut data;
            let mut limit = Limit::new(buf, 5);
            limit.put_slice(&[1, 2, 3]);
            assert_eq!(limit.remaining_mut(), 2);
        }
        assert_eq!(&data[..5], &[1, 2, 3, 0, 0]);
    }

    #[test]
    fn test_limit_accessors() {
        let mut data = [0u8; 10];
        let buf: &mut [u8] = &mut data;
        let mut limit = Limit::new(buf, 5);

        assert_eq!(limit.limit(), 5);
        limit.set_limit(3);
        assert_eq!(limit.limit(), 3);
    }

    #[test]
    fn test_limit_into_inner() {
        let mut data = [0u8; 10];
        let buf: &mut [u8] = &mut data;
        let limit = Limit::new(buf, 5);
        let inner = limit.into_inner();
        assert_eq!(inner.len(), 10);
    }
}

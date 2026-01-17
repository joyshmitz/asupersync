//! Chain adapter for chaining two Buf implementations.

use super::Buf;

/// A `Buf` that chains two buffers together.
///
/// Created by [`Buf::chain()`].
///
/// # Examples
///
/// ```
/// use asupersync::bytes::Buf;
///
/// let a: &[u8] = &[1, 2, 3];
/// let b: &[u8] = &[4, 5, 6];
///
/// let mut chain = a.chain(b);
/// assert_eq!(chain.remaining(), 6);
///
/// let mut dst = [0u8; 6];
/// chain.copy_to_slice(&mut dst);
/// assert_eq!(dst, [1, 2, 3, 4, 5, 6]);
/// ```
#[derive(Debug)]
pub struct Chain<T, U> {
    a: T,
    b: U,
}

impl<T, U> Chain<T, U> {
    /// Create a new `Chain` from two buffers.
    pub(crate) fn new(a: T, b: U) -> Self {
        Chain { a, b }
    }

    /// Gets a reference to the first buffer.
    #[must_use]
    pub fn first_ref(&self) -> &T {
        &self.a
    }

    /// Gets a mutable reference to the first buffer.
    pub fn first_mut(&mut self) -> &mut T {
        &mut self.a
    }

    /// Gets a reference to the second buffer.
    #[must_use]
    pub fn last_ref(&self) -> &U {
        &self.b
    }

    /// Gets a mutable reference to the second buffer.
    pub fn last_mut(&mut self) -> &mut U {
        &mut self.b
    }

    /// Consumes this `Chain`, returning the underlying buffers.
    #[must_use]
    pub fn into_inner(self) -> (T, U) {
        (self.a, self.b)
    }
}

impl<T: Buf, U: Buf> Buf for Chain<T, U> {
    fn remaining(&self) -> usize {
        self.a.remaining() + self.b.remaining()
    }

    fn chunk(&self) -> &[u8] {
        if self.a.has_remaining() {
            self.a.chunk()
        } else {
            self.b.chunk()
        }
    }

    fn advance(&mut self, mut cnt: usize) {
        let a_rem = self.a.remaining();

        if cnt <= a_rem {
            self.a.advance(cnt);
        } else {
            // Drain all of a
            if a_rem > 0 {
                self.a.advance(a_rem);
            }
            cnt -= a_rem;

            // Advance b
            self.b.advance(cnt);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_remaining() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[4, 5, 6];
        let chain = Chain::new(a, b);
        assert_eq!(chain.remaining(), 6);
    }

    #[test]
    fn test_chain_chunk() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[4, 5, 6];
        let chain = Chain::new(a, b);
        assert_eq!(chain.chunk(), &[1, 2, 3]);
    }

    #[test]
    fn test_chain_advance() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[4, 5, 6];
        let mut chain = Chain::new(a, b);

        chain.advance(2);
        assert_eq!(chain.remaining(), 4);
        assert_eq!(chain.chunk(), &[3]);

        chain.advance(1);
        assert_eq!(chain.remaining(), 3);
        assert_eq!(chain.chunk(), &[4, 5, 6]);

        chain.advance(2);
        assert_eq!(chain.remaining(), 1);
        assert_eq!(chain.chunk(), &[6]);
    }

    #[test]
    fn test_chain_copy_to_slice() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[4, 5, 6];
        let mut chain = Chain::new(a, b);

        let mut dst = [0u8; 6];
        chain.copy_to_slice(&mut dst);
        assert_eq!(dst, [1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_chain_getters() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[4, 5, 6];
        let mut chain = Chain::new(a, b);

        assert_eq!(*chain.first_ref(), &[1, 2, 3][..]);
        assert_eq!(*chain.last_ref(), &[4, 5, 6][..]);

        // Advance and check
        chain.advance(4);
        assert_eq!(*chain.first_ref(), &[][..]);
        assert_eq!(*chain.last_ref(), &[5, 6][..]);
    }

    #[test]
    fn test_chain_into_inner() {
        let a: &[u8] = &[1, 2, 3];
        let b: &[u8] = &[4, 5, 6];
        let chain = Chain::new(a, b);

        let (a_out, b_out) = chain.into_inner();
        assert_eq!(a_out, &[1, 2, 3]);
        assert_eq!(b_out, &[4, 5, 6]);
    }
}

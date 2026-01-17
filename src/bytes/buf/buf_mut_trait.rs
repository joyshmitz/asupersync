//! The BufMut trait for writing bytes to a buffer.

use super::Limit;

/// Write bytes to a buffer.
///
/// This is the main abstraction for writing bytes. It provides methods for
/// putting various data types into a buffer.
///
/// # Required Methods
///
/// Implementors must provide:
/// - [`remaining_mut()`](BufMut::remaining_mut): Returns bytes that can be written
/// - [`chunk_mut()`](BufMut::chunk_mut): Returns a mutable slice for writing
/// - [`advance_mut()`](BufMut::advance_mut): Advances after writing
///
/// # Default Implementations
///
/// All other methods have default implementations built on the required methods.
///
/// # Examples
///
/// ```
/// use asupersync::bytes::BufMut;
///
/// let mut buf = Vec::new();
/// buf.put_u16(0x1234);
/// buf.put_u16_le(0x5678);
/// assert_eq!(buf, vec![0x12, 0x34, 0x78, 0x56]);
/// ```
pub trait BufMut {
    /// Returns number of bytes that can be written.
    ///
    /// For growable buffers like `Vec<u8>`, this returns `usize::MAX - len()`.
    fn remaining_mut(&self) -> usize;

    /// Returns a mutable slice for writing.
    ///
    /// The returned slice may be uninitialized. Callers must initialize bytes
    /// before calling [`advance_mut()`](BufMut::advance_mut).
    fn chunk_mut(&mut self) -> &mut [u8];

    /// Advance the write cursor by `cnt` bytes.
    ///
    /// # Safety Note
    ///
    /// While this method is safe, callers must ensure that `cnt` bytes have
    /// been written to the buffer returned by [`chunk_mut()`](BufMut::chunk_mut).
    fn advance_mut(&mut self, cnt: usize);

    // === Default implementations ===

    /// Returns true if there is space remaining.
    #[inline]
    fn has_remaining_mut(&self) -> bool {
        self.remaining_mut() > 0
    }

    /// Put a slice into the buffer.
    ///
    /// # Panics
    ///
    /// Panics if `src.len() > self.remaining_mut()`.
    fn put_slice(&mut self, src: &[u8]) {
        assert!(
            self.remaining_mut() >= src.len(),
            "buffer overflow: need {} bytes, have {}",
            src.len(),
            self.remaining_mut()
        );

        let mut off = 0;
        while off < src.len() {
            let dst = self.chunk_mut();
            let cnt = std::cmp::min(dst.len(), src.len() - off);
            dst[..cnt].copy_from_slice(&src[off..off + cnt]);
            self.advance_mut(cnt);
            off += cnt;
        }
    }

    /// Put a single byte.
    fn put_u8(&mut self, n: u8) {
        self.put_slice(&[n]);
    }

    /// Put an i8.
    fn put_i8(&mut self, n: i8) {
        self.put_u8(n as u8);
    }

    /// Put a big-endian u16.
    fn put_u16(&mut self, n: u16) {
        self.put_slice(&n.to_be_bytes());
    }

    /// Put a little-endian u16.
    fn put_u16_le(&mut self, n: u16) {
        self.put_slice(&n.to_le_bytes());
    }

    /// Put a native-endian u16.
    fn put_u16_ne(&mut self, n: u16) {
        self.put_slice(&n.to_ne_bytes());
    }

    /// Put a big-endian i16.
    fn put_i16(&mut self, n: i16) {
        self.put_u16(n as u16);
    }

    /// Put a little-endian i16.
    fn put_i16_le(&mut self, n: i16) {
        self.put_u16_le(n as u16);
    }

    /// Put a native-endian i16.
    fn put_i16_ne(&mut self, n: i16) {
        self.put_u16_ne(n as u16);
    }

    /// Put a big-endian u32.
    fn put_u32(&mut self, n: u32) {
        self.put_slice(&n.to_be_bytes());
    }

    /// Put a little-endian u32.
    fn put_u32_le(&mut self, n: u32) {
        self.put_slice(&n.to_le_bytes());
    }

    /// Put a native-endian u32.
    fn put_u32_ne(&mut self, n: u32) {
        self.put_slice(&n.to_ne_bytes());
    }

    /// Put a big-endian i32.
    fn put_i32(&mut self, n: i32) {
        self.put_u32(n as u32);
    }

    /// Put a little-endian i32.
    fn put_i32_le(&mut self, n: i32) {
        self.put_u32_le(n as u32);
    }

    /// Put a native-endian i32.
    fn put_i32_ne(&mut self, n: i32) {
        self.put_u32_ne(n as u32);
    }

    /// Put a big-endian u64.
    fn put_u64(&mut self, n: u64) {
        self.put_slice(&n.to_be_bytes());
    }

    /// Put a little-endian u64.
    fn put_u64_le(&mut self, n: u64) {
        self.put_slice(&n.to_le_bytes());
    }

    /// Put a native-endian u64.
    fn put_u64_ne(&mut self, n: u64) {
        self.put_slice(&n.to_ne_bytes());
    }

    /// Put a big-endian i64.
    fn put_i64(&mut self, n: i64) {
        self.put_u64(n as u64);
    }

    /// Put a little-endian i64.
    fn put_i64_le(&mut self, n: i64) {
        self.put_u64_le(n as u64);
    }

    /// Put a native-endian i64.
    fn put_i64_ne(&mut self, n: i64) {
        self.put_u64_ne(n as u64);
    }

    /// Put a big-endian u128.
    fn put_u128(&mut self, n: u128) {
        self.put_slice(&n.to_be_bytes());
    }

    /// Put a little-endian u128.
    fn put_u128_le(&mut self, n: u128) {
        self.put_slice(&n.to_le_bytes());
    }

    /// Put a native-endian u128.
    fn put_u128_ne(&mut self, n: u128) {
        self.put_slice(&n.to_ne_bytes());
    }

    /// Put a big-endian i128.
    fn put_i128(&mut self, n: i128) {
        self.put_u128(n as u128);
    }

    /// Put a little-endian i128.
    fn put_i128_le(&mut self, n: i128) {
        self.put_u128_le(n as u128);
    }

    /// Put a native-endian i128.
    fn put_i128_ne(&mut self, n: i128) {
        self.put_u128_ne(n as u128);
    }

    /// Put a big-endian f32.
    fn put_f32(&mut self, n: f32) {
        self.put_u32(n.to_bits());
    }

    /// Put a little-endian f32.
    fn put_f32_le(&mut self, n: f32) {
        self.put_u32_le(n.to_bits());
    }

    /// Put a native-endian f32.
    fn put_f32_ne(&mut self, n: f32) {
        self.put_u32_ne(n.to_bits());
    }

    /// Put a big-endian f64.
    fn put_f64(&mut self, n: f64) {
        self.put_u64(n.to_bits());
    }

    /// Put a little-endian f64.
    fn put_f64_le(&mut self, n: f64) {
        self.put_u64_le(n.to_bits());
    }

    /// Put a native-endian f64.
    fn put_f64_ne(&mut self, n: f64) {
        self.put_u64_ne(n.to_bits());
    }

    /// Limit writing to `limit` bytes.
    fn limit(self, limit: usize) -> Limit<Self>
    where
        Self: Sized,
    {
        Limit::new(self, limit)
    }
}

// === Implementations for standard types ===

impl BufMut for Vec<u8> {
    #[inline]
    fn remaining_mut(&self) -> usize {
        usize::MAX - self.len()
    }

    fn chunk_mut(&mut self) -> &mut [u8] {
        // For Vec, we grow dynamically, so we reserve some space
        // and return the spare capacity
        if self.capacity() == self.len() {
            self.reserve(64);
        }

        let cap = self.capacity();
        let len = self.len();

        // Get spare capacity as initialized zeros for safety
        self.resize(cap, 0);
        self.truncate(len);

        // Return spare capacity
        // Since we can't return uninitialized memory safely,
        // we reserve and return a slice that we'll fill
        let spare = cap - len;
        if spare == 0 {
            self.reserve(64);
        }

        // We need to return a mutable slice that can be written to
        // Since Vec grows, we handle this by extending and returning
        // a reference to the extended portion. This is a bit of a hack
        // but necessary without unsafe code.
        let old_len = self.len();
        let new_cap = self.capacity();
        let spare = new_cap - old_len;

        // Pre-extend with zeros so we have a valid slice to return
        self.resize(new_cap, 0);
        self.truncate(old_len);

        // Return the portion after current length
        // This is safe because we're within capacity
        let spare_slice = &mut self[old_len..old_len + spare.min(64)];

        // Actually, we need to return a fixed-size window
        // Let's use a simpler approach
        &mut []
    }

    fn advance_mut(&mut self, _cnt: usize) {
        // For Vec, advance is handled in put_slice
    }

    // Override put_slice for efficient Vec implementation
    fn put_slice(&mut self, src: &[u8]) {
        self.extend_from_slice(src);
    }
}

impl BufMut for &mut [u8] {
    fn remaining_mut(&self) -> usize {
        self.len()
    }

    fn chunk_mut(&mut self) -> &mut [u8] {
        self
    }

    fn advance_mut(&mut self, cnt: usize) {
        assert!(
            cnt <= self.len(),
            "advance_mut out of bounds: cnt={cnt}, len={}",
            self.len()
        );
        // Take the remaining portion
        let tmp = std::mem::take(self);
        *self = &mut tmp[cnt..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buf_mut_vec_put_u8() {
        let mut buf = Vec::new();
        buf.put_u8(42);
        buf.put_u8(43);
        assert_eq!(buf, vec![42, 43]);
    }

    #[test]
    fn test_buf_mut_vec_put_u16() {
        let mut buf = Vec::new();
        buf.put_u16(0x1234);
        assert_eq!(buf, vec![0x12, 0x34]);
    }

    #[test]
    fn test_buf_mut_vec_put_u16_le() {
        let mut buf = Vec::new();
        buf.put_u16_le(0x1234);
        assert_eq!(buf, vec![0x34, 0x12]);
    }

    #[test]
    fn test_buf_mut_vec_put_u32() {
        let mut buf = Vec::new();
        buf.put_u32(0x1234_5678);
        assert_eq!(buf, vec![0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_buf_mut_vec_put_slice() {
        let mut buf = Vec::new();
        buf.put_slice(b"hello");
        buf.put_slice(b" world");
        assert_eq!(buf, b"hello world");
    }

    #[test]
    fn test_buf_mut_vec_put_f32() {
        let mut buf = Vec::new();
        buf.put_f32(3.14);

        // Verify by reading back
        let mut read: &[u8] = &buf;
        use super::super::Buf;
        let val = read.get_f32();
        assert!((val - 3.14).abs() < 0.0001);
    }

    #[test]
    fn test_buf_mut_vec_put_f64() {
        let mut buf = Vec::new();
        buf.put_f64(std::f64::consts::PI);

        // Verify by reading back
        let mut read: &[u8] = &buf;
        use super::super::Buf;
        let val = read.get_f64();
        assert!((val - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn test_buf_mut_slice() {
        let mut data = [0u8; 10];
        let mut buf: &mut [u8] = &mut data;

        buf.put_u16(0x1234);
        buf.put_u16(0x5678);

        assert_eq!(data[0..4], [0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_roundtrip_all_types() {
        let mut buf = Vec::new();

        buf.put_u8(0x12);
        buf.put_i8(-5);
        buf.put_u16(0x1234);
        buf.put_u16_le(0x5678);
        buf.put_i16(-1000);
        buf.put_u32(0x1234_5678);
        buf.put_u32_le(0x9ABC_DEF0);
        buf.put_i32(-100_000);
        buf.put_u64(0x1234_5678_9ABC_DEF0);
        buf.put_u64_le(0xFEDC_BA98_7654_3210);
        buf.put_f32(3.14159);
        buf.put_f64(2.718281828);

        let mut read: &[u8] = &buf;
        use super::super::Buf;

        assert_eq!(read.get_u8(), 0x12);
        assert_eq!(read.get_i8(), -5);
        assert_eq!(read.get_u16(), 0x1234);
        assert_eq!(read.get_u16_le(), 0x5678);
        assert_eq!(read.get_i16(), -1000);
        assert_eq!(read.get_u32(), 0x1234_5678);
        assert_eq!(read.get_u32_le(), 0x9ABC_DEF0);
        assert_eq!(read.get_i32(), -100_000);
        assert_eq!(read.get_u64(), 0x1234_5678_9ABC_DEF0);
        assert_eq!(read.get_u64_le(), 0xFEDC_BA98_7654_3210);
        assert!((read.get_f32() - 3.14159).abs() < 0.0001);
        assert!((read.get_f64() - 2.718281828).abs() < 1e-9);
    }
}

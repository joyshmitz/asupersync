//! WebSocket protocol implementation (RFC 6455).
//!
//! This module provides WebSocket frame encoding/decoding and HTTP upgrade handshake support.
//!
//! # Architecture
//!
//! - `frame`: Wire format encoding/decoding (RFC 6455 Section 5)
//! - `handshake`: HTTP upgrade negotiation (RFC 6455 Section 4)
//! - `close`: Close handshake protocol (RFC 6455 Section 7)
//!
//! # Example
//!
//! ```ignore
//! use asupersync::net::websocket::{FrameCodec, Frame, Role};
//!
//! // Create a server-side codec
//! let mut codec = FrameCodec::server();
//!
//! // Encode a text frame
//! let frame = Frame::text("Hello, WebSocket!");
//! codec.encode(frame, &mut buf)?;
//! ```

mod close;
mod frame;
mod handshake;

pub use close::{CloseConfig, CloseHandshake, CloseReason, CloseState};
pub use frame::{apply_mask, CloseCode, Frame, FrameCodec, Opcode, Role, WsError};
pub use handshake::{compute_accept_key, ClientHandshake, HandshakeError, ServerHandshake, WsUrl};

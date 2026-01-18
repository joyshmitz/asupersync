//! Helpers for forwarding streams to channels.

use crate::channel::mpsc;
use crate::cx::Cx;
use crate::error::SendError;
use crate::stream::{Stream, StreamExt};

/// Sink wrapper for mpsc sender.
pub struct SinkStream<T> {
    sender: mpsc::Sender<T>,
}

impl<T> SinkStream<T> {
    /// Create a new SinkStream.
    pub fn new(sender: mpsc::Sender<T>) -> Self {
        Self { sender }
    }

    /// Send item through the channel.
    pub fn send(&self, cx: &Cx, item: T) -> Result<(), SendError<T>> {
        self.sender.send(cx, item)
    }

    /// Send all items from stream.
    ///
    /// Note: This is an async method in the sense that it consumes the stream asynchronously,
    /// but in Phase 0 `send` is blocking (waiting for capacity).
    /// Since we don't have async trait methods, and `StreamExt::next()` returns a future,
    /// we need to await the next item.
    pub async fn send_all<S>(&self, cx: &Cx, mut stream: S) -> Result<(), SendError<S::Item>>
    where
        S: Stream<Item = T> + Unpin,
    {
        while let Some(item) = stream.next().await {
            self.sender.send(cx, item)?;
        }
        Ok(())
    }
}

/// Convert a stream into a channel sender.
pub fn into_sink<T>(sender: mpsc::Sender<T>) -> SinkStream<T> {
    SinkStream::new(sender)
}

/// Forward stream to channel.
pub async fn forward<S, T>(
    cx: &Cx,
    mut stream: S,
    sender: mpsc::Sender<T>,
) -> Result<(), SendError<T>>
where
    S: Stream<Item = T> + Unpin,
{
    while let Some(item) = stream.next().await {
        sender.send(cx, item)?;
    }
    Ok(())
}

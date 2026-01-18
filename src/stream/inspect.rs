//! Inspect combinator.

use super::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Stream for the [`inspect`](super::StreamExt::inspect) method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Inspect<S, F> {
    stream: S,
    f: F,
}

impl<S, F> Inspect<S, F> {
    pub(crate) fn new(stream: S, f: F) -> Self {
        Self { stream, f }
    }
}

impl<S, F> Stream for Inspect<S, F>
where
    S: Stream + Unpin,
    F: FnMut(&S::Item) + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let next = Pin::new(&mut self.stream).poll_next(cx);
        if let Poll::Ready(Some(ref item)) = next {
            (self.f)(item);
        }
        next
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

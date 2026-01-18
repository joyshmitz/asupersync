//! Enumerate combinator.

use super::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Stream for the [`enumerate`](super::StreamExt::enumerate) method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Enumerate<S> {
    stream: S,
    count: usize,
}

impl<S> Enumerate<S> {
    pub(crate) fn new(stream: S) -> Self {
        Self { stream, count: 0 }
    }
}

impl<S: Stream + Unpin> Stream for Enumerate<S> {
    type Item = (usize, S::Item);

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(item)) => {
                let index = self.count;
                self.count += 1;
                Poll::Ready(Some((index, item)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

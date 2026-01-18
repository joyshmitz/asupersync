//! Skip combinator.

use super::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Stream for the [`skip`](super::StreamExt::skip) method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct Skip<S> {
    stream: S,
    remaining: usize,
}

impl<S> Skip<S> {
    pub(crate) fn new(stream: S, remaining: usize) -> Self {
        Self { stream, remaining }
    }
}

impl<S: Stream + Unpin> Stream for Skip<S> {
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        while self.remaining > 0 {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(_)) => self.remaining -= 1,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }

        Pin::new(&mut self.stream).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        let lower = lower.saturating_sub(self.remaining);
        let upper = upper.map(|x| x.saturating_sub(self.remaining));
        (lower, upper)
    }
}

/// Stream for the [`skip_while`](super::StreamExt::skip_while) method.
#[derive(Debug)]
#[must_use = "streams do nothing unless polled"]
pub struct SkipWhile<S, F> {
    stream: S,
    predicate: F,
    done: bool,
}

impl<S, F> SkipWhile<S, F> {
    pub(crate) fn new(stream: S, predicate: F) -> Self {
        Self {
            stream,
            predicate,
            done: false,
        }
    }
}

impl<S, F> Stream for SkipWhile<S, F>
where
    S: Stream + Unpin,
    F: FnMut(&S::Item) -> bool + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Pin::new(&mut self.stream).poll_next(cx);
        }

        loop {
            match Pin::new(&mut self.stream).poll_next(cx) {
                Poll::Ready(Some(item)) => {
                    if !(self.predicate)(&item) {
                        self.done = true;
                        return Poll::Ready(Some(item));
                    }
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lower, upper) = self.stream.size_hint();
        if self.done {
            (lower, upper)
        } else {
            (0, upper)
        }
    }
}

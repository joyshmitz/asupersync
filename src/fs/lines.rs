//! Async line iterator for files.

use crate::fs::buf_reader::BufReader;
use crate::io::{AsyncRead, AsyncBufRead};
use crate::stream::Stream;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Iterator over the lines of an instance of `BufReader`.
#[derive(Debug)]
pub struct Lines<R> {
    pub(crate) reader: BufReader<R>,
}

impl<R: AsyncRead + Unpin> Stream for Lines<R> {
    type Item = io::Result<String>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut line = String::new();
        match Pin::new(&mut self.reader).poll_read_line(cx, &mut line) {
            Poll::Ready(Ok(0)) => Poll::Ready(None),
            Poll::Ready(Ok(_)) => {
                if line.ends_with('\n') {
                    line.pop();
                    if line.ends_with('\r') {
                        line.pop();
                    }
                }
                Poll::Ready(Some(Ok(line)))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}
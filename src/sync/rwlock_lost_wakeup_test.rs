use asupersync::sync::RwLock;
use asupersync::cx::Cx;
use asupersync::types::{RegionId, TaskId, Budget};
use asupersync::util::ArenaIndex;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::pin::Pin;

fn test_cx() -> Cx {
    Cx::new(
        RegionId::from_arena(ArenaIndex::new(0, 0)),
        TaskId::from_arena(ArenaIndex::new(0, 0)),
        Budget::INFINITE,
    )
}

fn poll_once<T>(future: &mut (impl Future<Output = T> + Unpin)) -> Option<T> {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    match std::pin::Pin::new(future).poll(&mut cx) {
        Poll::Ready(v) => Some(v),
        Poll::Pending => None,
    }
}

pub fn main() {
    let lock = Arc::new(RwLock::new(0_u32));
    let cx = test_cx();
    
    // 1. Writer 1 acquires lock
    let mut w1_fut = lock.write(&cx);
    let w1_guard = poll_once(&mut w1_fut).unwrap().unwrap();
    
    // 2. Reader 1 waits (queued in reader_waiters)
    let mut r1_fut = lock.read(&cx);
    assert!(poll_once(&mut r1_fut).is_none());
    
    // 3. Writer 1 releases. writer_waiters == 0, so it drains reader_waiters and wakes r1_fut.
    drop(w1_guard);
    
    // 4. Writer 2 calls write(). Since reader hasn't acquired yet, readers == 0, 
    // but the lock was PRE-GRANTED to the readers by Writer 1's release.
    let mut w2_fut = lock.write(&cx);
    // Writer 2 poll: `can_acquire` evaluates to false because readers were pre-granted
    // the lock (state.readers > 0) or state.writer_active was kept logically correct.
    // Actually, state.readers was pre-incremented by release_writer!
    // So Writer 2 WILL NOT ACQUIRE the lock!
    let w2_res = poll_once(&mut w2_fut);
    println!("Writer 2 result: {:?}", w2_res.is_some());
    assert!(!w2_res.is_some(), "Writer 2 should queue instead of stealing the lock!");
}

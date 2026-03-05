use crate::cx::Cx;
use crate::sync::RwLock;
use crate::types::{Budget, RegionId, TaskId};
use crate::util::ArenaIndex;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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

#[test]
fn test_my_lost_wakeup() {
    let lock = Arc::new(RwLock::new(0_u32));
    let cx = test_cx();

    // 1. Reader 1 acquires lock
    let mut r1_fut = lock.read(&cx);
    let r1_guard = poll_once(&mut r1_fut).unwrap().expect("failed");

    // 2. Writer 1 and Writer 2 created, writer_waiters becomes 2
    let mut w1_fut = lock.write(&cx);
    let mut w2_fut = lock.write(&cx);

    // 3. Reader 1 releases.
    // writer_waiters == 2, writer_queue is empty.
    // It pops None. writer_active remains false.
    drop(r1_guard);

    // 4. Writer 1 and Writer 2 poll.
    // Since writer_waiters == 2, both will see can_acquire == false!
    let w1_res = poll_once(&mut w1_fut);
    let w2_res = poll_once(&mut w2_fut);

    assert!(w1_res.is_some() || w2_res.is_some(), "LOST WAKEUP! Deadlock!");
}

use super::*;
use std::sync::Arc;

#[test]
fn test_lost_notify_one_token_on_broadcast_and_drop() {
    let notify = Arc::new(Notify::new());

    let mut fut1 = notify.notified();

    // Register fut1
    let waker = std::task::Waker::from(std::sync::Arc::new(DummyWaker));
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(std::pin::Pin::new(&mut fut1).poll(&mut cx).is_pending());

    // Notify one - consumes the token and assigns to fut1
    notify.notify_one();

    // Broadcast - wakes everyone (there is no one else right now, but updates fut1's generation)
    notify.notify_waiters();

    // Drop fut1 - it should pass the baton because it consumed the notify_one!
    drop(fut1);

    // Now a NEW waiter comes in. It should immediately complete because the notify_one token
    // should have been stored (since there were no other waiters to pass the baton to).
    let mut fut3 = notify.notified();
    assert!(
        std::pin::Pin::new(&mut fut3).poll(&mut cx).is_ready(),
        "LOST WAKEUP! notify_one token was lost!"
    );
}

struct DummyWaker;
impl std::task::Wake for DummyWaker {
    fn wake(self: std::sync::Arc<Self>) {}
}

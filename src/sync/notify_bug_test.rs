use super::*;
use std::sync::Arc;
use std::task::Wake;

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}
fn noop_waker() -> Waker {
    Arc::new(NoopWaker).into()
}

#[test]
fn notify_one_lost_wakeup() {
    let notify = Arc::new(Notify::new());

    let mut fut1 = notify.notified();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    assert!(Pin::new(&mut fut1).poll(&mut cx).is_pending());

    notify.notify_one();

    // Now fut1 is notified.
    // A later broadcast should not manufacture a second permit.
    notify.notify_waiters();

    assert!(Pin::new(&mut fut1).poll(&mut cx).is_ready());

    // The notify_one token was consumed by fut1.
    let mut fut2 = notify.notified();
    let res = Pin::new(&mut fut2).poll(&mut cx);
    assert!(res.is_pending(), "notify_one token should not spill over");
}

#[test]
fn notify_one_then_broadcast_does_not_create_phantom_token() {
    let notify = Arc::new(Notify::new());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut1 = notify.notified();
    let mut fut2 = notify.notified();

    assert!(Pin::new(&mut fut1).poll(&mut cx).is_pending());
    assert!(Pin::new(&mut fut2).poll(&mut cx).is_pending());

    notify.notify_one();
    notify.notify_waiters();

    assert!(Pin::new(&mut fut1).poll(&mut cx).is_ready());
    assert!(Pin::new(&mut fut2).poll(&mut cx).is_ready());

    let stored = notify.stored_notifications.load(Ordering::Acquire);
    assert_eq!(stored, 0, "no extra stored token should remain");

    let mut fut3 = notify.notified();
    let res = Pin::new(&mut fut3).poll(&mut cx);
    assert!(
        res.is_pending(),
        "broadcast overlap must not create a future permit"
    );
}

#[test]
fn dropped_notify_one_then_broadcast_waiter_restores_token() {
    let notify = Arc::new(Notify::new());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);

    let mut fut1 = notify.notified();
    let mut fut2 = notify.notified();

    assert!(Pin::new(&mut fut1).poll(&mut cx).is_pending());
    assert!(Pin::new(&mut fut2).poll(&mut cx).is_pending());

    notify.notify_one();
    notify.notify_waiters();

    drop(fut1);

    assert!(Pin::new(&mut fut2).poll(&mut cx).is_ready());

    let stored = notify.stored_notifications.load(Ordering::Acquire);
    assert_eq!(
        stored, 1,
        "dropping an overlapped waiter MUST recreate a notify_one token"
    );

    let mut fut3 = notify.notified();
    let res = Pin::new(&mut fut3).poll(&mut cx);
    assert!(
        res.is_ready(),
        "drop after broadcast overlap MUST wake a future waiter"
    );
}

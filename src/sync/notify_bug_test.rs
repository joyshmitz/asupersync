use super::*;
use std::sync::Arc;
use std::task::Wake;

struct NoopWaker;
impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}
fn noop_waker() -> Waker { Arc::new(NoopWaker).into() }

#[test]
fn notify_one_lost_wakeup() {
    let notify = Arc::new(Notify::new());
    
    let mut fut1 = notify.notified();
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    
    assert!(Pin::new(&mut fut1).poll(&mut cx).is_pending());
    
    notify.notify_one();
    
    // Now fut1 is notified.
    // Call notify_waiters.
    notify.notify_waiters();
    
    // Poll fut1. It will see gen_changed and drop its notify_one token.
    assert!(Pin::new(&mut fut1).poll(&mut cx).is_ready());
    
    // The notify_one token should be passed on, right?
    // Since there are no other waiters, it should be stored!
    let mut fut2 = notify.notified();
    let res = Pin::new(&mut fut2).poll(&mut cx);
    assert!(res.is_ready(), "notify_one token was lost!");
}

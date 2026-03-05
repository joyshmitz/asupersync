use super::{WaiterEntry, WaiterSlab};
use std::task::Waker;

fn active_waiter(generation: u64) -> WaiterEntry {
    WaiterEntry {
        waker: Some(Waker::noop().clone()),
        notified: false,
        generation,
    }
}

#[test]
fn waiter_slab_reuses_valid_slot_after_tail_shrink_discards_stale_indices() {
    let mut slab = WaiterSlab::new();

    let first = slab.insert(active_waiter(0));
    let middle = slab.insert(active_waiter(0));
    let tail = slab.insert(active_waiter(0));
    assert_eq!((first, middle, tail), (0, 1, 2));
    assert_eq!(slab.active_count(), 3);

    // Removing the middle and then the tail leaves a stale tail index in the
    // free-slot list after shrinking. The next insert must discard that stale
    // index and reuse the still-valid middle slot instead of growing the vec.
    slab.remove(middle);
    slab.remove(tail);
    assert_eq!(slab.entries.len(), 1);
    assert_eq!(slab.active_count(), 1);

    let reused = slab.insert(active_waiter(1));
    assert_eq!(reused, 1);
    assert_eq!(slab.entries.len(), 2);
    assert_eq!(slab.active_count(), 2);
}

//! Default benchmark definitions.

use crate::bench::{BenchCategory, Benchmark};
use crate::benchmark;
use crate::RuntimeInterface;
use std::time::{Duration, Instant};

/// Default benchmark set for conformance runtime comparisons.
pub fn default_benchmarks<R: RuntimeInterface>() -> Vec<Benchmark<R>> {
    vec![
        benchmark! {
            id: "bench-spawn-001",
            name: "Task spawn throughput",
            description: "Measure task spawn rate (tasks/second)",
            category: BenchCategory::TaskSpawn,
            warmup: 100,
            iterations: 1000,
            bench: |rt| {
                let start = Instant::now();
                rt.block_on(async {
                    let task_count = 100usize;
                    let mut handles = Vec::with_capacity(task_count);
                    for i in 0..task_count {
                        handles.push(rt.spawn(async move { i }));
                    }
                    for handle in handles {
                        let _ = handle.await;
                    }
                });
                start.elapsed()
            }
        },
        benchmark! {
            id: "bench-channel-001",
            name: "MPSC channel throughput",
            description: "Messages per second through bounded channel",
            category: BenchCategory::ChannelThroughput,
            warmup: 50,
            iterations: 200,
            bench: |rt| {
                let start = Instant::now();
                rt.block_on(async {
                    let message_count = 10_000usize;
                    let (tx, mut rx) = rt.mpsc_channel(message_count.min(1024));

                    let sender = rt.spawn(async move {
                        for i in 0..message_count {
                            if tx.send(i).await.is_err() {
                                break;
                            }
                        }
                    });

                    let receiver = rt.spawn(async move {
                        let mut received = 0usize;
                        while let Some(_) = rx.recv().await {
                            received += 1;
                            if received >= message_count {
                                break;
                            }
                        }
                        received
                    });

                    let _ = sender.await;
                    let _ = receiver.await;
                });
                start.elapsed()
            }
        },
        benchmark! {
            id: "bench-sleep-001",
            name: "Sleep accuracy",
            description: "Measure actual sleep duration vs requested",
            category: BenchCategory::TimerAccuracy,
            warmup: 50,
            iterations: 200,
            bench: |rt| {
                let requested = Duration::from_millis(1);
                let start = Instant::now();
                rt.block_on(async {
                    rt.sleep(requested).await;
                });
                let actual = start.elapsed();
                if actual > requested {
                    actual - requested
                } else {
                    requested - actual
                }
            }
        },
    ]
}

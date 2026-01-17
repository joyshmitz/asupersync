//! Benchmark runner for conformance performance comparisons.

use crate::bench::report::{render_console_summary, write_html_report, write_json_report};
use crate::bench::stats::{Comparison, Stats};
use crate::bench::{BenchCategory, Benchmark};
use crate::logging::{LogCollector, LogEntry, LogLevel};
use crate::RuntimeInterface;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Benchmark runner configuration.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Extra warmup multiplier beyond the benchmark spec.
    pub warmup_multiplier: f32,
    /// Minimum samples regardless of the benchmark spec.
    pub min_samples: u32,
    /// Maximum time per benchmark (0 = no limit).
    pub max_time: Duration,
    /// Minimum log level to record.
    pub log_level: LogLevel,
    /// Output options for summaries.
    pub output: BenchOutput,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            warmup_multiplier: 1.0,
            min_samples: 10,
            max_time: Duration::from_secs(5),
            log_level: LogLevel::Info,
            output: BenchOutput::None,
        }
    }
}

/// Output targets for benchmark summaries.
#[derive(Debug, Clone)]
pub enum BenchOutput {
    /// No output side effects.
    None,
    /// Render a console-friendly summary string.
    Console,
    /// Write a JSON report to the provided path.
    Json(PathBuf),
    /// Write an HTML report to the provided path.
    Html(PathBuf),
    /// Write both JSON and HTML reports.
    All { json: PathBuf, html: PathBuf },
}

/// Result of running a single benchmark for one runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchRunResult {
    pub benchmark_id: String,
    pub benchmark_name: String,
    pub category: BenchCategory,
    pub samples: Vec<Duration>,
    pub stats: Option<Stats>,
    pub error: Option<String>,
    pub logs: Vec<LogEntry>,
}

/// Summary of a benchmark run for one runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchRunSummary {
    pub runtime_name: String,
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub duration_ms: u64,
    pub results: Vec<BenchRunResult>,
    pub console_summary: Option<String>,
}

impl BenchRunSummary {
    /// Create an empty summary.
    pub fn new(runtime_name: impl Into<String>) -> Self {
        Self {
            runtime_name: runtime_name.into(),
            total: 0,
            completed: 0,
            failed: 0,
            duration_ms: 0,
            results: Vec::new(),
            console_summary: None,
        }
    }
}

/// Result of comparing two runtimes on the same benchmark.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchComparisonResult {
    pub benchmark_id: String,
    pub benchmark_name: String,
    pub category: BenchCategory,
    pub runtime_a: BenchRunResult,
    pub runtime_b: BenchRunResult,
    pub comparison: Option<Comparison>,
}

/// Summary of benchmark comparison between two runtimes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchComparisonSummary {
    pub runtime_a_name: String,
    pub runtime_b_name: String,
    pub total: usize,
    pub compared: usize,
    pub failed: usize,
    pub duration_ms: u64,
    pub results: Vec<BenchComparisonResult>,
}

/// Benchmark runner for a single runtime implementation.
pub struct BenchRunner<'a, R: RuntimeInterface> {
    runtime: &'a R,
    runtime_name: String,
    config: BenchConfig,
}

impl<'a, R: RuntimeInterface> BenchRunner<'a, R> {
    /// Create a new benchmark runner.
    pub fn new(runtime: &'a R, runtime_name: impl Into<String>, config: BenchConfig) -> Self {
        Self {
            runtime,
            runtime_name: runtime_name.into(),
            config,
        }
    }

    /// Run all benchmarks and return a summary.
    pub fn run_all(&self, benchmarks: &[Benchmark<R>]) -> BenchRunSummary {
        let start = Instant::now();
        let mut summary = BenchRunSummary::new(self.runtime_name.clone());
        summary.total = benchmarks.len();

        for bench in benchmarks {
            let result = self.run_single(bench);
            if result.error.is_some() {
                summary.failed += 1;
            } else {
                summary.completed += 1;
            }
            summary.results.push(result);
        }

        summary.duration_ms = start.elapsed().as_millis() as u64;

        match &self.config.output {
            BenchOutput::None => {}
            BenchOutput::Console => {
                summary.console_summary = Some(render_console_summary(&summary));
            }
            BenchOutput::Json(path) => {
                if let Err(err) = write_json_report(&summary, path) {
                    summary.console_summary = Some(format!(
                        "Failed to write JSON report to {:?}: {}",
                        path, err
                    ));
                }
            }
            BenchOutput::Html(path) => {
                if let Err(err) = write_html_report(&summary, path) {
                    summary.console_summary = Some(format!(
                        "Failed to write HTML report to {:?}: {}",
                        path, err
                    ));
                }
            }
            BenchOutput::All { json, html } => {
                if let Err(err) = write_json_report(&summary, json) {
                    summary.console_summary = Some(format!(
                        "Failed to write JSON report to {:?}: {}",
                        json, err
                    ));
                }
                if let Err(err) = write_html_report(&summary, html) {
                    summary.console_summary = Some(format!(
                        "Failed to write HTML report to {:?}: {}",
                        html, err
                    ));
                }
            }
        }

        summary
    }

    fn run_single(&self, bench: &Benchmark<R>) -> BenchRunResult {
        let mut collector = LogCollector::new(self.config.log_level);
        collector.start();
        collector.info(format!("Starting benchmark {}", bench.id));

        let warmup = scaled_warmup(bench.warmup, self.config.warmup_multiplier);
        for _ in 0..warmup {
            let _ = (bench.bench_fn)(self.runtime);
        }

        let mut samples = Vec::new();
        let mut error = None;
        let min_samples = self.config.min_samples.max(1);
        let target_samples = bench.iterations.max(min_samples);
        let start = Instant::now();

        for i in 0..target_samples {
            let duration = (bench.bench_fn)(self.runtime);
            collector.debug(format!(
                "sample {} duration_us={} benchmark_id={}",
                i,
                duration.as_micros(),
                bench.id
            ));
            samples.push(duration);

            if self.config.max_time != Duration::ZERO
                && samples.len() >= min_samples as usize
                && start.elapsed() >= self.config.max_time
            {
                collector.warn(format!(
                    "Reached max time {:?} after {} samples for {}",
                    self.config.max_time,
                    samples.len(),
                    bench.id
                ));
                break;
            }
        }

        let stats = match Stats::from_samples(&samples) {
            Ok(stats) => {
                if stats.cv() > 0.5 {
                    collector.warn(format!(
                        "High variance detected (cv={:.2}) for {}",
                        stats.cv(),
                        bench.id
                    ));
                }
                Some(stats)
            }
            Err(err) => {
                error = Some(err.to_string());
                collector.error(format!(
                    "Failed to compute stats for {}: {}",
                    bench.id, err
                ));
                None
            }
        };

        collector.info(format!("Benchmark {} complete", bench.id));

        BenchRunResult {
            benchmark_id: bench.id.to_string(),
            benchmark_name: bench.name.to_string(),
            category: bench.category,
            samples,
            stats,
            error,
            logs: collector.drain(),
        }
    }
}

/// Run comparison between two runtimes.
pub fn run_benchmark_comparison<RTA: RuntimeInterface, RTB: RuntimeInterface>(
    runtime_a: &RTA,
    runtime_a_name: &str,
    runtime_b: &RTB,
    runtime_b_name: &str,
    benches_a: &[Benchmark<RTA>],
    benches_b: &[Benchmark<RTB>],
    config: BenchConfig,
) -> BenchComparisonSummary {
    let start = Instant::now();
    let mut summary = BenchComparisonSummary {
        runtime_a_name: runtime_a_name.to_string(),
        runtime_b_name: runtime_b_name.to_string(),
        total: 0,
        compared: 0,
        failed: 0,
        duration_ms: 0,
        results: Vec::new(),
    };

    let benches_a_map: HashMap<&str, &Benchmark<RTA>> =
        benches_a.iter().map(|b| (b.id, b)).collect();
    let benches_b_map: HashMap<&str, &Benchmark<RTB>> =
        benches_b.iter().map(|b| (b.id, b)).collect();

    let common_ids: Vec<&str> = benches_a_map
        .keys()
        .filter(|id| benches_b_map.contains_key(*id))
        .copied()
        .collect();

    let runner_a = BenchRunner::new(runtime_a, runtime_a_name, config.clone());
    let runner_b = BenchRunner::new(runtime_b, runtime_b_name, config.clone());

    summary.total = common_ids.len();

    for id in common_ids {
        let bench_a = benches_a_map[id];
        let bench_b = benches_b_map[id];

        let result_a = runner_a.run_single(bench_a);
        let result_b = runner_b.run_single(bench_b);

        let comparison = match (&result_a.stats, &result_b.stats) {
            (Some(a), Some(b)) => Some(Comparison::compute(a, b)),
            _ => None,
        };

        if result_a.error.is_some() || result_b.error.is_some() {
            summary.failed += 1;
        } else {
            summary.compared += 1;
        }

        summary.results.push(BenchComparisonResult {
            benchmark_id: bench_a.id.to_string(),
            benchmark_name: bench_a.name.to_string(),
            category: bench_a.category,
            runtime_a: result_a,
            runtime_b: result_b,
            comparison,
        });
    }

    summary.duration_ms = start.elapsed().as_millis() as u64;
    summary
}

fn scaled_warmup(base: u32, multiplier: f32) -> u32 {
    if multiplier <= 0.0 || !multiplier.is_finite() || base == 0 {
        return 0;
    }
    let scaled = f32::from(base) * multiplier;
    if !scaled.is_finite() || scaled <= 0.0 {
        return 0;
    }
    if scaled >= u32::MAX as f32 {
        return u32::MAX;
    }
    scaled.round() as u32
}

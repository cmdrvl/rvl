// Opt-in runtime harness. Run with: cargo bench --bench runtime
use std::hint::black_box;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use rvl::cli::args::Args;
use rvl::orchestrator;

struct Case {
    name: &'static str,
    old: &'static str,
    new: &'static str,
    key: Option<&'static str>,
}

fn main() {
    let iterations = env_u64("RVL_RUNTIME_ITERS", 50);
    let warmup = env_u64("RVL_RUNTIME_WARMUP", 3);
    let budget_ms = env_f64("RVL_RUNTIME_BUDGET_MS");

    println!("rvl runtime harness");
    println!("iterations={iterations} warmup={warmup}");
    if let Some(budget) = budget_ms {
        println!("budget_ms={budget}");
    }

    let cases = [
        Case {
            name: "row_order_basic",
            old: "tests/fixtures/corpus/basic_old.csv",
            new: "tests/fixtures/corpus/basic_new.csv",
            key: None,
        },
        Case {
            name: "key_basic",
            old: "tests/fixtures/corpus/basic_old.csv",
            new: "tests/fixtures/corpus/basic_new.csv",
            key: Some("id"),
        },
    ];

    let mut failed = false;
    for case in &cases {
        let avg_ms = run_case(case, iterations, warmup);
        if let Some(budget) = budget_ms
            && avg_ms > budget
        {
            eprintln!(
                "budget exceeded for {}: avg_ms={:.3} budget_ms={:.3}",
                case.name, avg_ms, budget
            );
            failed = true;
        }
    }

    if failed {
        std::process::exit(1);
    }
}

fn run_case(case: &Case, iterations: u64, warmup: u64) -> f64 {
    let args = Args {
        old: PathBuf::from(case.old),
        new: PathBuf::from(case.new),
        key: case.key.map(|value| value.to_string()),
        threshold: 0.95,
        tolerance: 1e-9,
        delimiter: None,
        json: false,
    };

    for _ in 0..warmup {
        let result = orchestrator::run(&args).expect("warmup run failed");
        black_box(result);
    }

    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        let result = orchestrator::run(&args).expect("timed run failed");
        black_box(result);
        total += start.elapsed();
    }

    let total_ms = total.as_secs_f64() * 1000.0;
    let avg_ms = if iterations == 0 {
        0.0
    } else {
        total_ms / iterations as f64
    };

    println!(
        "case {}: avg_ms={:.3} total_ms={:.3}",
        case.name, avg_ms, total_ms
    );

    avg_ms
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| *value > 0.0)
}

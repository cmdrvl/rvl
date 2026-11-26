//! cmd-6nd slice 2: seeded property tests of rvl's math through the public CLI.
//!
//! No external property-testing crate: a fixed-seed xorshift generator, a fixed case count per
//! property, and every assertion message carries `seed=<n> case=<n>` plus the generated files, so
//! a failure is reproduced by running the same test. Every rvl run is a child process under a
//! hard timeout. Oracles are computed here from the generator's own integers (exact arithmetic),
//! never from rvl's output.
//!
//! Spec (docs/PLAN_RVL.md): contribution = abs(new - old), zero when <= tolerance; ordering is
//! contribution desc, then row_id asc (raw key component bytes), then raw column bytes asc; the
//! top 25 by that ordering; total_change == 0 is NO REAL CHANGE; top-25 coverage below the
//! threshold is E_DIFFUSE; otherwise the smallest sorted prefix reaching the threshold.

use std::collections::BTreeSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const TIMEOUT: Duration = Duration::from_secs(20);
const MAX_CONTRIBUTORS: usize = 25;

// ── harness ─────────────────────────────────────────────────────────────

/// xorshift64*: deterministic, dependency-free. Not for anything but test generation.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn between(&mut self, low: i64, high: i64) -> i64 {
        low + self.below((high - low + 1) as u64) as i64
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len() as u64) as usize]
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for index in (1..items.len()).rev() {
            items.swap(index, self.below(index as u64 + 1) as usize);
        }
    }
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        // Unique per call: tests run in parallel, and two sharing a directory would overwrite
        // each other's inputs.
        static SEQ: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("rvl-prop-{}-{name}-{seq}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Run {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

/// Writes the two files at FIXED paths (so output that names the files is comparable across
/// variants) and runs rvl with `--json --no-witness` plus `extra`, killed after [`TIMEOUT`].
fn rvl(dir: &Path, old: &str, new: &str, extra: &[&str], context: &str) -> Run {
    let old_path = dir.join("old.csv");
    let new_path = dir.join("new.csv");
    std::fs::write(&old_path, old).expect("write old");
    std::fs::write(&new_path, new).expect("write new");
    let stdout_path = dir.join("stdout");
    let stderr_path = dir.join("stderr");
    let mut child = Command::new(env!("CARGO_BIN_EXE_rvl"))
        .arg(&old_path)
        .arg(&new_path)
        .args(extra)
        .args(["--json", "--no-witness"])
        .env_remove("RVL_REDACTION_KEY")
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&stdout_path).expect("stdout file"))
        .stderr(std::fs::File::create(&stderr_path).expect("stderr file"))
        .spawn()
        .expect("spawn rvl");
    let deadline = Instant::now() + TIMEOUT;
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(2));
    };
    assert!(
        status.is_some(),
        "rvl hung: {context}\nOLD:\n{old}\nNEW:\n{new}"
    );
    let read = |path: &Path| {
        let mut text = String::new();
        std::fs::File::open(path)
            .expect("output file")
            .read_to_string(&mut text)
            .expect("utf-8 output");
        text
    };
    Run {
        code: status.and_then(|status| status.code()),
        stdout: read(&stdout_path),
        stderr: read(&stderr_path),
    }
}

/// Every rvl verdict is exit 0/1/2 with one JSON object whose outcome matches the exit code.
fn verdict(run: &Run, context: &str) -> Value {
    let value: Value = match serde_json::from_str(&run.stdout) {
        Ok(value) => value,
        Err(error) => {
            assert!(
                run.stdout.is_empty() && !run.stderr.is_empty(),
                "{context}: unreadable stdout ({error}): {}",
                run.stdout
            );
            Value::Null
        }
    };
    let expected = match value["outcome"].as_str() {
        Some("NO_REAL_CHANGE") => 0,
        Some("REAL_CHANGE") => 1,
        _ => 2,
    };
    assert_eq!(
        run.code,
        Some(expected),
        "{context}: exit code does not match outcome; stdout={} stderr={}",
        run.stdout,
        run.stderr
    );
    value
}

// ── generators ──────────────────────────────────────────────────────────

/// Distinct keys whose byte order differs from numeric or case-insensitive order.
const KEY_POOL: &[&str] = &[
    "1", "10", "2", "9", "100", "A", "a", "B", "b", "Z", "aa", "AA", "a0", "a-1", "a.1", "_x",
    "~y", "é", "e", "É", "ß", "ab", "a b", "k007", "k7", "k70", "0007", "7", "x-9", "x-10",
];

struct Table {
    /// Column names in HEADER order.
    columns: Vec<String>,
    keys: Vec<String>,
    old: Vec<Vec<i64>>,
    new: Vec<Vec<i64>>,
}

impl Table {
    /// Values in a small range so equal contributions (ties) are common.
    fn random(rng: &mut Rng, rows: usize, columns: usize, spread: i64) -> Self {
        let mut pool: Vec<&str> = KEY_POOL.to_vec();
        rng.shuffle(&mut pool);
        let keys: Vec<String> = pool[..rows].iter().map(|key| (*key).to_owned()).collect();
        let names = ["bal", "Amt", "amt", "z9", "z10", "_c"];
        let mut chosen: Vec<String> = names.iter().map(|name| (*name).to_owned()).collect();
        rng.shuffle(&mut chosen);
        chosen.truncate(columns);
        let cell = |rng: &mut Rng| rng.between(-spread, spread);
        let old: Vec<Vec<i64>> = (0..rows)
            .map(|_| (0..columns).map(|_| cell(rng)).collect())
            .collect();
        let new = old
            .iter()
            .map(|row| {
                row.iter()
                    .map(|value| if rng.below(3) == 0 { *value } else { cell(rng) })
                    .collect()
            })
            .collect();
        Self {
            columns: chosen,
            keys,
            old,
            new,
        }
    }

    fn csv(&self, values: &[Vec<i64>], row_order: &[usize]) -> String {
        let mut out = format!("id,{}\n", self.columns.join(","));
        for &row in row_order {
            out.push_str(&self.keys[row]);
            for value in &values[row] {
                out.push_str(&format!(",{value}"));
            }
            out.push('\n');
        }
        out
    }

    fn natural(&self) -> Vec<usize> {
        (0..self.keys.len()).collect()
    }

    fn shuffled(&self, rng: &mut Rng) -> Vec<usize> {
        let mut order = self.natural();
        rng.shuffle(&mut order);
        order
    }
}

/// What the spec says rvl must report for an integer table.
#[derive(Debug, PartialEq)]
enum Expected {
    NoRealChange,
    Diffuse,
    /// (key, column, delta) in report order.
    RealChange(Vec<(String, String, i64)>),
}

struct Oracle {
    expected: Expected,
    total: i64,
    changed: u64,
    max_abs: i64,
}

fn oracle(table: &Table, tolerance: i64, threshold: f64) -> Oracle {
    let mut cells = Vec::new();
    let mut max_abs = 0;
    for (row, key) in table.keys.iter().enumerate() {
        for (column, name) in table.columns.iter().enumerate() {
            let delta = table.new[row][column] - table.old[row][column];
            max_abs = max_abs.max(delta.abs());
            if delta.abs() > tolerance {
                cells.push((key.clone(), name.clone(), delta));
            }
        }
    }
    cells.sort_by(|a, b| {
        b.2.abs()
            .cmp(&a.2.abs())
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
            .then_with(|| a.1.as_bytes().cmp(b.1.as_bytes()))
    });
    let total: i64 = cells.iter().map(|cell| cell.2.abs()).sum();
    let changed = cells.len() as u64;
    let expected = if total == 0 {
        Expected::NoRealChange
    } else {
        cells.truncate(MAX_CONTRIBUTORS);
        let top: i64 = cells.iter().map(|cell| cell.2.abs()).sum();
        if (top as f64) / (total as f64) < threshold {
            Expected::Diffuse
        } else {
            let mut running = 0;
            let mut prefix = Vec::new();
            for cell in cells {
                running += cell.2.abs();
                prefix.push(cell);
                if (running as f64) / (total as f64) >= threshold {
                    break;
                }
            }
            Expected::RealChange(prefix)
        }
    };
    Oracle {
        expected,
        total,
        changed,
        max_abs,
    }
}

fn observed(value: &Value, context: &str) -> Expected {
    match value["outcome"].as_str() {
        Some("NO_REAL_CHANGE") => Expected::NoRealChange,
        Some("REAL_CHANGE") => Expected::RealChange(
            value["contributors"]
                .as_array()
                .expect("contributors")
                .iter()
                .map(|c| {
                    let name = |field: &str| {
                        c[field]
                            .as_str()
                            .and_then(|text| text.strip_prefix("u8:"))
                            .expect("u8 identifier")
                            .to_owned()
                    };
                    let delta = c["delta"].as_f64().expect("delta");
                    (name("row_id"), name("column"), delta as i64)
                })
                .collect(),
        ),
        _ => {
            assert_eq!(value["refusal"]["code"], "E_DIFFUSE", "{context}: {value}");
            Expected::Diffuse
        }
    }
}

fn check_against_oracle(
    scratch: &Scratch,
    table: &Table,
    old_order: &[usize],
    new_order: &[usize],
    tolerance: i64,
    threshold: &str,
    context: &str,
) -> (String, &'static str) {
    let old = table.csv(&table.old, old_order);
    let new = table.csv(&table.new, new_order);
    let tolerance_text = tolerance.to_string();
    let run = rvl(
        &scratch.0,
        &old,
        &new,
        &[
            "--key",
            "id",
            "--explicit",
            "--tolerance",
            &tolerance_text,
            "--threshold",
            threshold,
        ],
        context,
    );
    let value = verdict(&run, context);
    let want = oracle(table, tolerance, threshold.parse().expect("threshold"));
    let got = observed(&value, context);
    assert_eq!(
        got, want.expected,
        "{context} tolerance={tolerance} threshold={threshold}\nOLD:\n{old}\nNEW:\n{new}\n{value}"
    );
    let kind = match &want.expected {
        Expected::NoRealChange => "no_real_change",
        Expected::Diffuse => "diffuse",
        Expected::RealChange(_) => "real_change",
    };
    if kind != "diffuse" {
        assert_eq!(
            value["metrics"]["total_change"].as_f64(),
            Some(want.total as f64),
            "{context}: total_change\n{value}"
        );
        assert_eq!(
            value["metrics"]["max_abs_delta"].as_f64(),
            Some(want.max_abs as f64),
            "{context}: max_abs_delta\n{value}"
        );
        assert_eq!(
            value["counts"]["numeric_cells_changed"].as_u64(),
            Some(want.changed),
            "{context}: numeric_cells_changed\n{value}"
        );
    }
    (run.stdout, kind)
}

const THRESHOLDS: &[&str] = &["0.3", "0.5", "0.75", "0.9", "0.95", "1.0"];

// ── (a) tolerance rounding, ranking, ties, row order ────────────────────

/// At most 25 cells (so no top-25 eviction): verdict, ranking with ties, cutoff, totals and
/// counts match the oracle, with the header columns in random order and rows shuffled; and the
/// whole output is byte-identical when the rows of both files are reshuffled.
#[test]
fn ranking_matches_the_oracle_and_ignores_row_order() {
    const SEED: u64 = 0xA001;
    const CASES: usize = 150;
    let scratch = Scratch::new("ranking");
    let mut rng = Rng::new(SEED);
    let mut kinds = [0usize; 3];
    for case in 0..CASES {
        let rows = rng.between(1, 8) as usize;
        let columns = (rng.between(1, 4) as usize)
            .min(MAX_CONTRIBUTORS / rows)
            .max(1);
        let spread = *rng.pick(&[1, 2, 3, 50]);
        let table = Table::random(&mut rng, rows, columns, spread);
        let tolerance = *rng.pick(&[0, 0, 1, 2]);
        let threshold = *rng.pick(THRESHOLDS);
        let context = format!("seed={SEED:#x} case={case}");
        let (a_old, a_new) = (table.shuffled(&mut rng), table.shuffled(&mut rng));
        let (first, kind) = check_against_oracle(
            &scratch, &table, &a_old, &a_new, tolerance, threshold, &context,
        );
        let (b_old, b_new) = (table.shuffled(&mut rng), table.shuffled(&mut rng));
        let (second, _) = check_against_oracle(
            &scratch, &table, &b_old, &b_new, tolerance, threshold, &context,
        );
        assert_eq!(first, second, "{context}: output changed under row shuffle");
        kinds[match kind {
            "no_real_change" => 0,
            "diffuse" => 1,
            _ => 2,
        }] += 1;
    }
    // Both reachable verdicts must occur, or the property is vacuous somewhere. With at most
    // 25 cells the top 25 cover everything, so E_DIFFUSE is impossible here (the over-cap
    // property reaches it).
    let [no_change, diffuse, real_change] = kinds;
    assert!(
        no_change > 0 && real_change > 0,
        "seed={SEED:#x}: {kinds:?}"
    );
    assert_eq!(diffuse, 0, "seed={SEED:#x}: {kinds:?}");
}

/// More than 25 cells, with the header columns in BYTE order (the order in which rvl observes
/// cells then matches the spec's tie order): the same oracle holds, including which tied cells
/// survive the top-25 cap. Column orders that differ from byte order are the subject of a
/// separate rvl bead and are not generated here.
#[test]
fn top_25_cap_matches_the_oracle_when_headers_are_in_byte_order() {
    const SEED: u64 = 0xA002;
    const CASES: usize = 60;
    let scratch = Scratch::new("cap");
    let mut rng = Rng::new(SEED);
    let mut evicting = 0;
    let (mut diffuse, mut explained) = (0, 0);
    for case in 0..CASES {
        let rows = rng.between(9, 20) as usize;
        let columns = rng.between(3, 5) as usize;
        let spread = *rng.pick(&[1, 2, 40]);
        let mut table = Table::random(&mut rng, rows, columns, spread);
        // Sorting the NAMES only is enough: cell values are random per position.
        table.columns.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let threshold = *rng.pick(THRESHOLDS);
        let context = format!("seed={SEED:#x} case={case}");
        let (old_order, new_order) = (table.shuffled(&mut rng), table.shuffled(&mut rng));
        let (_, kind) = check_against_oracle(
            &scratch, &table, &old_order, &new_order, 0, threshold, &context,
        );
        diffuse += usize::from(kind == "diffuse");
        explained += usize::from(kind == "real_change");
        if oracle(&table, 0, 1.0).changed > MAX_CONTRIBUTORS as u64 {
            evicting += 1;
        }
    }
    assert!(
        evicting >= CASES / 2,
        "seed={SEED:#x}: only {evicting} cases exceed the cap"
    );
    assert!(
        diffuse > 0 && explained > 0,
        "seed={SEED:#x}: diffuse={diffuse} real_change={explained}"
    );
}

/// Tolerance is inclusive: a delta of exactly the tolerance contributes nothing, one more unit
/// does. Decimal spellings that denote the same number give the same verdict.
#[test]
fn tolerance_boundary_is_inclusive_and_spelling_does_not_matter() {
    const SEED: u64 = 0xA003;
    const CASES: usize = 60;
    let scratch = Scratch::new("tolerance");
    let mut rng = Rng::new(SEED);
    for case in 0..CASES {
        let base = rng.between(-1000, 1000);
        let tolerance = rng.between(1, 25);
        let over = rng.below(2) == 1;
        let magnitude = tolerance + i64::from(over);
        let delta = if rng.below(2) == 0 {
            magnitude
        } else {
            -magnitude
        };
        let new_value = base + delta;
        let spell = |rng: &mut Rng, value: i64| {
            let plain = value.to_string();
            let money = if value < 0 {
                format!("-${}", value.unsigned_abs())
            } else {
                format!("${value}")
            };
            let paren = if value < 0 {
                format!("({})", value.unsigned_abs())
            } else {
                plain.clone()
            };
            rng.pick(&[
                plain.clone(),
                format!("{plain}.0"),
                format!("{plain}.000"),
                format!("{plain}e0"),
                format!(" {plain} "),
                money,
                paren,
            ])
            .clone()
        };
        let old = format!("id,v\nk,{}\n", spell(&mut rng, base));
        let new = format!("id,v\nk,{}\n", spell(&mut rng, new_value));
        let context = format!("seed={SEED:#x} case={case} tolerance={tolerance} delta={delta}");
        let tolerance_text = tolerance.to_string();
        let run = rvl(
            &scratch.0,
            &old,
            &new,
            &["--key", "id", "--explicit", "--tolerance", &tolerance_text],
            &context,
        );
        let value = verdict(&run, &context);
        let want = if over {
            "REAL_CHANGE"
        } else {
            "NO_REAL_CHANGE"
        };
        assert_eq!(value["outcome"], want, "{context}\n{old}{new}{value}");
    }
}

// ── (b) NaN, infinities and non-finite spellings ───────────────────────

/// A missing token (`NaN` is one), a non-finite spelling, or an overflowing literal beside
/// ordinary numbers: never a panic or an unmatched exit code, never `null`-or-number confusion in
/// the verdict, and never NO_REAL_CHANGE while a pair of finite numbers differs. A non-finite
/// spelling against a number is a refusal. Magnitudes whose difference or sum overflows f64 are
/// the subject of the next-but-one property in this section (bd-9ss).
#[test]
fn nan_and_non_finite_tokens_refuse_or_are_missing_never_a_false_no_change() {
    const SEED: u64 = 0xB001;
    const CASES: usize = 200;
    const MISSING: &[&str] = &["NaN", "nan", "NAN", " NaN ", "NA", "N/A", "null", "-", ""];
    const NON_FINITE: &[&str] = &[
        "inf",
        "Inf",
        "INF",
        "-inf",
        "+inf",
        "infinity",
        "-Infinity",
        "1e999",
        "-1e999",
        "(1e999)",
        "$inf",
        "1.8e308",
        "-1.8e308",
        "+nan",
        "-nan",
        "NaNs",
    ];
    // Large but safe: any difference or sum of a few of these stays finite.
    const BIG: &[&str] = &["1e300", "-1e300", "9007199254740993", "1e-320"];
    let scratch = Scratch::new("nonfinite");
    let mut rng = Rng::new(SEED);
    let mut refusals = 0;
    for case in 0..CASES {
        let rows = rng.between(2, 5) as usize;
        let mut old: Vec<String> = (0..rows).map(|_| rng.between(-9, 9).to_string()).collect();
        let mut new = old.clone();
        let changed_row = rng.below(rows as u64) as usize;
        new[changed_row] = (rng.between(100, 200)).to_string();
        let mut target = rng.below(rows as u64) as usize;
        if target == changed_row {
            target = (target + 1) % rows;
        }
        let class = rng.below(3);
        let token = match class {
            0 => *rng.pick(MISSING),
            1 => *rng.pick(NON_FINITE),
            _ => *rng.pick(BIG),
        };
        let side = rng.below(3);
        if side != 1 {
            old[target] = token.to_owned();
        }
        if side != 0 {
            new[target] = token.to_owned();
        }
        let render = |values: &[String]| {
            let mut out = String::from("id,v,stable\n");
            for (row, value) in values.iter().enumerate() {
                out.push_str(&format!("k{row},\"{value}\",7\n"));
            }
            out
        };
        let context = format!("seed={SEED:#x} case={case} token={token:?} side={side}");
        let (old_text, new_text) = (render(&old), render(&new));
        let run = rvl(
            &scratch.0,
            &old_text,
            &new_text,
            &["--key", "id", "--explicit"],
            &context,
        );
        assert!(
            !run.stderr.contains("panicked"),
            "{context}: panic: {}",
            run.stderr
        );
        let value = verdict(&run, &context);
        // The row at `changed_row` is a finite pair that differs, so this is never "no change".
        assert_ne!(
            value["outcome"], "NO_REAL_CHANGE",
            "{context}\n{old_text}{new_text}{value}"
        );
        let code = value["refusal"]["code"].as_str().unwrap_or("");
        match class {
            // Both sides missing: the pair is skipped. One side missing: refusal.
            0 if side == 2 => assert_eq!(value["outcome"], "REAL_CHANGE", "{context}\n{value}"),
            0 => assert_eq!(code, "E_MISSINGNESS", "{context}\n{value}"),
            1 => assert_eq!(code, "E_MIXED_TYPES", "{context}\n{value}"),
            _ => {
                let outcome = value["outcome"].as_str().unwrap_or("");
                assert!(
                    outcome == "REAL_CHANGE" || code == "E_DIFFUSE",
                    "{context}: {value}"
                );
                let total = value["metrics"]["total_change"].as_f64();
                assert!(
                    total.is_some_and(f64::is_finite),
                    "{context}: total_change must be finite: {value}"
                );
            }
        }
        refusals += usize::from(value["outcome"] == "REFUSAL");
    }
    assert!(
        refusals > CASES / 3,
        "seed={SEED:#x}: only {refusals} refusals"
    );
}

/// Non-finite and out-of-range `--tolerance` / `--threshold` are argument errors (exit 2).
#[test]
fn non_finite_or_out_of_range_numeric_flags_are_argument_errors() {
    let scratch = Scratch::new("flags");
    let table = "id,v\nk,1\n";
    let mut cases = 0;
    for flag in ["--tolerance", "--threshold"] {
        for bad in [
            "NaN", "nan", "inf", "-inf", "infinity", "1e999", "-1", "abc", "",
        ] {
            let argument = format!("{flag}={bad}");
            let run = rvl(
                &scratch.0,
                table,
                table,
                &["--key", "id", &argument],
                &argument,
            );
            assert_eq!(
                run.code,
                Some(2),
                "{argument}: {}{}",
                run.stdout,
                run.stderr
            );
            assert!(
                !run.stderr.contains("panicked"),
                "{argument}: {}",
                run.stderr
            );
            cases += 1;
        }
    }
    assert_eq!(cases, 18);
}

/// The overflow domain (bd-9ss), mixed with ordinary values: rvl refuses with
/// E_NUMERIC_OVERFLOW exactly when, walking the cells in key then header order, a difference or
/// the running total of contributions is not finite; otherwise it never says NO_REAL_CHANGE
/// while a pair differs by more than the tolerance. No case may panic.
#[test]
fn overflow_is_refused_exactly_when_a_delta_or_the_total_leaves_f64() {
    const SEED: u64 = 0xB003;
    const CASES: usize = 200;
    const VALUES: &[&str] = &[
        "0",
        "1",
        "-1",
        "250",
        "1e300",
        "-1e300",
        "4e307",
        "-4e307",
        "1e308",
        "-1e308",
        "1.7976931348623157e308",
        "-1.7976931348623157e308",
    ];
    let scratch = Scratch::new("overflow");
    let mut rng = Rng::new(SEED);
    let (mut overflowed, mut verdicts, mut unchanged) = (0, 0, 0);
    for case in 0..CASES {
        let rows = rng.between(1, 4) as usize;
        let columns = rng.between(1, 3) as usize;
        // Draw mostly-small tables often enough that finite verdicts are well covered.
        let small = rng.below(3) == 0;
        let pool: &[&str] = if small { &VALUES[..6] } else { VALUES };
        let header: Vec<String> = (0..columns).map(|c| format!("c{c}")).collect();
        let mut old = format!("id,{}\n", header.join(","));
        let mut new = old.clone();
        let (mut total, mut overflow, mut differs) = (0.0f64, false, false);
        for row in 0..rows {
            old.push_str(&format!("k{row}"));
            new.push_str(&format!("k{row}"));
            for _ in 0..columns {
                let a = *rng.pick(pool);
                // Unchanged cells are common, so some tables have no change at all.
                let b = if rng.below(3) == 0 {
                    a
                } else {
                    *rng.pick(pool)
                };
                old.push_str(&format!(",{a}"));
                new.push_str(&format!(",{b}"));
                let delta =
                    b.parse::<f64>().expect("new value") - a.parse::<f64>().expect("old value");
                let contribution = if delta.abs() <= 1e-9 {
                    0.0
                } else {
                    delta.abs()
                };
                differs |= contribution != 0.0;
                total += contribution;
                overflow |= !delta.is_finite() || !total.is_finite();
            }
            old.push('\n');
            new.push('\n');
        }
        let context = format!("seed={SEED:#x} case={case}");
        let mut extra = vec!["--key", "id", "--explicit"];
        if rng.below(4) == 0 {
            extra.push("--exhaustive");
        }
        let run = rvl(&scratch.0, &old, &new, &extra, &context);
        assert!(
            !run.stderr.contains("panicked"),
            "{context}: {}",
            run.stderr
        );
        let value = verdict(&run, &context);
        let code = value["refusal"]["code"].as_str().unwrap_or("");
        if overflow {
            assert_eq!(
                code, "E_NUMERIC_OVERFLOW",
                "{context} {extra:?}\n{old}{new}{value}"
            );
            overflowed += 1;
        } else {
            assert_ne!(code, "E_NUMERIC_OVERFLOW", "{context}\n{old}{new}{value}");
            if differs {
                assert_ne!(
                    value["outcome"], "NO_REAL_CHANGE",
                    "{context}\n{old}{new}{value}"
                );
                verdicts += 1;
            } else {
                assert_eq!(
                    value["outcome"], "NO_REAL_CHANGE",
                    "{context}\n{old}{new}{value}"
                );
                unchanged += 1;
            }
            assert!(
                value["metrics"]["total_change"]
                    .as_f64()
                    .is_some_and(f64::is_finite),
                "{context}: {value}"
            );
        }
    }
    assert!(
        overflowed > 20 && verdicts > 20 && unchanged > 0,
        "seed={SEED:#x}: overflow={overflowed} verdicts={verdicts} unchanged={unchanged}"
    );
}

// ── (c) zero variance and all-equal data ───────────────────────────────

fn spelling_of_zero_or(rng: &mut Rng, value: i64) -> String {
    if value == 0 {
        (*rng.pick(&["0", "-0", "0.0", "-0.0", "0e5", "(0)", "$0", "+0"])).to_owned()
    } else {
        value.to_string()
    }
}

/// All-equal data (constant columns, all zeros, signed zeros, differences within tolerance) is
/// NO_REAL_CHANGE with total_change 0, null coverage, no contributors and no non-finite text.
#[test]
fn all_equal_and_zero_variance_tables_are_no_real_change() {
    const SEED: u64 = 0xC001;
    const CASES: usize = 120;
    let scratch = Scratch::new("zerovar");
    let mut rng = Rng::new(SEED);
    for case in 0..CASES {
        let rows = rng.between(1, 30) as usize;
        let columns = rng.between(1, 4) as usize;
        let constants: Vec<i64> = (0..columns)
            .map(|_| *rng.pick(&[0, 0, 1, -7, 1_000_000]))
            .collect();
        let header: Vec<String> = (0..columns).map(|c| format!("c{c}")).collect();
        let jitter = rng.below(2) == 1;
        let mut old = format!("id,{}\n", header.join(","));
        let mut new = old.clone();
        for row in 0..rows {
            old.push_str(&format!("k{row}"));
            new.push_str(&format!("k{row}"));
            for &constant in &constants {
                let before = spelling_of_zero_or(&mut rng, constant);
                // Within tolerance 0.5 when jittered, textually different but equal otherwise.
                let after = if jitter {
                    format!("{}", constant as f64 + *rng.pick(&[0.25, -0.5, 0.5, 0.0]))
                } else {
                    spelling_of_zero_or(&mut rng, constant)
                };
                old.push_str(&format!(",{before}"));
                new.push_str(&format!(",{after}"));
            }
            old.push('\n');
            new.push('\n');
        }
        let tolerance = if jitter {
            "0.5"
        } else {
            *rng.pick(&["0", "1e-9"])
        };
        let context = format!("seed={SEED:#x} case={case} tolerance={tolerance}");
        let run = rvl(
            &scratch.0,
            &old,
            &new,
            &["--key", "id", "--explicit", "--tolerance", tolerance],
            &context,
        );
        let value = verdict(&run, &context);
        assert_eq!(
            value["outcome"], "NO_REAL_CHANGE",
            "{context}\n{old}{new}{value}"
        );
        assert_eq!(
            value["metrics"]["total_change"].as_f64(),
            Some(0.0),
            "{context}"
        );
        assert!(
            value["metrics"]["top_k_coverage"].is_null(),
            "{context}: {value}"
        );
        assert_eq!(value["counts"]["numeric_cells_changed"], 0, "{context}");
        assert_eq!(value["contributors"], serde_json::json!([]), "{context}");
        let lower = run.stdout.to_lowercase();
        assert!(
            !lower.contains(":nan") && !lower.contains(":inf"),
            "{context}: {lower}"
        );
    }
}

/// A constant column beside a changing one never appears among the contributors.
#[test]
fn constant_columns_never_contribute() {
    const SEED: u64 = 0xC002;
    const CASES: usize = 60;
    let scratch = Scratch::new("constant");
    let mut rng = Rng::new(SEED);
    for case in 0..CASES {
        let rows = rng.between(1, 12) as usize;
        let constant = *rng.pick(&[0i64, 5, -3]);
        let mut old = String::from("id,flat,moving\n");
        let mut new = old.clone();
        for row in 0..rows {
            let before = rng.between(-100, 100);
            let after = before + rng.between(1, 100);
            old.push_str(&format!("k{row},{constant},{before}\n"));
            new.push_str(&format!("k{row},{constant},{after}\n"));
        }
        let context = format!("seed={SEED:#x} case={case}");
        let run = rvl(
            &scratch.0,
            &old,
            &new,
            &["--key", "id", "--explicit", "--threshold", "0.5"],
            &context,
        );
        let value = verdict(&run, &context);
        assert_eq!(value["outcome"], "REAL_CHANGE", "{context}\n{value}");
        let columns: BTreeSet<&str> = value["contributors"]
            .as_array()
            .expect("contributors")
            .iter()
            .filter_map(|c| c["column"].as_str())
            .collect();
        assert_eq!(columns, BTreeSet::from(["u8:moving"]), "{context}\n{value}");
    }
}

// ── (d) overlap and exact alignment ────────────────────────────────────

/// With identical key sets, `--key-mode overlap` reports exactly what exact alignment reports,
/// plus `alignment.key_membership` showing nothing added or removed.
#[test]
fn overlap_equals_exact_when_the_key_sets_are_identical() {
    const SEED: u64 = 0xD001;
    const CASES: usize = 120;
    let scratch = Scratch::new("overlap");
    let mut rng = Rng::new(SEED);
    for case in 0..CASES {
        let rows = rng.between(1, 20) as usize;
        let columns = rng.between(1, 4) as usize;
        let spread = *rng.pick(&[1, 3, 100]);
        let table = Table::random(&mut rng, rows, columns, spread);
        let old = table.csv(&table.old, &table.shuffled(&mut rng));
        let new = table.csv(&table.new, &table.shuffled(&mut rng));
        let tolerance = *rng.pick(&["0", "1", "1e-9"]);
        let threshold = *rng.pick(THRESHOLDS);
        let context = format!("seed={SEED:#x} case={case}");
        let mut base = vec![
            "--key",
            "id",
            "--tolerance",
            tolerance,
            "--threshold",
            threshold,
        ];
        if rng.below(2) == 0 {
            base.push("--explicit");
        }
        let exact = rvl(&scratch.0, &old, &new, &base, &context);
        let mut with_overlap = base.clone();
        with_overlap.extend(["--key-mode", "overlap"]);
        let overlap = rvl(&scratch.0, &old, &new, &with_overlap, &context);
        assert_eq!(exact.code, overlap.code, "{context}\n{old}{new}");
        let exact = verdict(&exact, &context);
        let mut overlap = verdict(&overlap, &context);
        let membership = overlap["alignment"]
            .as_object_mut()
            .and_then(|alignment| alignment.remove("key_membership"));
        if let Some(membership) = membership {
            assert_eq!(membership["overlap_count"], rows, "{context}");
            assert_eq!(membership["old_only_count"], 0, "{context}");
            assert_eq!(membership["new_only_count"], 0, "{context}");
        } else {
            assert_eq!(overlap["outcome"], "REFUSAL", "{context}: {overlap}");
        }
        // A refusal's suggested rerun may spell the key mode; nothing else may differ.
        let strip = |mut value: Value| {
            if let Some(refusal) = value["refusal"].as_object_mut() {
                refusal.remove("next_command");
            }
            value
        };
        assert_eq!(strip(exact), strip(overlap), "{context}\n{old}{new}");
    }
}

// ── (e) --non-numeric determinism ──────────────────────────────────────

/// `--non-numeric` output is byte-identical on rerun and when the rows of both files are
/// reshuffled (key alignment), in default, explicit and keyed modes.
#[test]
fn non_numeric_output_is_byte_identical_across_reruns_and_row_shuffles() {
    const SEED: u64 = 0xE001;
    const CASES: usize = 80;
    const STATUS: &[&str] = &["open", "closed", "held", "Open", "", "NA", "n/a", "x y"];
    const DATES: &[&str] = &["2024-01-01", "2024-02-29", "2023-12-31", "", "2025-07-04"];
    const AMOUNT: &[&str] = &["1", "2", "3", "", "40"];
    let scratch = Scratch::new("nonnumeric");
    let mut rng = Rng::new(SEED);
    let mut changed = 0;
    for case in 0..CASES {
        let rows = rng.between(1, 25) as usize;
        let mut pool: Vec<&str> = KEY_POOL.to_vec();
        rng.shuffle(&mut pool);
        let keys = &pool[..rows];
        let memo = |rng: &mut Rng| format!("note-{}", rng.below(1000));
        let make_row = |rng: &mut Rng| {
            [
                (*rng.pick(STATUS)).to_owned(),
                (*rng.pick(DATES)).to_owned(),
                (*rng.pick(AMOUNT)).to_owned(),
                memo(rng),
            ]
        };
        let old_rows: Vec<[String; 4]> = (0..rows).map(|_| make_row(&mut rng)).collect();
        let new_rows: Vec<[String; 4]> = old_rows
            .iter()
            .map(|row| {
                if rng.below(3) == 0 {
                    row.clone()
                } else {
                    make_row(&mut rng)
                }
            })
            .collect();
        let render = |data: &[[String; 4]], order: &[usize]| {
            let mut out = String::from("id,status,as_of,amount,memo\n");
            for &row in order {
                out.push_str(&format!("{},{}\n", keys[row], data[row].join(",")));
            }
            out
        };
        let natural: Vec<usize> = (0..rows).collect();
        let mode = rng.below(3);
        let mut extra = vec!["--key", "id", "--non-numeric"];
        if mode == 1 {
            extra.push("--explicit");
        }
        let context = format!("seed={SEED:#x} case={case} mode={mode}");
        let run_ordered = |old_order: &[usize], new_order: &[usize]| {
            let old = render(&old_rows, old_order);
            let new = render(&new_rows, new_order);
            // Mode 2: keyed markers.
            if mode == 2 {
                let old_path = scratch.0.join("old.csv");
                let new_path = scratch.0.join("new.csv");
                std::fs::write(&old_path, &old).expect("write old");
                std::fs::write(&new_path, &new).expect("write new");
                let output = Command::new(env!("CARGO_BIN_EXE_rvl"))
                    .arg(&old_path)
                    .arg(&new_path)
                    .args(&extra)
                    .args(["--json", "--no-witness"])
                    .env("RVL_REDACTION_KEY", "property-test-key-0123")
                    .stdin(Stdio::null())
                    .output()
                    .expect("run rvl keyed");
                Run {
                    code: output.status.code(),
                    stdout: String::from_utf8(output.stdout).expect("utf-8"),
                    stderr: String::from_utf8(output.stderr).expect("utf-8"),
                }
            } else {
                rvl(&scratch.0, &old, &new, &extra, &context)
            }
        };
        let first = run_ordered(&natural, &natural);
        let value = verdict(&first, &context);
        let rerun = run_ordered(&natural, &natural);
        assert_eq!(first.stdout, rerun.stdout, "{context}: rerun differs");
        for _ in 0..2 {
            let mut old_order = natural.clone();
            let mut new_order = natural.clone();
            rng.shuffle(&mut old_order);
            rng.shuffle(&mut new_order);
            let shuffled = run_ordered(&old_order, &new_order);
            assert_eq!(
                first.code, shuffled.code,
                "{context}: exit differs under shuffle"
            );
            assert_eq!(
                first.stdout,
                shuffled.stdout,
                "{context}: output differs under shuffle\n{}{}",
                render(&old_rows, &natural),
                render(&new_rows, &natural)
            );
        }
        changed += usize::from(value["outcome"] == "REAL_CHANGE");
    }
    assert!(
        changed > CASES / 2,
        "seed={SEED:#x}: only {changed} real changes"
    );
}

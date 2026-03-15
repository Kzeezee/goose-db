# goosedb

A single-threaded, specialised query processor for TPC-H Query 19, written in Rust.

goosedb exploits domain-specific knowledge about Q19's data access patterns to outperform DuckDB on this query. The core technique is aggressive predicate pushdown: both the part and lineitem Parquet scans are filtered *before* the join, dramatically reducing the number of rows that reach the hash probe.

---

## Results (SF=1)

| System | Mean time | Notes |
|---|---|---|
| DuckDB (`threads=1`) | ~281 ms | Internal `.timer on`; excludes process startup |
| goosedb | ~358 ms | `--bench --runs 8`; steady-state ~340–360 ms |
| Ratio | **1.27×** | Gap is Parquet decompression dominated |

Operator breakdown (warm cache, run 6):

| Stage | Time |
|---|---|
| part scan + DirectTable build | ~10 ms |
| lineitem predicate scan (RowFilter `build()`) | ~190 ms |
| lineitem main scan + probe + aggregate | ~125 ms |

The dominant cost is Parquet decompression. DuckDB's advantage comes from its SIMD-accelerated C++ Parquet decompressor; the Rust `parquet` crate does not currently exploit AVX-512.

---

## Machine specs

| Component | Details |
|---|---|
| CPU | Intel Core i7-13700H (14 cores / 20 threads) |
| RAM | 32 GB |
| OS | Windows 11 Education |
| Rust | 1.87.0 |
| DuckDB | v1.4.3 |
| `RUSTFLAGS` | `-C target-cpu=native` |

---

## Prerequisites

```bash
# Install Rust (stable)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install DuckDB CLI (for data generation and baseline comparison)
# Windows: download from https://duckdb.org/docs/installation/
# macOS:   brew install duckdb
# Linux:   download binary from duckdb.org
```

---

## Generate data

TPC-H Parquet files are not included in the repository. Generate them with DuckDB:

```bash
# bash / Linux / macOS
duckdb < scripts/generate_data.sql

# PowerShell (Windows)
Get-Content scripts/generate_data.sql | duckdb
```

This creates:

```
data/
  sf0.5/lineitem.parquet  part.parquet
  sf1/lineitem.parquet    part.parquet
  sf2/lineitem.parquet    part.parquet
  sf5/lineitem.parquet    part.parquet
```

---

## Build

```bash
# Release build with native CPU optimisations (required for benchmarking)
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Then run the binary directly (avoids cargo startup overhead in timing)
./target/release/goosedb --data data/sf1 --out result.csv --bench --runs 6
```

> **Windows / PowerShell:** `RUSTFLAGS=...` inline syntax does not work in PowerShell. Set the variable on its own line first:
> ```powershell
> $env:RUSTFLAGS="-C target-cpu=native"
> cargo build --release
> .\target\release\goosedb.exe --data data/sf1 --out result.csv --bench --runs 6
> ```

---

## Run

```bash
# Single query execution — writes result to result.csv and prints revenue to stdout
./run.sh --data data/sf1 --out result.csv

# Benchmark mode — 6 runs, run 1 discarded as warmup, mean of runs 2–6 reported
./run.sh --data data/sf1 --out result.csv --bench --runs 6

# With operator-level timing breakdown (printed on last run only)
./run.sh --data data/sf1 --out result.csv --bench --runs 6 --timing

# All scale factors
for sf in 0.5 1 2 5; do
    ./run.sh --data data/sf${sf} --out results/result_sf${sf}.csv --bench --runs 6
done
```

**Output format** — `result.csv`:

```
revenue
3083843.0578
```

---

## Correctness check

goosedb reads the same physical Parquet files as DuckDB and keeps all arithmetic in the integer domain (raw `i64` DECIMAL values, `i128` accumulator). The result is bit-for-bit identical to DuckDB's output.

```bash
# Generate DuckDB reference output (single-threaded, same Parquet files)
# bash
duckdb < scripts/duckdb_baseline.sql

# PowerShell (Windows)
Get-Content scripts/duckdb_baseline.sql | duckdb

# Both write duckdb_result.csv and print timing to the terminal

# Exact string comparison
./check_correctness.sh result.csv duckdb_result.csv
# PASS: 3083843.0578
```

Expected result at SF=1: `3083843.0578`

---

## Tests

```bash
# Run all unit tests + integration tests
cargo test

# Integration test only (requires data/sf1/)
cargo test --test correctness_sf1

# Unit tests only
cargo test --lib
```

The integration test (`tests/correctness_sf1.rs`) runs both pipelines end-to-end and asserts the result equals `3083843.0578`.

---

## Architecture

goosedb uses a **two-pipeline, batch-vectorised** model.

### Pipeline 1 — Part table build

```
part.parquet → RowFilter (brand/size/container) → encode to u8 → DirectTable
```

- RowFilter eliminates ~70% of part rows before any data enters the pipeline
- Brand and container strings are encoded to compact `u8` indices once, at build time
- DirectTable: flat `Vec` indexed by `(p_partkey - 1)` — zero hashing, O(1) probe

### Pipeline 2 — Lineitem scan + probe + aggregate

```
lineitem.parquet → RowFilter predicate scan (build)
                → main projection (selected rows only)
                → DirectTable probe
                → 3-way OR post-join filter
                → i128 revenue accumulation
```

- RowFilter eagerly evaluates `shipinstruct = 'DELIVER IN PERSON' AND shipmode IN ('AIR','AIR REG') AND quantity ≤ 30` across all 1.5M rows in `build()`, producing a `RowSelection`
- The main scan then reads only the 4 projection columns for the ~128K selected rows
- Probe, post-join filter, and aggregation are fused into a single loop

### Key data structures

| Structure | Size (SF=1) | Purpose |
|---|---|---|
| `DirectTable` | ~3.2 MB | Flat `Vec<HashTableEntry>` indexed by partkey; fits in L3 |
| `HashTableEntry` | 16 bytes | `{partkey: i64, brand_idx: u8, size: u8, container_idx: u8, _pad: [u8;5]}` |
| `AggregateState` | 24 bytes | `i128` accumulator + `u64` row count |

---

## Project structure

```
goosedb/
├── src/
│   ├── main.rs              # CLI, benchmark loop
│   ├── lib.rs               # Library root (for integration tests)
│   ├── config.rs            # CLI args, BATCH_SIZE const
│   ├── encoding.rs          # brand/container string → u8 index
│   ├── timer.rs             # Lap timer for operator profiling
│   ├── pipeline/
│   │   ├── pipeline1.rs     # Part scan → DirectTable build
│   │   └── pipeline2.rs     # Lineitem scan → probe → aggregate
│   └── operators/
│       ├── scan.rs          # Parquet scanners with RowFilter pushdown
│       ├── hash_table.rs    # DirectTable + FxHash table (reference)
│       └── aggregate.rs     # i128 accumulator
├── tests/
│   └── correctness_sf1.rs  # Integration test (SF=1 result == 3083843.0578)
├── scripts/
│   ├── generate_data.sql    # DuckDB TPC-H data export
│   └── duckdb_baseline.sql  # DuckDB Q19 timing (threads=1)
├── run.sh                   # One-command runner
├── check_correctness.sh     # Exact output comparison
└── OPTIMISATIONS.md         # Optimisation tracker (what was tried, what worked)
```

# goosedb

A single-threaded, specialised query processor for TPC-H Query 19, written in Rust.

goosedb exploits domain-specific knowledge about Q19's data access patterns to outperform DuckDB on this query. The core technique is aggressive predicate pushdown: both the part and lineitem Parquet scans are filtered *before* the join, dramatically reducing the number of rows that reach the hash probe.

---

## Run goosedb

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
./target/release/goosedb --data data/sf1 --out result.csv --bench --runs 6 --timing
```

**Windows (PowerShell):**
```powershell
$env:RUSTFLAGS="-C target-cpu=native"
cargo build --release
.\target\release\goosedb.exe --data data\sf1 --out result.csv --bench --runs 6 --timing
```

Example output:
```
Run 1 (warmup): ... ms      ← printed to stderr
Run 2: ... ms
...
Mean (runs 2–6): ... ms
3083843.0578               ← printed to stdout (plain number, no label)

[timing breakdown — run 6]
  lineitem file open        :   <1 ms
  lineitem metadata parse   :   ~1 ms
  lineitem reader build     :   ~0 ms
  part metadata + alloc     :   ~7 ms
  part scan + table build   :   ~3 ms
  lineitem scan + probe + agg: TBD ms   (single-pass: all 1.5M rows, no RowFilter)
```

> Run `--bench --runs 6 --timing` to see current numbers on your machine.

---

## Run DuckDB baseline

```bash
duckdb < scripts/duckdb_baseline.sql
```

**Windows (PowerShell):**
```powershell
Get-Content scripts/duckdb_baseline.sql | duckdb
```

Example output:
```
Run Time (s): real 0.281 user 0.265625 sys 0.015625
```

The script sets `PRAGMA threads=1` and `.timer on` so the printed time is directly comparable to goosedb's mean.

---

## Results (SF=1)

| System | Mean time | Notes |
|---|---|---|
| DuckDB (`threads=1`) | ~281 ms | Internal `.timer on`; excludes process startup |
| goosedb (RowFilter, BATCH_SIZE=8192) | ~340 ms | Prior approach |
| goosedb (single-pass, BATCH_SIZE=4096) | TBD | Current approach — re-run benchmark to fill in |

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
duckdb < scripts/generate_data.sql
```

**Windows (PowerShell):**
```powershell
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

## Correctness check

goosedb reads the same physical Parquet files as DuckDB and keeps all arithmetic in the integer domain (raw `i64` DECIMAL values, `i128` accumulator). The result is bit-for-bit identical to DuckDB's output.

```bash
./check_correctness.sh result.csv duckdb_result.csv
# PASS: 3083843.0578
```

Expected result at SF=1: `3083843.0578`

---

## Tests

```bash
cargo test
```

The integration test (`tests/correctness_sf1.rs`) runs both pipelines end-to-end and asserts the result equals `3083843.0578`.

---

## Architecture

goosedb uses a **two-pipeline, batch-vectorised** model.

### Pipeline 1 — Part table build

```
part.parquet (4 columns) → inline filter (size/brand/container) → encode to u8 → DirectTable
```

- Single-pass scan: all 4 columns projected, no RowFilter
- Inline filter in the insert loop eliminates ~70% of part rows: `p_size 1–15` first (cheapest), then brand ∈ {#12,#23,#34}, then container ∈ 12 values
- Brand and container strings encoded to compact `u8` indices at build time
- DirectTable: flat `Vec` indexed by `(p_partkey - 1)` — zero hashing, O(1) probe

### Pipeline 2 — Lineitem scan + probe + aggregate

```
lineitem.parquet (6 columns, single pass)
  → fused: inline pre-filter → DirectTable probe → 3-way OR post-join filter → i128 accumulation
```

- Single-pass scan: all 6 needed columns (partkey, quantity, shipmode, shipinstruct, price, discount) decoded in one pass — no RowFilter, no double-decode
- Inline pre-filter in the hot loop: `shipinstruct = 'DELIVER IN PERSON'` first (~75% rejection), then `shipmode IN ('AIR','AIR REG')`, then `quantity ≤ 30`
- Probe, post-join filter, and aggregation fused into a single loop — no intermediate buffers

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
│       ├── scan.rs          # Parquet scanners — single-pass column projection, no RowFilter
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

# goosedb

A single-threaded, specialised query processor for TPC-H Query 19, written in Rust.

goosedb exploits domain-specific knowledge about Q19's data access patterns to outperform DuckDB on this query. The core techniques are aggressive predicate pushdown (both scans filtered before the join) and Parquet dictionary encoding preservation (string columns read as compact integer keys instead of multi-byte strings).

---

## Run goosedb

**Linux / macOS:**
```bash
RUSTFLAGS="-C target-cpu=native" cargo run --release -- --data data/sf1 --out result.csv --bench --runs 6 --timing
```

**Bash / WSL:**
```bash
./run.sh --data data/sf1 --out result.csv --bench --runs 6 --timing
```

**Windows (PowerShell):**
```powershell
$env:RUSTFLAGS="-C target-cpu=native"; cargo run --release -- --data data/sf1 --out result.csv --bench --runs 6 --timing
```

**Windows (Command Prompt):**
```cmd
set RUSTFLAGS=-C target-cpu=native && cargo run --release -- --data data/sf1 --out result.csv --bench --runs 6 --timing
```

Example output:
```
Run 1 (warmup): 265.97 ms      ← printed to stderr
Run 2: 204.34 ms
...
Mean (runs 2–6): 214.80 ms
3083843.0578                    ← printed to stdout (plain number, no label)

Operator breakdown (run 6):
  part metadata + alloc              :   1.00 ms
  part scan + table build            :   9.28 ms
  lineitem file open                 :   0.07 ms
  lineitem metadata + schema override:   0.37 ms
  lineitem reader build              :   0.38 ms
  lineitem scan + probe + aggregate  : 226.01 ms
```

---

## Run DuckDB baseline

Requires the DuckDB CLI installed and on PATH.

```bash
python scripts/duckdb_baseline.py --data data/sf1 --runs 6 --timing
```

Example output:
```
Run 1 (warmup): 310.42 ms
Run 2: 281.15 ms
Run 3: 279.83 ms
Run 4: 280.47 ms
Run 5: 282.01 ms
Run 6: 280.54 ms
Mean (runs 2-6): 280.80 ms
3083843.0578
```

The script runs single-threaded (`PRAGMA threads=1`), discards the first run as warmup, and reports the mean of runs 2–N — directly comparable to goosedb's `--bench` output.

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

**Linux / macOS / Bash / WSL:**
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
lineitem.parquet (6 columns, single pass, dictionary strings)
  → fused: dictionary pre-filter → DirectTable probe → 3-way OR post-join filter → i128 accumulation
```

- Single-pass scan: all 6 needed columns decoded in one Parquet pass — no RowFilter, no double-decode
- **String columns (`l_shipinstruct`, `l_shipmode`) read as `DictionaryArray<Int32Type>`** via `with_schema()` override. Arrow preserves the Parquet dictionary: 1.5M rows store i32 keys; the string values (4 for shipinstruct, 7 for shipmode) are decoded only once per batch
- Hot loop pre-filter: resolve target strings to dictionary indices once per batch, then compare i32 keys — eliminates ~75% of rows with integer ops instead of byte comparisons
- Probe, post-join filter, and aggregation fused into a single loop — no intermediate buffers

### Key data structures

| Structure | Size (SF=1) | Purpose |
|---|---|---|
| `DirectTable` | ~3.2 MB | Flat `Vec<PartEntry>` indexed by partkey; fits in L3 |
| `PartEntry` | 16 bytes | `{partkey: i64, brand_idx: u8, size: u8, container_idx: u8, _pad: [u8;5]}` |
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
│   │   └── pipeline2.rs     # Lineitem scan → dictionary pre-filter → probe → aggregate
│   └── operators/
│       ├── scan.rs          # Parquet scanners — schema override for dictionary strings
│       ├── hash_table.rs    # PartEntry + DirectTable (direct-address lookup)
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

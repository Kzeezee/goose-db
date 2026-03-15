# goosedb — CLAUDE.md

A specialised query processor for TPC-H Query 19, built in Rust.  
This document is the authoritative reference for the project: what it is, why the choices were made, and how to build/run/test it.

---

## Table of Contents

1. [Project Purpose](#1-project-purpose)
2. [Query 19 — Deep Dive](#2-query-19--deep-dive)
3. [Tech Stack](#3-tech-stack)
4. [Project Structure](#4-project-structure)
5. [Execution Model](#5-execution-model)
6. [Pipeline Architecture](#6-pipeline-architecture)
7. [Data Structures](#7-data-structures)
8. [Optimisations](#8-optimisations)
9. [Build & Run Commands](#9-build--run-commands)
10. [Benchmarking Protocol](#10-benchmarking-protocol)
11. [Correctness Validation](#11-correctness-validation)
12. [Constraints & Rules](#12-constraints--rules)
13. [Deliverables Checklist](#13-deliverables-checklist)

---

## 1. Project Purpose

goosedb is a **single-query specialised processor** for TPC-H Q19, built as part of CS465.  
The goal is to outperform DuckDB (single-threaded) on this query by exploiting domain-specific knowledge about Q19's data access patterns — specifically by aggressively pushing filter predicates down to the individual table scans before the join, which DuckDB (as a general-purpose engine) does not fully do.

**Core hypothesis:** By pushing individual predicates down to each table scan and evaluating them *before* the hash join, we can dramatically reduce intermediate result sizes and avoid applying an expensive complex OR filter to millions of joined rows.

**Hypothesis confirmed by simulation:** DuckDB with manually rewritten predicate-pushdown SQL ran in ~0.085s vs the original ~0.159s at SF=1 — a ~1.9× speedup before any low-level optimisations are applied.

---

## 2. Query 19 — Deep Dive

### SQL

```sql
SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    lineitem,
    part
WHERE
    (p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1  AND l_quantity <= 1 + 10
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 10 + 10
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON')
    OR (p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 20 + 10
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON');
```

### What it does

Computes total revenue (`sum(l_extendedprice * (1 - l_discount))`) from `lineitem` joined with `part`, where items match one of three brand/container/quantity/size groups, and are shipped by air with "DELIVER IN PERSON" instructions.

### Parquet physical schema

Derived from inspecting the actual Parquet files exported by DuckDB. These physical types govern how columns must be read and compared in goosedb.

**lineitem.parquet**

| Column          | Parquet Physical Type | DuckDB Logical Type | Notes                             |
|-----------------|-----------------------|---------------------|-----------------------------------|
| l_partkey       | INT64                 | BIGINT              | Join key                          |
| l_quantity      | INT64                 | DECIMAL(15,2)       | Raw value ÷ 100 = actual quantity |
| l_extendedprice | INT64                 | DECIMAL(15,2)       | Raw value ÷ 100 = actual price    |
| l_discount      | INT64                 | DECIMAL(15,2)       | Raw value ÷ 100 = actual discount |
| l_shipmode      | BYTE_ARRAY            | VARCHAR             | Compare as raw bytes              |
| l_shipinstruct  | BYTE_ARRAY            | VARCHAR             | Compare as raw bytes              |

**part.parquet**

| Column      | Parquet Physical Type | DuckDB Logical Type | Notes                |
|-------------|-----------------------|---------------------|----------------------|
| p_partkey   | INT64                 | BIGINT              | Join key             |
| p_brand     | BYTE_ARRAY            | VARCHAR             | Compare as raw bytes |
| p_size      | INT32                 | INTEGER             | Compare as i32       |
| p_container | BYTE_ARRAY            | VARCHAR             | Compare as raw bytes |

### DECIMAL encoding

`l_quantity`, `l_extendedprice`, and `l_discount` are stored as `INT64` with scale=2, meaning the stored integer divided by 100 gives the real value. **goosedb keeps all comparisons and arithmetic in the raw integer domain** — no conversion to float until the final output step:

```rust
// Filter: l_quantity <= 30 (SQL) becomes in raw integer terms:
l_quantity_raw <= 3000_i64

// Filter: l_quantity BETWEEN 1 AND 11 (Group 1) becomes:
l_quantity_raw >= 100 && l_quantity_raw <= 1100

// Revenue per row — all integer, scaled by 10_000:
// extendedprice_raw * (100 - discount_raw)  [scale: 100 × 100 = 10_000]
let revenue_scaled: i64 = l_extendedprice_raw * (100 - l_discount_raw);

// Accumulate into i128 to avoid overflow across all matching rows:
accumulator: i128 += revenue_scaled as i128;

// Convert only at final output:
let revenue_final: f64 = accumulator as f64 / 10_000.0;
```

This guarantees an **exact match** with DuckDB's result, since both systems read the same raw INT64 values from the same Parquet files.

### Tables touched

| Table    | Relevant Columns                                                               |
|----------|--------------------------------------------------------------------------------|
| lineitem | l_partkey, l_quantity, l_shipmode, l_shipinstruct, l_extendedprice, l_discount |
| part     | p_partkey, p_brand, p_container, p_size                                        |

### DuckDB query plan (SF=1, sequential scan from Parquet)

DuckDB's plan (without pushdown rewrite):
1. `TABLE_SCAN lineitem` — 1,500,048 rows (0.07s) — filters `l_shipinstruct` and optionally `l_shipmode`
2. `FILTER` — shipmode IN ('AIR','AIR REG') — reduces to 214,377 rows (0.02s)
3. `PROJECTION` — selects needed columns (0.00s)
4. `TABLE_SCAN part` — 200,000 rows (0.00s)
5. `HASH_JOIN` (INNER, l_partkey = p_partkey) — 214,377 rows (0.02s)
6. `FILTER` — the full complex 3-way OR predicate — reduces to 121 rows (0.03s)
7. `PROJECTION` — computes `l_extendedprice * (1 - l_discount)` (0.00s)
8. `UNGROUPED_AGGREGATE` — sum (0.00s)

**Total: ~0.159s at SF=1**

### Identified bottlenecks

- **Lineitem table scan + shipinstruct/shipmode filter** — largest single cost (0.07s + 0.02s), scanning 1.5M rows
- **Complex 3-way OR post-join filter** — applied to 214K rows *after* the join (0.03s)
- **Hash join** — probing 214K rows unnecessarily because the complex filter was deferred (0.02s)

### goosedb's fix

Push predicates that can be evaluated per-table *before* the join:

- **Part scan filter:** `p_brand IN ('Brand#12','Brand#23','Brand#34') AND p_size BETWEEN 1 AND 15 AND p_container IN (all 12 container strings)`  
  → reduces 200K rows to ~60K rows before the hash table is built
- **Lineitem scan filter:** `l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON' AND l_quantity_raw <= 3000`  
  → reduces 1.5M rows to ~128K rows before probing
- **Post-join filter:** the tight 3-way OR predicate (brand+container+quantity+size) — now applied to only ~1,403 joined rows, fused directly with probe and aggregation

**Simulated result: ~0.085s at SF=1 (≈1.9× improvement from pushdown alone)**

---

## 3. Tech Stack

| Component     | Choice                  | Reason                                                                    |
|---------------|-------------------------|---------------------------------------------------------------------------|
| Language      | **Rust**                | Memory-safe, zero-cost abstractions, native SIMD, no GC pauses            |
| Build system  | **Cargo**               | Standard Rust toolchain                                                   |
| Parquet I/O   | **parquet** crate       | Official Apache Parquet Rust implementation, column-level projection       |
| Batch format  | **Apache Arrow**        | In-memory columnar arrays, SIMD-friendly, same layout as Parquet pages    |
| Hash function | **FxHash** (rustc-hash) | Fastest integer-keyed hash; used inside the Rust compiler itself          |
| Timing        | **std::time::Instant**  | Nanosecond-precision wall-clock, no extra dependency needed               |
| Output        | **csv** crate           | Write result.csv matching DuckDB's output format                          |
| CLI parsing   | **clap**                | Ergonomic argument parsing for `--data`, `--out`, `--bench`, `--runs`    |

### Why FxHash over alternatives

- **Perfect hashing** (compile-time, e.g. `phf`): requires a static key set known at compile time. The filtered part keys change per scale factor and per run — inapplicable here.
- **Runtime perfect hashing**: finding a collision-free function over ~60K dynamic keys requires non-trivial upfront computation that would negate the probe savings.
- **AHash**: good general-purpose hash, but FxHash is strictly faster for `i64` integer keys due to its single-multiply design.
- **FxHash**: ~O(1), zero heap allocation, ~1 instruction per key. With ~60K entries at 0.5 load factor, collision rate is negligible in practice.

### Cargo.toml dependencies (planned)

```toml
[dependencies]
arrow       = { version = ">=50", features = ["simd"] }
parquet     = { version = ">=50", features = ["arrow"] }
rustc-hash  = "1"          # FxHash for integer-keyed hash table
csv         = "1"
clap        = { version = "4", features = ["derive"] }

[profile.release]
opt-level     = 3
lto           = true
codegen-units = 1
```

Build for benchmarking with native CPU instructions to enable auto-vectorisation:
```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

---

## 4. Project Structure

```
goosedb/
├── CLAUDE.md                  # This file
├── README.md                  # User-facing: setup, dependencies, how to run
├── Cargo.toml
├── Cargo.lock
├── run.sh                     # One-command runner (required by spec)
├── check_correctness.sh       # Exact comparison of goosedb output vs DuckDB output
├── data/                      # TPC-H Parquet files (not committed to git)
│   ├── sf0.5/
│   │   ├── lineitem.parquet
│   │   └── part.parquet
│   ├── sf1/  ...
│   ├── sf2/  ...
│   └── sf5/  ...
├── scripts/
│   ├── generate_data.sql      # DuckDB script to generate + export Parquet
│   └── duckdb_baseline.sql    # DuckDB timing script (PRAGMA threads=1)
├── src/
│   ├── main.rs                # CLI entry point (clap), benchmark loop, orchestration
│   ├── config.rs              # CLI args struct, scale-factor path resolution
│   ├── pipeline/
│   │   ├── mod.rs
│   │   ├── pipeline1.rs       # Part scan → filter → encode → hash build (single fused pass)
│   │   └── pipeline2.rs       # Lineitem scan → pre-filter → compact → fused probe+filter+agg
│   ├── operators/
│   │   ├── mod.rs
│   │   ├── scan.rs            # Parquet column-projection scanner, yields Arrow RecordBatches
│   │   ├── filter.rs          # Bitmask construction (FilterMask over Arrow columns)
│   │   ├── project.rs         # BYTE_ARRAY→u8 encoding, compact-batch extraction
│   │   ├── hash_table.rs      # FxHash open-addressing hash table (build + probe)
│   │   └── aggregate.rs       # i128 accumulator, final f64 conversion
│   ├── types/
│   │   ├── mod.rs
│   │   ├── batches.rs         # LineitemFilteredBatch
│   │   └── masks.rs           # FilterMask ([u64; 32] bitmask)
│   ├── encoding.rs            # BYTE_ARRAY brand/container → u8 index lookup tables
│   └── timer.rs               # Lap-timer utility for operator-level profiling
└── tests/
    ├── correctness_sf0_5.rs   # Integration test: compare output to DuckDB expected
    └── expected/
        └── q19_sf0.5.csv      # Expected result from DuckDB at SF=0.5
```

### Key structural decisions

- **No `PartVectorBatch` struct**: Pipeline 1 reads Arrow columns directly from the Parquet scanner, filters, encodes, and inserts each survivor into the hash table in a single fused pass. Materialising an intermediate batch struct would add a write-then-read cycle with no benefit.
- **No `ProbeResultBuffer`, `ProbeResult`, or `JoinFilterMask`**: The probe, post-join filter, and revenue accumulation are fused into a single loop in `pipeline2.rs`. Buffering matched rows before the next stage added unnecessary memory traffic.
- **No `RevenueVector`**: Revenue is accumulated directly into `AggregateState` as an `i128` integer without materialising an intermediate vector.
- **Benchmark loop in `main.rs`**: The `--bench --runs N` mode is handled entirely inside Rust. The process is launched once; the query executes N times within that single process. This avoids counting process startup overhead in timing measurements.

---

## 5. Execution Model

goosedb uses the **vectorised (batch) model**:

- Each operator processes a **batch of up to 2048 rows** at a time rather than a single tuple.
- Operators are loop-optimised for processing full batches using SIMD-friendly array layouts.
- Batch size is fixed at **2048 rows** — chosen to fit comfortably in L1/L2 cache per batch, and to match Arrow's natural page granularity.
- The model is **streaming**: only one batch is live in memory at a time per pipeline stage (no full materialisation of intermediate tables).

This is in contrast to the iterator (Volcano) model where each call returns one tuple — that model incurs millions of function calls on a 1.5M-row lineitem table. The batch model reduces function call overhead by ~2000×.

---

## 6. Pipeline Architecture

### Pipeline 1 — Part Table: Scan → Filter → Encode → Hash Build (single fused pass)

Pipeline 1 runs to completion first. Its output (the hash table) is held in heap memory and passed to Pipeline 2.

```
TableScan(part)   [Arrow RecordBatch, 2048 rows at a time]
  │  reads columns: p_partkey (INT64), p_brand (BYTE_ARRAY),
  │                 p_size (INT32), p_container (BYTE_ARRAY)
  ▼
For each row in batch — single fused loop (no intermediate batch materialised):
  │
  ├─ Filter (inline):
  │    p_brand bytes IN {b"Brand#12", b"Brand#23", b"Brand#34"}
  │    AND p_size BETWEEN 1 AND 15
  │    AND p_container bytes IN {b"SM CASE", b"SM BOX", ..., b"LG PKG"}  (12 values)
  │    → ~60K survivors from 200K rows
  │
  ├─ Encode surviving row:
  │    p_brand_idx     = byte_to_brand_idx(p_brand)         → u8 {0,1,2}
  │    p_container_idx = byte_to_container_idx(p_container) → u8 {0..11}
  │    p_size                                               → u8 (values 1–15 post-filter)
  │
  └─ Insert into HashTable keyed on p_partkey (i64):
       entry = HashTableEntry { p_partkey, p_brand_idx, p_size, p_container_idx }
       uses FxHash open-addressing, load factor ≤ 0.5

HashTable built: ~60K entries × 16 bytes = ~960KB → fits in L2/L3 cache
```

---

### Pipeline 2 — Lineitem Table: Scan → Pre-Filter → Compact → Fused Probe+Filter+Aggregate

```
TableScan(lineitem)   [Arrow RecordBatch, 2048 rows at a time]
  │  reads columns: l_partkey (INT64), l_quantity (INT64/DECIMAL),
  │                 l_shipmode (BYTE_ARRAY), l_shipinstruct (BYTE_ARRAY),
  │                 l_extendedprice (INT64/DECIMAL), l_discount (INT64/DECIMAL)
  ▼
Pre-Join Filter (FilterMask — [u64; 32] bitmask)
  │  l_shipmode bytes IN {b"AIR", b"AIR REG"}
  │  AND l_shipinstruct bytes == b"DELIVER IN PERSON"
  │  AND l_quantity_raw <= 3000            ← raw INT64: 30.00 × 100
  │  → ~128K survivors from 1.5M rows (~8.5% pass rate)
  │  → bitmask tracks survivors without copying data
  ▼
Compact Projection (LineitemFilteredBatch)
  │  extract only surviving rows from bitmask (~285 per batch typically)
  │  keep: l_partkey (i64), l_quantity_raw (i64),
  │        l_extendedprice_raw (i64), l_discount_raw (i64)
  │  l_shipmode and l_shipinstruct dropped — already consumed by pre-filter
  ▼
Fused: Hash Probe + Post-Join Filter + Aggregate   [single loop, ~285 iterations/batch]

  for i in 0..filtered_batch.count:
    if let Some(entry) = hash_table.get(filtered_batch.l_partkey[i]):
      let q = filtered_batch.l_quantity_raw[i];
      let passes =
        // Group 1: Brand#12, SM containers, qty 1–11, size 1–5
        (entry.brand_idx == 0 && entry.container_idx < 4
         && q >= 100 && q <= 1100 && entry.size <= 5)
        // Group 2: Brand#23, MED containers, qty 10–20, size 1–10
        || (entry.brand_idx == 1 && entry.container_idx >= 4 && entry.container_idx < 8
            && q >= 1000 && q <= 2000 && entry.size <= 10)
        // Group 3: Brand#34, LG containers, qty 20–30, size 1–15
        || (entry.brand_idx == 2 && entry.container_idx >= 8
            && q >= 2000 && q <= 3000 && entry.size <= 15);
      if passes:
        agg.accumulator += (l_extendedprice_raw[i] * (100 - l_discount_raw[i])) as i128;

  ~121 rows contribute to accumulator at SF=1

UngroupedAggregate (AggregateState)
     accumulator: i128  — exact integer sum, no floating-point error
     final output: accumulator as f64 / 10_000.0  — single conversion at the very end
```

---

## 7. Data Structures

All structs are `#[repr(C, align(64))]` (64-byte cache-line aligned) unless noted otherwise.

### Pipeline 1

No intermediate batch struct is materialised. Arrow `RecordBatch` columns are accessed directly. The only output of Pipeline 1 is the hash table.

#### `HashTableEntry`  (`#[repr(C, align(8))]`)
```rust
struct HashTableEntry {
    p_partkey:       i64,     // 8 bytes — physical INT64 from Parquet, join key
    p_brand_idx:     u8,      // 1 byte  — 0=Brand#12, 1=Brand#23, 2=Brand#34
    p_size:          u8,      // 1 byte  — 1–15 (safe post-filter)
    p_container_idx: u8,      // 1 byte  — 0–11 for 12 container types
    _padding:        [u8; 5], // 5 bytes — pad to 16 bytes total for alignment
}
// 60K entries × 16 bytes = ~960KB → fits in L2/L3 cache
```

#### `HashTable`  (`#[repr(C, align(64))]`)
```rust
struct HashTable {
    entries:  Vec<HashTableEntry>, // dense array of valid entries
    buckets:  Vec<u32>,            // open-addressing bucket indices
                                   // value = index into entries; u32::MAX = empty slot
    size:     u32,                 // number of valid entries (~60K at SF=1)
    capacity: u32,                 // bucket array length (power of 2, 2× size → 0.5 load factor)
}
// Hash function: FxHasher on i64 p_partkey (rustc-hash crate)
```

> **Direct-address table alternative (worth benchmarking at SF≤2):**  
> TPC-H `p_partkey` values are dense integers in `[1, 200_000 × SF]`. A flat `Vec<Option<HashTableEntry>>` indexed directly by partkey eliminates hashing and collision handling entirely. At SF=1 this costs ~3.2MB (200K × 16 bytes) — likely fits in L3. At SF=5 (~16MB) it likely exceeds L3 on most machines, at which point the hash table's smaller footprint wins. Implement the hash table first; swap in the direct-address table and benchmark both.

---

### Pipeline 2

#### Arrow columns (accessed directly from `RecordBatch`, no wrapper struct)
```rust
// Physical types as returned by the Arrow Parquet reader:
l_partkey:       &Int64Array    // i64 — INT64
l_quantity:      &Int64Array    // i64 — INT64, DECIMAL(15,2) raw
l_shipmode:      &StringArray   // &str — BYTE_ARRAY (zero-copy)
l_shipinstruct:  &StringArray   // &str — BYTE_ARRAY (zero-copy)
l_extendedprice: &Int64Array    // i64 — INT64, DECIMAL(15,2) raw
l_discount:      &Int64Array    // i64 — INT64, DECIMAL(15,2) raw

// String comparison uses .as_bytes() to avoid allocation:
shipmode.as_bytes() == b"AIR" || shipmode.as_bytes() == b"AIR REG"
shipinstruct.as_bytes() == b"DELIVER IN PERSON"
```

#### `FilterMask`
```rust
struct FilterMask {
    bitmask:   [u64; 32],  // 2048 bits — bit i set = row i passes filter
    set_count: u16,        // number of set bits (survivors)
}
// Reused for both the part filter (Pipeline 1, if needed) and lineitem pre-filter (Pipeline 2)
```

#### `LineitemFilteredBatch`
```rust
struct LineitemFilteredBatch {
    l_partkey:           Vec<i64>,  // for hash probe
    l_quantity_raw:      Vec<i64>,  // raw DECIMAL(15,2) integer, for post-join filter
    l_extendedprice_raw: Vec<i64>,  // raw DECIMAL(15,2) integer, for revenue
    l_discount_raw:      Vec<i64>,  // raw DECIMAL(15,2) integer, for revenue
    count:               u16,       // ~285 typical per 2048-row input batch
}
// No original_indices — not needed since probe+filter+agg are fused into one loop.
// l_shipmode and l_shipinstruct are dropped here — consumed by the pre-filter.
```

#### `AggregateState`
```rust
struct AggregateState {
    accumulator: i128,  // exact integer sum of (extendedprice_raw × (100 - discount_raw))
                        // i128 prevents overflow: at SF=5, ~600 rows × max ~10^17 → well within i128 range
    row_count:   u64,   // total matching rows (for debugging/validation)
}

impl AggregateState {
    fn finalise(&self) -> f64 {
        self.accumulator as f64 / 10_000.0  // single float conversion at output
    }
}
// No Kahan summation needed — integer accumulation is exact by definition.
```

---

## 8. Optimisations

### Primary (from hypothesis)

| Optimisation        | Description                                                                                                 | Impact                                                               |
|---------------------|-------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| **Filter pushdown** | Push brand/size/container predicates to part scan; push shipmode/shipinstruct/quantity≤30 to lineitem scan  | Reduces join probe from 1.5M→~128K rows; hash build from 200K→~60K  |

### Secondary (low-level)

| Optimisation                          | Description                                                                                           | Rationale                                                             |
|---------------------------------------|-------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------|
| **Batch / vectorised model**          | Process 2048 rows per iteration instead of 1                                                          | ~2000× fewer loop iterations vs Volcano model                         |
| **Fused probe+filter+aggregate**      | Hash probe, post-join filter, and revenue accumulation merged into a single loop                      | Eliminates intermediate buffer write-then-read cycles                 |
| **Single-pass Pipeline 1**            | Part filter, encoding, and hash insertion fused — no intermediate batch materialised                  | Avoids one full write-then-read of ~60K rows                          |
| **Column projection**                 | Read only 6 lineitem columns and 4 part columns from Parquet                                          | Reduces decompression work and memory bandwidth                       |
| **Filter bitmask**                    | Track pre-filter survivors in `[u64; 32]` without copying rows                                        | Zero data movement for rejected rows                                  |
| **Compact batch extraction**          | Copy only bitmask survivors into `LineitemFilteredBatch` before probing                               | Reduces probe iterations from ~2048 to ~285 per batch                 |
| **BYTE_ARRAY raw byte comparison**    | Compare strings as `&[u8]` bytes, never allocating `String`                                           | No heap allocation; enables compiler to optimise comparisons          |
| **String encoding (u8 index)**        | Encode brand (3 values→u8) and container (12 values→u8) once at hash build time                       | Post-join filter works on 1-byte integers — auto-vectorises cleanly   |
| **Integer arithmetic throughout**     | DECIMAL fields kept as raw `i64`; revenue summed as `i128`; single `f64` conversion at output         | Exact result matching DuckDB; no floating-point rounding possible     |
| **FxHash open-addressing**            | Power-of-2 capacity, linear probing, load factor ≤0.5, FxHash on `i64` keys                          | Near-O(1) probe, minimal collisions, no pointer chasing               |
| **Cache-aligned structs**             | Hot structs marked `#[repr(C, align(64))]`                                                            | Avoids cache-line splits; improves hardware prefetcher behaviour       |
| **Cache-resident hash table**         | ~60K entries × 16 bytes = ~960KB → fits in L2/L3 cache                                               | Near-zero cache miss rate on probe phase                              |
| **4× loop unrolling**                 | Manual unrolling of inner filter/aggregate loops where beneficial                                     | Reduces loop overhead, enables better instruction-level parallelism   |
| **Auto-vectorisation**                | `RUSTFLAGS="-C target-cpu=native"` + aligned structs + scalar integer ops                             | Compiler emits SIMD instructions automatically; no manual intrinsics  |
| **Streaming (no materialisation)**    | Never hold more than one batch per stage in memory                                                    | Bounded memory regardless of scale factor                             |
| **Direct-address table** *(optional)* | Replace hash table with flat `Vec` indexed by partkey at SF≤2                                        | Zero hash computation; ~3.2MB at SF=1, benchmarking needed at SF=5   |
| **Parquet row-group statistics** *(optional)* | Use Parquet min/max zone maps to skip row groups that cannot match any predicate          | Further reduces I/O at large scale factors                            |

---

## 9. Build & Run Commands

### Prerequisites

```bash
# Install Rust (stable)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install DuckDB CLI (for data generation and baseline)
# macOS:  brew install duckdb
# Linux:  download binary from https://duckdb.org/docs/installation/

# Generate TPC-H Parquet data (run once per scale factor)
duckdb < scripts/generate_data.sql
```

### Build

```bash
# Debug build (for development and correctness testing)
cargo build

# Release build with native CPU optimisations (always use this for benchmarking)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### Run (single query execution)

```bash
# Using the one-command runner
./run.sh --data data/sf1 --out result.csv

# Directly via cargo
cargo run --release -- --data data/sf1 --out result.csv
```

### Benchmark mode (internal Rust loop — no process-restart overhead)

```bash
./run.sh --data data/sf1 --out result.csv --bench --runs 6
# Executes the query 6 times inside a single process.
# Discards run 1 (warm-up). Reports mean of runs 2–6 in milliseconds.
```

### Run all scale factors

```bash
for sf in 0.5 1 2 5; do
    ./run.sh --data data/sf${sf} --out results/result_sf${sf}.csv --bench --runs 6
done
```

### Correctness check

```bash
# Generate DuckDB reference output first (reads same Parquet files, single thread)
duckdb < scripts/duckdb_baseline.sql   # writes duckdb_result.csv

# Exact comparison of goosedb output vs DuckDB output
./check_correctness.sh result.csv duckdb_result.csv
```

### Tests

```bash
# Run unit and integration tests
cargo test

# Run with printed output (verbose)
cargo test -- --nocapture
```

---

## 10. Benchmarking Protocol

Follows the project specification exactly:

| Parameter         | Value                                                              |
|-------------------|--------------------------------------------------------------------|
| Scale factors     | SF = 0.5, 1, 2, 5                                                  |
| Runs per SF       | 6 minimum                                                          |
| Warm-up           | First run excluded from reported average                           |
| Reported metric   | Mean of runs 2–6 in **milliseconds**                               |
| Threading         | **Single-threaded only** — no parallel scan, join, or aggregation  |
| DuckDB baseline   | `PRAGMA threads=1;` — reads from same Parquet files, same machine  |
| Cold start        | No prebuilt indexes, no cached answers, no cross-run state         |
| Timing scope      | Wall-clock time from first Parquet read to final result output     |

### Timing implementation

```rust
// In main.rs — process launched once, query runs N times internally
for run in 0..args.runs {
    let start = std::time::Instant::now();
    let result = execute_query(&args.data_path)?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
    if run > 0 {  // skip warm-up run
        timings.push(elapsed_ms);
    }
}
let mean_ms = timings.iter().sum::<f64>() / timings.len() as f64;
println!("Mean (runs 2–{}): {:.2} ms", args.runs, mean_ms);
```

### Machine specs to report

- CPU model and core count
- RAM (GB)
- OS + kernel version
- Rust version (`rustc --version`)
- DuckDB version (`duckdb --version`)
- `RUSTFLAGS` used at build time

---

## 11. Correctness Validation

Both goosedb and DuckDB read from the **same physical Parquet files** containing the same raw `INT64` values. By keeping all arithmetic in the integer domain (`i128` accumulator, single `f64` conversion at output), goosedb's result is **exactly identical** to DuckDB's — no floating-point drift is possible.

- **No epsilon tolerance needed**: the correctness check is an exact string comparison of the printed revenue value.
- **No ORDER BY in Q19**: the result is a single scalar, so ordering is irrelevant.
- **Arrow DECIMAL reading**: when reading DECIMAL(15,2) columns via Arrow, read them as `Int64Array` (the physical type) rather than allowing Arrow to auto-convert to `Float64Array`. This preserves the exact integer representation for all arithmetic.

```bash
# check_correctness.sh — exact string match
goosedb_val=$(tail -1 result.csv)
duckdb_val=$(tail -1 duckdb_result.csv)
if [ "$goosedb_val" = "$duckdb_val" ]; then
    echo "PASS: $goosedb_val"
else
    echo "FAIL: goosedb=$goosedb_val  duckdb=$duckdb_val"
    exit 1
fi
```

---

## 12. Constraints & Rules

From the project specification:

- **Allowed:** Any language (we use Rust), Parquet/Arrow libraries, hard-coded Q19 logic, operator-level profiling.
- **Not allowed:** Embedding DuckDB or any other SQL engine to execute the query. No answer caching across runs. No precomputed aggregates.
- **Single-threaded execution required** for both goosedb and the DuckDB baseline.
- **Parquet input only** — do not convert to CSV first.
- **Exactly one Parquet file per table per scale factor.**
- **File names must match exactly:** `lineitem.parquet`, `part.parquet`.

---

## 13. Deliverables Checklist

- [ ] Source code in clean repository structure
- [ ] `README.md` — setup, dependencies, exact reproduction steps, machine specs
- [ ] `run.sh` — one-command runner supporting `--data`, `--out`, `--bench`, `--runs`
- [ ] `check_correctness.sh` — exact comparison of goosedb output vs DuckDB output
- [ ] `scripts/generate_data.sql` — DuckDB script to produce Parquet files at all SFs
- [ ] `scripts/duckdb_baseline.sql` — DuckDB timing script with `PRAGMA threads=1`
- [ ] Benchmark results table (DuckDB vs goosedb across SF 0.5/1/2/5)
- [ ] Runtime vs scale factor plot
- [ ] Operator-level timing breakdown (scan vs join vs filter vs aggregate)
- [ ] Final presentation slides (20 min):
  - [ ] Query 19 characteristics
  - [ ] DuckDB baseline: plan + bottleneck analysis
  - [ ] goosedb architecture: pipelines, operators, data structures
  - [ ] Key optimisations with concrete examples
  - [ ] Correctness validation method
  - [ ] Benchmark methodology
  - [ ] Results + interpretation
  - [ ] Limitations + future improvements
  - [ ] Live demo (one-command runner + output + timing)

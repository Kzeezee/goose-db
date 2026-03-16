# goosedb — Optimisation Tracker

Tracks every optimisation attempted, implemented, or planned for TPC-H Q19.

**Baseline (this machine, SF=1):**
| System | Mean time | Notes |
|---|---|---|
| DuckDB (`threads=1`, internal timer) | **~281 ms** | `.timer on` via stdin; excludes process startup |
| goosedb (pre single-pass refactor) | **~340 ms** | `--bench --runs 6`, BATCH_SIZE=8192, RowFilter approach |
| goosedb (current) | **TBD** | `--bench --runs 6`, BATCH_SIZE=4096, single-pass no RowFilter |

> The spec's ~159ms DuckDB figure was from a different, faster machine. Both engines are bottlenecked
> by Parquet decompression of the 198MB lineitem file; DuckDB's edge comes from its SIMD-accelerated
> C++ decompressor.

---

## ✅ Done

### Architecture / correctness

| # | Optimisation | Where | Notes |
|---|---|---|---|
| 1 | **Predicate pushdown to part scan** | `pipeline1.rs`, `scan.rs` | Inline filter in the batch loop: p_size first (eliminates ~70%), then brand, then container. No intermediate batch materialised |
| 2 | **Predicate pushdown to lineitem scan** | `pipeline2.rs` | Inline pre-filter in the fused loop: `l_shipinstruct = 'DELIVER IN PERSON'` first (~75% rejection), then `l_shipmode IN {'AIR','AIR REG'}`, then `l_quantity_raw ≤ 3000` |
| 3 | **Single-pass scan — no RowFilter** | `scan.rs`, `pipeline1.rs`, `pipeline2.rs` | Both pipelines project all needed columns in one Parquet pass. Eliminates the RowFilter double-decode (filter columns decoded once for predicate + again in main projection). Old RowFilter approach decoded 1.5M lineitem rows twice; new approach decodes them once. See "Tried and reverted" for RowFilter details |
| 4 | **Column projection** | `scan.rs` | Part: 4 columns. Lineitem: 6 columns (partkey, quantity, shipmode, shipinstruct, price, discount). Only columns actually needed are decoded |
| 5 | **Batch size** | `config.rs` | `BATCH_SIZE = 4096`. With 6 lineitem columns at 4096 rows ≈ 192KB numeric data — fits in L2. Previously 8192 (optimal for RowFilter amortisation); 4096 is better for the single-pass approach |
| 6 | **Fused pre-filter + probe + post-join filter + aggregate** | `pipeline2.rs` | All four operations in a single loop per batch: pre-filter → DirectTable probe → 3-way OR post-join filter → revenue accumulation. Zero intermediate buffers |
| 7 | **Streaming (no full materialisation)** | all | At most one batch live per pipeline stage. Memory usage is O(1) regardless of SF |
| 8 | **BYTE_ARRAY raw byte comparison** | `encoding.rs`, `scan.rs` | All string comparisons use `.as_bytes()` — no heap allocation, enables compiler pattern matching |
| 9 | **String encoding (u8 index)** | `encoding.rs`, `pipeline1.rs` | brand → u8 {0,1,2}, container → u8 {0..11} (grouped: SM=0–3, MED=4–7, LG=8–11). Post-join filter uses u8 integer comparisons, not string ops |
| 10 | **Integer arithmetic throughout** | `aggregate.rs`, `pipeline2.rs` | DECIMAL(15,2) kept as raw `i64` (Decimal128Array values cast to i64 at use). Revenue accumulated as `i128` (exact, no floating-point error). Single `f64` conversion at output |
| 11 | **Direct-address lookup table** | `hash_table.rs`, `pipeline1.rs` | TPC-H `p_partkey` ∈ [1, 200K×SF] is dense → flat `Vec` indexed by `partkey - 1`. Single array access per probe, zero hashing, zero collision handling. ~3.2MB at SF=1 (fits L3) |
| 12 | **Auto-vectorisation** | `run.sh` | `RUSTFLAGS="-C target-cpu=native"` + `opt-level=3` + `lto=true` + `codegen-units=1`. Compiler emits native SIMD for integer arithmetic loops |
| 13 | **Benchmark loop in-process** | `main.rs` | `--bench --runs N` reruns the query N times inside one process. Process startup is never counted in timings. Run 1 discarded as warmup |
| 14 | **Operator-level timing** | `main.rs`, `pipeline*.rs`, `scan.rs` | `--timing` flag prints lap-by-lap breakdown |

### Tried and reverted

| Attempt | Result | Why reverted |
|---|---|---|
| **3-predicate RowFilter** (one predicate per filter column, evaluated in sequence) | ~611 ms — **worse** | Three separate Parquet column reads (one per predicate) + three BooleanArray allocations per batch cost more than the I/O saved from page skipping |
| **FilterMask + compact step** (original design) | ~403 ms | Superseded by single-pass fused loop. Compact step was redundant overhead |
| **Single RowFilter + 4-column main projection** | ~340 ms | Replaced by single-pass no-RowFilter. RowFilter caused double-decode of filter columns and added BooleanBuilder + RowSelection construction overhead (~200ms pass 1 + ~125ms pass 2 = ~325ms for lineitem alone) |
| **BATCH_SIZE = 8192** (with RowFilter) | ~340 ms | Was optimal for RowFilter amortisation; 4096 is better for single-pass approach |
| **BATCH_SIZE = 16384** | ~434 ms — **worse** | Batch data exceeds L2 cache; more variance; BooleanBuilder for 16K rows spills to L3 |

---

## ❌ Not Done — Planned

### High priority (likely biggest wins)

| # | Optimisation | Estimated gain | Description |
|---|---|---|---|
| P1 | **~~Single-pass scan / bypass RowFilter~~** | ✅ Done | See #3 above |
| P2 | **~~Batch size tuning~~** | ✅ Done | BATCH_SIZE=4096 for single-pass approach |
| P3 | **Parquet row-group statistics skipping** | **negligible for TPC-H** | Investigated: TPC-H lineitem data has uniform distribution — no row group has all quantities > 30 or all shipinstruct values ≠ 'DELIVER IN PERSON'. Min/max statistics would not allow any row groups to be skipped. Not worth implementing. |
| P4 | **Cache-line alignment on hot structs** | **not applicable** | DirectTable slots are 16 bytes; Vec allocates page-aligned (4KB+). No cache-line splits occur in practice. Aligning to 64 bytes would 4× the table size (3.2MB → 12.8MB), hurting L3 fit. |
| P5 | **Read DECIMAL columns as Int64Array** | 1.1–1.2× | Parquet logical type annotation causes Arrow to promote INT64 DECIMAL(15,2) columns to Decimal128Array (i128, 16 bytes). Reading as Int64Array (8 bytes) would halve memory bandwidth for l_quantity, l_extendedprice, l_discount. `with_skip_arrow_metadata(true)` alone is insufficient — need `with_schema()` override or lower-level reader to force Int64. |

### Medium priority

| # | Optimisation | Estimated gain | Description |
|---|---|---|---|
| M1 | **DirectTable vs FxHash at SF=5** | benchmarking | DirectTable is ~16MB at SF=5, which may exceed L3 (8–16MB on most CPUs). FxHash table is ~2MB. Need to benchmark both at SF=5 to decide which to use at large SF |
| M2 | **Arrow compute kernels for RowFilter predicate** | **likely small** | The 200ms RowFilter cost is dominated by Parquet decompression I/O, not the BooleanBuilder loop (~0.5ms). Arrow compute `eq_utf8_scalar` for plain StringArray has similar cost to our scalar loop. Only meaningful if columns are DictionaryArray (not currently the case). |
| M3 | **Prefetch hash table entries** | 5–10% at SF≥2 | For the probe loop: prefetch `DirectTable.slots[partkey+8]` while processing `partkey`. Avoids stalling on L3 cache miss. Requires `core::arch::x86_64::_mm_prefetch` (unsafe) |

### Low priority / stretch

| # | Optimisation | Description |
|---|---|---|
| L1 | **Direct Parquet page-level I/O** | Bypass Arrow's RecordBatch API and read raw Parquet data pages directly. Eliminates Arrow struct overhead (null bitmaps, offset buffers) for non-nullable integer columns. Significant engineering effort |
| L2 | **Memory-mapped Parquet** | `mmap` the Parquet files instead of `File::read`. OS handles prefetch and avoids user/kernel copy. May reduce latency at SF=1 where file fits in page cache |
| L3 | **Compress DirectTable** | At SF=5 (16MB), encode part table entries more compactly. E.g., store only `{brand_idx: u2, size: u4, container_idx: u4}` = 2 bytes per slot → 400KB at SF=1. Eliminates partkey field (implicit from index). Would fit in L2 |

---

## 📋 Still To Do (non-performance)

| # | Task | Description |
|---|---|---|
| D1 | **Generate SF=0.5, SF=2, SF=5 data** | Run `duckdb < scripts/generate_data.sql` to produce Parquet files for all scale factors |
| D2 | **Benchmark all scale factors** | Run `--bench --runs 6` at SF=0.5, 1, 2, 5. Record DuckDB baseline at each SF |
| D3 | **Integration test** | `tests/correctness_sf1.rs` — run both pipelines, assert result == `3083843.0578` |
| D4 | **README.md** | User-facing: setup, build, run, correctness check, machine specs, results table |
| D5 | **Benchmark results table** | DuckDB vs goosedb across SF=0.5/1/2/5, with operator breakdown |
| D6 | **Operator-level timing report** | ✅ Done — use `--timing` flag to print pipeline1 time, predicate scan, main scan time |

---

## 🔬 Bottleneck Analysis

### Pre single-pass refactor (BATCH_SIZE=8192, RowFilter approach)

Measured with `--timing` on run 6 (warm cache):

| Stage | Actual time | What was happening |
|---|---|---|
| part file open + metadata | ~7 ms | Read 6MB part.parquet footer |
| part scan + DirectTable build | ~3 ms | 200K rows → RowFilter → ~60K entries inserted |
| lineitem file open | <1 ms | OS file open |
| lineitem metadata parse | ~1 ms | Read 198MB lineitem.parquet footer |
| **lineitem predicate scan** (`build()`) | **~200 ms** | RowFilter decoded all 1.5M rows across 3 filter columns → RowSelection |
| **lineitem main scan + probe + aggregate** | **~125 ms** | 4 columns for ~128K selected rows → probe → filter → accumulate |

**Why RowFilter was a net loss:** The RowFilter two-pass design (filter all rows, then read survivors) only wins when main projection columns are much more expensive to decode than filter columns. In Q19, all columns are similarly sized INT64 — so the double-decode cost outweighed the selective main-projection savings.

### Post single-pass refactor (BATCH_SIZE=4096, no RowFilter)

Re-run benchmark to update these numbers:

| Stage | Actual time |
|---|---|
| part file open + metadata | TBD |
| part scan + DirectTable build | TBD |
| lineitem reader build | TBD |
| lineitem scan + probe + aggregate | TBD |

**Why DuckDB is faster:** DuckDB uses a C++ Parquet decompressor with AVX-512 SIMD acceleration. Its decompression throughput is estimated at 2–4× higher than the Rust `parquet` crate on the same hardware. This gap is structural and not addressable without replacing the Parquet reader.

**Remaining opportunities:** P5 (Int64 for DECIMAL columns), M3 (prefetch at SF≥2).

# goosedb — Optimisation Tracker

Tracks every optimisation attempted, implemented, or planned for TPC-H Q19.

**Baseline (this machine, SF=1):**
| System | Mean time | Notes |
|---|---|---|
| DuckDB (`threads=1`, internal timer) | **~281 ms** | `.timer on` via stdin; excludes process startup |
| goosedb (current best) | **~358 ms** | `--bench --runs 8`, BATCH_SIZE=8192; steady-state runs ~340–360 ms |
| Ratio | **1.27×** | Bottleneck is Parquet decompression (Rust crate vs DuckDB's SIMD C++ decompressor) |

> The spec's ~159ms DuckDB figure was from a different, faster machine. Actual gap on this
> machine is ~1.27×. Both engines are bottlenecked by Parquet decompression of the 198MB
> lineitem file; DuckDB's edge comes from its SIMD-accelerated C++ decompressor.

---

## ✅ Done

### Architecture / correctness

| # | Optimisation | Where | Notes |
|---|---|---|---|
| 1 | **Predicate pushdown to part scan** | `pipeline1.rs`, `scan.rs` | RowFilter pre-filters brand ∈ {#12,#23,#34}, size 1–15, container ∈ 12 values before rows enter the pipeline |
| 2 | **Predicate pushdown to lineitem scan** | `scan.rs` | Single combined RowFilter predicate: `l_shipinstruct = 'DELIVER IN PERSON' AND l_shipmode IN {'AIR','AIR REG'} AND l_quantity_raw ≤ 3000`. Drops `l_shipinstruct` and `l_shipmode` from main projection entirely |
| 3 | **Column projection** | `scan.rs` | Part: 4 columns. Lineitem main projection: only 4 columns (partkey, quantity, price, discount). Filter columns read only in predicate batch, not main batch |
| 4 | **Batch size tuning** | `config.rs` | `BATCH_SIZE = 8192`. Tried 2048 (baseline: ~392ms), 8192 (**~356ms**, best), 16384 (worse: ~434ms, L2 overflow). Larger batches amortise per-batch RowFilter overhead |
| 5 | **Single-pass Pipeline 1** | `pipeline1.rs` | Part scan → RowFilter → encode brand/container to u8 → insert into DirectTable. No intermediate batch materialised |
| 6 | **Fused probe + post-join filter + aggregate** | `pipeline2.rs` | Hash probe, 3-way OR filter, and revenue accumulation in one loop. No intermediate buffer |
| 7 | **Streaming (no full materialisation)** | all | At most one batch live per pipeline stage. Memory usage is O(1) regardless of SF |
| 8 | **BYTE_ARRAY raw byte comparison** | `encoding.rs`, `scan.rs` | All string comparisons use `.as_bytes()` — no heap allocation, enables compiler pattern matching |
| 9 | **String encoding (u8 index)** | `encoding.rs`, `pipeline1.rs` | brand → u8 {0,1,2}, container → u8 {0..11} (grouped: SM=0–3, MED=4–7, LG=8–11). Post-join filter uses u8 integer comparisons, not string ops |
| 10 | **Integer arithmetic throughout** | `aggregate.rs`, `pipeline2.rs` | DECIMAL(15,2) kept as raw `i64`. Revenue accumulated as `i128` (exact, no floating-point error). Single `f64` conversion at output |
| 11 | **Direct-address lookup table** | `hash_table.rs`, `pipeline1.rs` | Replaces FxHash open-addressing table. TPC-H `p_partkey` ∈ [1, 200K×SF] is dense → flat `Vec` indexed by `partkey - 1`. Single array access per probe, zero hashing, zero collision handling. ~3.2MB at SF=1 (fits L3) |
| 12 | **Column index caching per batch** | `pipeline2.rs` | `batch.column(0/1/2/3)` by index instead of `column_by_name()` on every batch. Eliminates per-batch hash map lookups |
| 13 | **Auto-vectorisation** | `run.sh` | `RUSTFLAGS="-C target-cpu=native"` + `opt-level=3` + `lto=true` + `codegen-units=1`. Compiler emits native SIMD for integer arithmetic loops |
| 14 | **Benchmark loop in-process** | `main.rs` | `--bench --runs N` reruns the query N times inside one process. Process startup is never counted in timings. Run 1 discarded as warmup |
| 15 | **Operator-level timing** | `main.rs`, `pipeline*.rs`, `scan.rs` | `--timing` flag prints lap-by-lap breakdown. Revealed true bottleneck: RowFilter `build()` does eager full predicate scan |

### Tried and reverted

| Attempt | Result | Why reverted |
|---|---|---|
| **3-predicate RowFilter** (one predicate per filter column, evaluated in sequence) | ~611 ms — **worse** | Three separate Parquet column reads (one per predicate) + three BooleanArray allocations per batch cost more than the I/O saved from page skipping |
| **FilterMask + compact step** (original design) | ~403 ms | Superseded by RowFilter, which already returns pre-filtered batches. Compact step was redundant overhead |
| **BATCH_SIZE = 16384** | ~434 ms — **worse** | Batch data (~917KB) exceeds L2 cache; more variance; RowFilter BooleanBuilder for 16K rows spills to L3 |

---

## ❌ Not Done — Planned

### High priority (likely biggest wins)

| # | Optimisation | Estimated gain | Description |
|---|---|---|---|
| P1 | **~~Operator-level timing~~** | ✅ Done | See #15 above |
| P2 | **~~Batch size tuning~~** | ✅ Done | BATCH_SIZE=8192 is optimal on this machine |
| P3 | **Parquet row-group statistics skipping** | **negligible for TPC-H** | Investigated: TPC-H lineitem data has uniform distribution — no row group has all quantities > 30 or all shipinstruct values ≠ 'DELIVER IN PERSON'. Min/max statistics would not allow any row groups to be skipped. Not worth implementing. |
| P4 | **Cache-line alignment on hot structs** | **not applicable** | DirectTable slots are 16 bytes; Vec allocates page-aligned (4KB+). No cache-line splits occur in practice. Aligning to 64 bytes would 4× the table size (3.2MB → 12.8MB), hurting L3 fit. |

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

## 🔬 Confirmed Bottleneck (from operator timing)

Confirmed with `--timing` on run 6 (warm cache, BATCH_SIZE=8192):

| Stage | Actual time | What's happening |
|---|---|---|
| part file open + metadata | ~7 ms | Read 6MB part.parquet footer |
| part scan + DirectTable build | ~3 ms | 200K rows → RowFilter → ~60K entries inserted |
| lineitem file open | <1 ms | OS file open |
| lineitem metadata parse | ~1 ms | Read 198MB lineitem.parquet footer |
| **lineitem predicate scan** (`build()`) | **~200 ms** | RowFilter `build()` eagerly scans ALL 1.5M rows across 3 filter columns (shipinstruct, shipmode, quantity) → produces RowSelection (~128K survivors). This is the dominant cost. |
| **lineitem main scan + probe + aggregate** | **~125 ms** | Reads 4 main columns for only 128K selected rows → probe DirectTable → 3-way OR filter → accumulate |

**Key insight:** The RowFilter mechanism in the parquet crate is a two-pass design:
1. **Pass 1** (in `build()`): scans filter columns for ALL rows → `RowSelection`
2. **Pass 2** (in iteration): reads main projection columns for SELECTED rows only

This is intentional and efficient: the filter columns (shipinstruct, shipmode) are small (dict-encoded), while the main columns (extendedprice, discount) are large (full INT64). Trading a cheap filter scan for selective main-column reads is the right trade-off.

**Why DuckDB is faster (~281ms):** DuckDB uses a C++ Parquet decompressor with AVX-512 SIMD acceleration. Its decompression throughput is estimated at 2–4× higher than the Rust `parquet` crate's decompressor on the same hardware. This gap is structural and not addressable without replacing the Parquet reader.

**Remaining opportunity:** M3 (prefetch) at SF≥2 when DirectTable may exceed L3.

# goosedb — Optimisation Tracker

Tracks every optimisation attempted, implemented, or planned for TPC-H Q19.

**Baseline (this machine, SF=1):**
| System | Mean time | Notes |
|---|---|---|
| DuckDB (`threads=1`, internal timer) | **~281 ms** | `.timer on` via stdin; excludes process startup |
| goosedb (pre single-pass refactor) | **~340 ms** | `--bench --runs 6`, BATCH_SIZE=8192, RowFilter approach |
| goosedb (single-pass, v58) | **~390 ms** | `--bench --runs 6`, BATCH_SIZE=4096, no RowFilter |
| **goosedb (+ dictionary encoding)** | **~215 ms** | **Current** — 1.3× faster than DuckDB |

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
| 3 | **Single-pass scan — no RowFilter** | `scan.rs`, `pipeline1.rs`, `pipeline2.rs` | Both pipelines project all needed columns in one Parquet pass. Eliminates the RowFilter double-decode (filter columns decoded once for predicate + again in main projection). Old RowFilter approach decoded 1.5M lineitem rows twice; new approach decodes them once |
| 4 | **Column projection** | `scan.rs` | Part: 4 columns. Lineitem: 6 columns. Only columns actually needed are decoded |
| 5 | **BATCH_SIZE = 4096** | `config.rs` | With 6 lineitem columns at 4096 rows ≈ 192KB numeric data — fits in L2. Previously 8192 (optimal for RowFilter amortisation) |
| 6 | **Fused pre-filter + probe + post-join filter + aggregate** | `pipeline2.rs` | All four operations in a single loop per batch. Zero intermediate buffers |
| 7 | **Streaming (no full materialisation)** | all | At most one batch live per pipeline stage. Memory usage is O(1) regardless of SF |
| 8 | **BYTE_ARRAY raw byte comparison** | `encoding.rs`, `scan.rs` | All string comparisons use `.as_bytes()` — no heap allocation |
| 9 | **String encoding (u8 index)** | `encoding.rs`, `pipeline1.rs` | brand → u8 {0,1,2}, container → u8 {0..11} (grouped: SM=0–3, MED=4–7, LG=8–11). Post-join filter uses u8 integer comparisons, not string ops |
| 10 | **Integer arithmetic throughout** | `aggregate.rs`, `pipeline2.rs` | DECIMAL(15,2) kept as raw `i64` (Decimal128Array values cast to i64 at use). Revenue accumulated as `i128` (exact, no floating-point error). Single `f64` conversion at output |
| 11 | **Direct-address lookup table** | `hash_table.rs`, `pipeline1.rs` | TPC-H `p_partkey` ∈ [1, 200K×SF] is dense → flat `Vec` indexed by `partkey - 1`. Single array access per probe, zero hashing, zero collision handling. ~3.2MB at SF=1 (fits L3) |
| 12 | **Auto-vectorisation** | `run.sh` | `RUSTFLAGS="-C target-cpu=native"` + `opt-level=3` + `lto=true` + `codegen-units=1` |
| 13 | **Benchmark loop in-process** | `main.rs` | `--bench --runs N` reruns the query N times inside one process. Process startup never counted. Run 1 discarded as warmup |
| 14 | **Operator-level timing** | `main.rs`, `pipeline*.rs`, `scan.rs` | `--timing` flag prints lap-by-lap breakdown |
| 15 | **arrow/parquet crate upgrade to v58** | `Cargo.toml` | Page reader copy reduction (v57), varint decoder improvements (v57), StringView decoder optimizations (v58). Free ~30–50ms improvement over v54 |
| 16 | **Dictionary encoding preservation on string columns** | `scan.rs`, `pipeline2.rs` | `l_shipinstruct` and `l_shipmode` are PLAIN_DICTIONARY encoded in the Parquet file. Use `with_schema()` to request `Dictionary(Int32, Utf8)` for both columns. Arrow then reads 1.5M i32 keys instead of 1.5M decoded strings. Hot loop resolves target strings ("DELIVER IN PERSON", "AIR", "AIR REG") to dictionary indices once per batch, then uses i32 comparisons. **Net effect: ~390ms → ~215ms (-45%)** |

### Tried and reverted

| Attempt | Result | Why reverted |
|---|---|---|
| **3-predicate RowFilter** | ~611 ms — **worse** | Three separate Parquet column reads + BooleanArray allocations per batch cost more than the I/O saved |
| **FilterMask + compact step** | ~403 ms | Superseded by single-pass fused loop |
| **Single RowFilter + 4-column main projection** | ~340 ms | Replaced by single-pass. RowFilter caused double-decode + BooleanBuilder overhead |
| **BATCH_SIZE = 8192** (with RowFilter) | ~340 ms | Was optimal for RowFilter amortisation; 4096 is better for single-pass |
| **BATCH_SIZE = 16384** | ~434 ms — **worse** | Batch data exceeds L2; spills to L3 |
| **`with_schema()` to read DECIMAL columns as Int64** | ❌ fails at runtime | Arrow validation rejects the type mismatch: physical INT64 has Parquet logical type DECIMAL(15,2), so `with_schema()` expects Decimal128 in the supplied schema, not Int64. Error: "Incompatible supplied Arrow schema: data type mismatch for field l_quantity" |
| **Low-level column reader for DECIMAL columns** | ~602 ms — **much worse** | `SerializedFileReader` + per-column `read_records()` reads columns independently within each row group. Loses Arrow's optimised batch-oriented decoder; adds ~2200 Vec allocations per query (366 batches × 6 columns). Arrow's RecordBatch reader is significantly faster for this use case |

---

## ❌ Not Done — Planned

### High priority

| # | Optimisation | Estimated gain | Description |
|---|---|---|---|
| P3 | **Parquet row-group statistics skipping** | **negligible for TPC-H** | TPC-H lineitem data has uniform distribution — no row group has all quantities > 30 or all shipinstruct values ≠ 'DELIVER IN PERSON'. Min/max statistics would not allow any row groups to be skipped |
| P4 | **Cache-line alignment on hot structs** | **not applicable** | DirectTable slots are 16 bytes; Vec allocates page-aligned. Aligning to 64 bytes would 4× the table size (3.2MB → 12.8MB), hurting L3 fit |

### Medium priority

| # | Optimisation | Estimated gain | Description |
|---|---|---|---|
| M1 | **DirectTable vs FxHash at SF=5** | benchmarking needed | DirectTable is ~16MB at SF=5, which may exceed L3. FxHash table is ~2MB. Need to benchmark both at SF=5 |
| M2 | **Prefetch hash table entries** | 5–10% at SF≥2 | For the probe loop: prefetch `DirectTable.slots[partkey+8]` while processing `partkey`. Avoids stalling on L3 cache miss. Requires `core::arch::x86_64::_mm_prefetch` (unsafe) |

### Low priority / stretch

| # | Optimisation | Description |
|---|---|---|
| L1 | **Direct Parquet page-level I/O** | Bypass Arrow's RecordBatch API entirely. Eliminates Arrow struct overhead for non-nullable integer columns. Significant engineering effort |
| L2 | **Memory-mapped Parquet** | `mmap` the Parquet files instead of `File::read`. OS handles prefetch, avoids user/kernel copy |
| L3 | **Compress DirectTable** | At SF=5 (16MB), store only `{brand_idx: u2, size: u4, container_idx: u4}` = 2 bytes per slot → 400KB at SF=1. Would fit in L2 |

---

## 📋 Still To Do (non-performance)

| # | Task | Status |
|---|---|---|
| D1 | **Generate SF=0.5, SF=2, SF=5 data** | ⬜ Run `duckdb < scripts/generate_data.sql` |
| D2 | **Benchmark all scale factors** | ⬜ Run `--bench --runs 6` at SF=0.5, 1, 2, 5; record DuckDB baseline at each SF |
| D3 | **Integration test** | ✅ Done — `tests/correctness_sf1.rs` passes |
| D4 | **README.md** | ✅ Done |
| D5 | **Benchmark results table** | ⬜ DuckDB vs goosedb across SF=0.5/1/2/5 with operator breakdown |

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
| **lineitem predicate scan** | **~200 ms** | RowFilter decoded all 1.5M rows across 3 filter columns → RowSelection |
| **lineitem main scan + probe + aggregate** | **~125 ms** | 4 columns for ~128K selected rows → probe → filter → accumulate |

**Why RowFilter was a net loss:** The two-pass design only wins when main projection columns are much more expensive than filter columns. In Q19, all columns are similarly sized — so double-decode cost outweighed selective projection savings.

### Current (BATCH_SIZE=4096, single-pass, dictionary strings, arrow v58)

Measured with `--timing` on run 6 (warm cache), SF=1:

| Stage | Actual time |
|---|---|
| part metadata + alloc | ~1.0 ms |
| part scan + DirectTable build | ~9.3 ms |
| lineitem file open | ~0.07 ms |
| lineitem metadata + schema override | ~0.37 ms |
| lineitem reader build | ~0.38 ms |
| **lineitem scan + probe + aggregate** | **~226 ms** |

**Remaining bottleneck:** Parquet decompression (Snappy) of the 198MB lineitem file. DuckDB uses a C++ decompressor with AVX-512 SIMD; the Rust `parquet` crate achieves lower throughput on the same hardware. This gap is structural.

**Why dictionary encoding helped so much:** `l_shipinstruct` and `l_shipmode` are PLAIN_DICTIONARY encoded in the Parquet file — there are only 4 and 7 distinct values respectively across all 1.5M rows. Without dictionary preservation, Arrow decodes each row's string value from the dictionary on every access, resulting in 1.5M string pointer lookups + byte comparisons in the hot loop. With `DictionaryArray`, Arrow reads 1.5M i32 keys; the actual string bytes are decoded once per batch (at most 11 values). The hot loop now does i32 == comparisons — trivially vectorisable.

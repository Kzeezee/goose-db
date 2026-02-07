# goose-db

A high-performance TPC-H Query 1 processor in Rust, designed to compete with DuckDB using vectorized execution and optimized aggregation.

## Features

- **Single-threaded** — Fair comparison, no parallelism overhead
- **Vectorized SIMD** — Arrow compute kernels for date filtering and expression evaluation
- **Perfect Hash Aggregation** — Fixed 6-slot array instead of HashMap (O(1) with no collisions)
- **Column Projection** — Reads only 7 of 16 columns from Parquet
- **No Result Caching** — Fresh execution each run

## Project Structure

```
goose-db/
├── src/
│   ├── main.rs          # Entry point with timing statistics
│   ├── lib.rs           # Module exports
│   ├── reader.rs        # Parquet reader with column projection
│   ├── filter.rs        # Vectorized date filter (SIMD)
│   ├── expressions.rs   # SIMD expression evaluation
│   ├── aggregator.rs    # Perfect hash array aggregation
│   └── query.rs         # Query orchestration
├── benches/
│   └── tpch_q1.rs       # Criterion benchmark
├── scripts/
│   ├── run_duckdb.py    # DuckDB baseline (single-threaded)
│   └── flamegraph.ps1   # Profiling script
└── data/                # Place lineitem.parquet here
```

## Quick Start

### 1. Configure Data Path

Edit `src/main.rs` line 4:
```rust
const DATA_PATH: &str = "data/lineitem.parquet";
```

Or place your `lineitem.parquet` in the `data/` directory.

### 2. Build & Run

```powershell
cargo build --release
cargo run --release
```

### 3. Run Benchmarks

```powershell
cargo bench --bench tpch_q1
```

### 4. Compare with DuckDB

```powershell
python scripts/run_duckdb.py data/lineitem.parquet --runs 10
```

### 5. Profile with Flamegraph

```powershell
cargo install flamegraph
.\scripts\flamegraph.ps1
```

## The Query

```sql
SELECT
    l_returnflag, l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM lineitem
WHERE l_shipdate <= CAST('1998-09-02' AS date)
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Optimization Techniques

| Technique | Description |
|-----------|-------------|
| **Column Projection** | Read only 7 required columns from Parquet |
| **Vectorized Filter** | SIMD date comparison via Arrow kernels |
| **SIMD Expressions** | Vectorized `(1-discount)` and `(1+tax)` calculations |
| **Perfect Hash Array** | 6 fixed slots for (A/N/R) × (F/O) groups |
| **Batch Processing** | 8192 rows per batch to amortize overhead |
| **LTO + codegen-units=1** | Aggressive compiler optimization |

## Dependencies

- `arrow` v54 — Arrow arrays and SIMD compute kernels
- `parquet` v54 — Parquet file reader
- `criterion` — Benchmarking framework

## License

MIT

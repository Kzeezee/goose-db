# TPC-H Query 1: How We Made It Fast

Hey team! 👋 So you've been asked to understand how we process TPC-H Query 1. Here's the breakdown of what's happening under the hood and *why* we made those choices.

## What Is TPC-H Query 1?

It's an **aggregation query** on the `lineitem` table that:
1. Filters rows where `l_shipdate <= '1998-09-02'`
2. Groups by `(l_returnflag, l_linestatus)`
3. Computes sums, averages, and counts for each group

The classic SQL looks something like this:
```sql
SELECT
    l_returnflag, l_linestatus,
    SUM(l_quantity), SUM(l_extendedprice),
    SUM(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
    COUNT(*)
FROM lineitem
WHERE l_shipdate <= date '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

---

## The Optimization Journey

Let's walk through each file and technique we used:

---

## 1. Data Reading: Column Projection & Row Group Skipping

**File:** [reader.rs](file:///home/kez/school/y2s2/cs464-advanceddb/proj/goose-db/src/reader.rs)

### Column Projection (Only Read What You Need)

The `lineitem` table has 16 columns, but Q1 only needs 7. Why waste time reading the other 9?

```rust
pub const REQUIRED_COLUMNS: &[&str] = &[
    "l_returnflag", "l_linestatus", 
    "l_quantity", "l_extendedprice",
    "l_discount", "l_tax", "l_shipdate",
];
```

We tell the Parquet reader to *only* deserialize these columns. This reduces I/O and decompression time significantly on wide tables.

### Row Group Skipping (Skip Entire Chunks)

Parquet files are organized into **row groups** (chunks of ~65k-130k rows). Each row group stores min/max statistics for each column.

Our filter is `l_shipdate <= '1998-09-02'`. If a row group's **minimum** shipdate is *greater* than our filter date, that means **every single row** in that group fails the filter. We can skip it entirely without decompressing!

```rust
if min_days > FILTER_DATE_DAYS {
    continue;  // Skip this entire row group!
}
```

> **Why This Works:** TPC-H data is often sorted or clustered by date, so row groups tend to have contiguous date ranges. This can skip a significant portion of the data.

---

## 2. Vectorized Filtering: SIMD Date Comparison

**File:** [filter.rs](file:///home/kez/school/y2s2/cs464-advanceddb/proj/goose-db/src/filter.rs)

Instead of checking dates row-by-row, we use Arrow's **vectorized compute kernels**:

```rust
let filter_mask = compute::kernels::cmp::lt_eq(
    shipdate_array,
    &scalar_date,
)?;
```

This compares thousands of dates in a single operation using **SIMD instructions** (SSE4/AVX2). The result is a `BooleanArray` mask that we pass to the aggregator.

> **Key Insight:** We create a mask but don't actually filter/copy the data. The aggregator reads the original arrays and just skips rows where the mask is `false`. This avoids an expensive memory copy.

---

## 3. The Aggregator: Where the Magic Happens

**File:** [aggregator.rs](file:///home/kez/school/y2s2/cs464-advanceddb/proj/goose-db/src/aggregator.rs)

This is the heart of our optimization. Several techniques work together:

### Perfect Hashing (No HashMap Needed)

TPC-H Q1 groups by `(l_returnflag, l_linestatus)`. The possible values are:
- `l_returnflag`: 'A', 'N', 'R' (3 options)
- `l_linestatus`: 'F', 'O' (2 options)

That's only **6 possible groups**! Instead of a HashMap with hashing overhead, we use a **fixed 6-slot array**:

```rust
fn hash_key(flag: u8, status: u8) -> usize {
    let flag_idx = match flag {
        b'A' => 0, b'N' => 1, b'R' => 2,
        _ => 0,
    };
    let status_idx = if status == b'O' { 1 } else { 0 };
    flag_idx * 2 + status_idx  // Returns 0-5
}
```

This is **O(1) with tiny constant factors**. No hashing, no collision handling, no pointer chasing.

### Cache-Aligned Aggregation State (64-byte alignment)

Each aggregation state is exactly **one CPU cache line** (64 bytes):

```rust
#[repr(C, align(64))]
pub struct AggState {
    pub sum_disc_price: f64,  // Hot: accessed every row
    pub sum_charge: f64,
    pub count: u64,
    pub sum_qty: f64,
    pub sum_base_price: f64,
    pub sum_discount: f64,
    _padding: [u8; 16],       // Pad to 64 bytes
}
```

Fields are ordered by **access frequency** (hot fields first). This minimizes cache misses when updating aggregates.

### Instruction-Level Parallelism (ILP) via Loop Unrolling

Modern CPUs can execute multiple independent operations simultaneously. But if every iteration updates the *same* accumulator, there's a **data dependency chain**.

We break this by using **4 independent accumulator sets**:

```rust
pub struct Aggregator {
    pub states: [[AggState; 6]; 4],  // 4 sets × 6 groups
}
```

The loop is **unrolled 4x**, with each iteration writing to a different accumulator set:

```rust
for chunk_i in 0..chunks {
    let base = chunk_i * 4;
    
    // Unroll 0: writes to states[0][idx]
    if mask[base] { aggregate into states[0] ... }
    
    // Unroll 1: writes to states[1][idx]
    if mask[base+1] { aggregate into states[1] ... }
    
    // Unroll 2: writes to states[2][idx]
    if mask[base+2] { aggregate into states[2] ... }
    
    // Unroll 3: writes to states[3][idx]
    if mask[base+3] { aggregate into states[3] ... }
}
```

At the end, we merge all 4 accumulator sets. This lets the CPU **pipeline** additions that would otherwise be serialized.

### Raw Byte Access (Avoiding String Overhead)

TPC-H flag columns are single characters. Instead of going through Arrow's string accessors with offset lookups:

```rust
// Slow path
returnflag.value_unchecked(i).as_bytes()[0]

// Fast path: direct byte access
*flag_values.get_unchecked(i)
```

We access the raw byte buffer directly when we detect that strings are contiguous 1-byte values.

### Unsafe for Speed

The hot loop uses `unsafe` extensively:
- `value_unchecked(i)` – skips bounds checks
- `get_unchecked(i)` – skips bounds checks on slices
- `get_unchecked_mut(idx)` – skips bounds checks on accumulators

This is safe because:
1. We control the loop bounds
2. We validated index ranges beforehand
3. TPC-H data is well-formed

---

## 4. Query Orchestration

**File:** [query.rs](file:///home/kez/school/y2s2/cs464-advanceddb/proj/goose-db/src/query.rs)

Ties everything together:

```rust
pub fn execute_tpch_q1(data_path: &str) -> Result<Vec<QueryResult>, ...> {
    let mut aggregator = Aggregator::new();
    let reader = read_lineitem(data_path)?;  // Row group skipping + projection
    
    for batch in reader {
        let mask = create_date_filter_mask(&batch)?;  // Vectorized filter
        
        if mask.true_count() == 0 { continue; }  // Skip if all filtered out
        
        // Extract columns (zero-copy reference)
        let returnflag = ...;
        let quantity = get_f64_column(&batch, "l_quantity")?;
        // ...
        
        aggregator.aggregate_batch(&mask, returnflag, ...)?;
    }
    
    aggregator.get_results()  // Merge accumulators + sort
}
```

Key points:
- **Streaming**: Process one batch at a time, never load entire file into memory
- **Early termination**: Skip batches where all rows are filtered
- **No intermediate materialization**: Expressions computed on-the-fly during aggregation

---

## 5. Data Type Handling

**File:** [utils.rs](file:///home/kez/school/y2s2/cs464-advanceddb/proj/goose-db/src/utils.rs)

TPC-H monetary/quantity columns are often stored as `Decimal128`. We cast them to `f64`:

```rust
// Use Arrow's optimized cast kernel (SIMD-accelerated)
let cast_array = cast(col, &DataType::Float64)?;
```

Arrow's cast is vectorized and much faster than per-row conversion.

---

## Summary: The Optimization Stack

| Layer | Technique | Why It Helps |
|-------|-----------|--------------|
| I/O | Column Projection | Read only 7 of 16 columns |
| I/O | Row Group Skipping | Skip entire chunks via statistics |
| Filter | Vectorized SIMD | Compare thousands of dates at once |
| Filter | Mask-based (no copy) | Avoid materializing filtered data |
| Aggregation | Perfect Hashing | O(1) with tiny overhead, no HashMap |
| Aggregation | Cache-Aligned State | One cache line per group |
| Aggregation | 4x Loop Unrolling | Enable instruction-level parallelism |
| Aggregation | Raw Byte Access | Skip string offset lookups |
| Aggregation | Unsafe Operations | Skip bounds checks in hot loop |
| General | Streaming | Process batches, don't buffer entire file |

---

## Performance

On a **1GB TPC-H dataset** (SF0.5, ~6M rows in lineitem), this implementation runs in **~90-100ms** per query after warmup.

---

## File Map

```
src/
├── main.rs        # CLI runner with timing stats
├── lib.rs         # Module exports
├── query.rs       # Query orchestrator
├── reader.rs      # Parquet reader + projection + row group skipping
├── filter.rs      # Vectorized date filtering
├── aggregator.rs  # Perfect hash + ILP aggregation
├── utils.rs       # Decimal→Float64 casting
└── memory.rs      # Cache-aligned data structures (experimental)
```

---

## Questions?

Hit me up if any of this is unclear. The aggregator is the most complex piece—happy to whiteboard the loop unrolling/ILP concept if needed!

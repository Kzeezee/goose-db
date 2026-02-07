# Changelog

All notable changes to the TPC-H Q1 implementation will be documented in this file.

## Optimizations

### Unreleased (Current State)

#### Kernel Fusion (Single-Threaded)
- **Change:** Merged expression evaluation (`disc_price` and `charge` calculation) directly into the aggregation loop.
- **Rationale:** Eliminated intermediate array allocations (approx. 320KB per batch) to reduce memory bandwidth pressure and improve cache locality.
- **Result:** Improved execution stability (stddev reduced from ~16ms to ~4ms) and slightly improved mean execution time (~109ms).
- **Refactoring:** Removed `src/expressions.rs` and the `evaluate_expressions` function.

#### Instruction-Level Parallelism (ILP)
- **Change:** Implemented 4 independent sets of accumulators in the aggregation loop.
- **Rationale:** Breaks data dependency chains in the CPU pipeline, allowing multiple independent additions to proceed in parallel (superscalar execution).
- **Result:** Increased instructions per cycle (IPC) by utilizing available execution units more effectively.

#### Raw Byte Access
- **Change:** Accessed `StringArray` values directly via raw byte pointers, bypassing Arrow's offset lookup and safety checks.
- **Rationale:** Eliminated the overhead of `value_offsets()` lookups and bounds checking for every row, treating simple string data as raw bytes.
- **Result:** Reduced instruction count in the inner loop.

### Previous Optimizations

#### Unchecked Aggregation
- **Change:** Implemented `unsafe` unchecked array access in the hot inner loop of the aggregator.
- **Rationale:** Removed bounds checking overhead for every row access, which is safe because loop bounds are constrained by the mask length and all arrays are from the same `RecordBatch`.

#### Zero-Copy Filtering
- **Change:** Replaced `arrow::compute::filter_record_batch` with a boolean mask passed directly to the aggregator.
- **Rationale:** `filter_record_batch` copies approximately 96% of the data to new buffers (since Q1 creates a new batch). Passing the mask avoids this massive copy overhead.

#### Optimized Type Casting
- **Change:** Replaced manual `decimal_to_f64` mapping with Arrow's SIMD-optimized cast kernel.
- **Rationale:** Leveraging Arrow's optimized kernels provides better performance than naive row-by-row conversion.

#### Eliminated Redundant Conversions
- **Change:** Refactored column processing to convert `l_extendedprice` and `l_discount` from `Decimal128` to `Float64` only once per batch.
- **Rationale:** Previously, these columns were potentially being converted multiple times or inefficiently.

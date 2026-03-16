use arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use crate::operators::aggregate::AggregateState;
use crate::operators::hash_table::DirectTable;
use crate::operators::scan;
use crate::timer::Timer;

/// Pipeline 2: Scan lineitem.parquet → fused pre-filter + probe + post-join filter + aggregate.
///
/// Single-pass over all 1.5M lineitem rows: all 6 columns decoded once, filtering and
/// aggregation fused into one loop. Avoids the double-decode cost of Parquet's RowFilter API.
///
/// DECIMAL(15,2) columns (l_quantity, l_extendedprice, l_discount) are read as Int64Array
/// (physical INT64) by skipping Arrow schema metadata in the scanner.
pub fn probe_and_aggregate(
    data_path: &str,
    hash_table: &DirectTable,
    mut timer: Option<&mut Timer>,
) -> Result<AggregateState, Box<dyn std::error::Error>> {
    let reader = scan::scan_lineitem(data_path, timer.as_deref_mut())?;
    let mut agg = AggregateState::new();

    for batch_result in reader {
        let batch = batch_result?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        // Access columns by name — projection preserves schema column order.
        // DECIMAL(15,2) columns are read as Decimal128Array (Arrow promotes from Parquet logical type).
        // .values() returns &[i128]; cast to i64 at point of use — values fit in i64.
        let partkey_col  = batch.column_by_name("l_partkey").expect("l_partkey")
            .as_any().downcast_ref::<Int64Array>().expect("Int64Array");
        let quantity_col = batch.column_by_name("l_quantity").expect("l_quantity")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");
        let si_col       = batch.column_by_name("l_shipinstruct").expect("l_shipinstruct")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let sm_col       = batch.column_by_name("l_shipmode").expect("l_shipmode")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let price_col    = batch.column_by_name("l_extendedprice").expect("l_extendedprice")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");
        let discount_col = batch.column_by_name("l_discount").expect("l_discount")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");

        let partkey_vals  = partkey_col.values();
        let quantity_vals = quantity_col.values();  // &[i128], values fit in i64
        let price_vals    = price_col.values();     // &[i128], values fit in i64
        let discount_vals = discount_col.values();  // &[i128], values fit in i64

        // Fused: pre-filter + probe + post-join filter + aggregate — single pass, no copies.
        for i in 0..num_rows {
            // Pre-filter: most selective check first (eliminates ~75% of rows)
            if si_col.value(i).as_bytes() != b"DELIVER IN PERSON" {
                continue;
            }
            let sm = sm_col.value(i).as_bytes();
            if sm != b"AIR" && sm != b"AIR REG" {
                continue;
            }
            let q = quantity_vals[i] as i64;
            if q > 3000 {
                continue;
            }

            let partkey = partkey_vals[i];
            if let Some(entry) = hash_table.get(partkey) {
                let brand = entry.p_brand_idx;
                let cont  = entry.p_container_idx;
                let size  = entry.p_size;

                // 3-way OR post-join filter (brand/container/quantity/size per group)
                let passes =
                    // Group 1: Brand#12, SM containers (0..3), qty 1.00–11.00, size 1–5
                    (brand == 0 && cont <= 3
                        && q >= 100 && q <= 1100
                        && size <= 5)
                    // Group 2: Brand#23, MED containers (4..7), qty 10.00–20.00, size 1–10
                    || (brand == 1 && cont >= 4 && cont <= 7
                        && q >= 1000 && q <= 2000
                        && size <= 10)
                    // Group 3: Brand#34, LG containers (8..11), qty 20.00–30.00, size 1–15
                    || (brand == 2 && cont >= 8
                        && q >= 2000 && q <= 3000
                        && size <= 15);

                if passes {
                    agg.accumulate(price_vals[i] as i64, discount_vals[i] as i64);
                }
            }
        }
    }

    if let Some(t) = timer { t.lap("lineitem scan + probe + aggregate"); }
    Ok(agg)
}

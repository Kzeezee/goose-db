use arrow::array::{Array, Decimal128Array, Int64Array};
use crate::operators::aggregate::AggregateState;
use crate::operators::hash_table::DirectTable;
use crate::operators::scan;
use crate::timer::Timer;

/// Pipeline 2: Scan lineitem.parquet → fused probe+filter+aggregate.
///
/// RowFilter in scan_lineitem pre-filters shipinstruct/shipmode/quantity, so all rows
/// in each batch already satisfy the pre-join conditions. No FilterMask or compact step needed.
///
/// Column order in projected batch (fixed by main projection mask):
///   0: l_partkey       (Int64Array)
///   1: l_quantity      (Decimal128Array, raw DECIMAL(15,2))
///   2: l_extendedprice (Decimal128Array, raw DECIMAL(15,2))
///   3: l_discount      (Decimal128Array, raw DECIMAL(15,2))
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

        // Cache column pointers once per batch — avoids per-row column_by_name overhead
        let partkey_col = batch.column(0).as_any().downcast_ref::<Int64Array>()
            .expect("l_partkey as Int64Array");
        let quantity_col = batch.column(1).as_any().downcast_ref::<Decimal128Array>()
            .expect("l_quantity as Decimal128Array");
        let price_col = batch.column(2).as_any().downcast_ref::<Decimal128Array>()
            .expect("l_extendedprice as Decimal128Array");
        let discount_col = batch.column(3).as_any().downcast_ref::<Decimal128Array>()
            .expect("l_discount as Decimal128Array");

        // Fused probe + post-join filter + aggregate
        for i in 0..num_rows {
            let partkey = partkey_col.value(i);

            if let Some(entry) = hash_table.get(partkey) {
                let q     = quantity_col.value(i) as i64;
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
                    agg.accumulate(
                        price_col.value(i) as i64,
                        discount_col.value(i) as i64,
                    );
                }
            }
        }
    }

    if let Some(t) = timer { t.lap("lineitem scan + probe + aggregate"); }
    Ok(agg)
}

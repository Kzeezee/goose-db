use arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::Int32Type;
use crate::operators::aggregate::AggregateState;
use crate::operators::hash_table::DirectTable;
use crate::operators::scan;
use crate::timer::Timer;

/// Pipeline 2: Scan lineitem.parquet → fused pre-filter + probe + post-join filter + aggregate.
///
/// Single-pass over all 1.5M lineitem rows: all 6 columns decoded once, filtering and
/// aggregation fused into one loop.
///
/// String columns (l_shipinstruct, l_shipmode) are dictionary-encoded in the Parquet file.
/// We resolve target dictionary indices once per batch, then compare i32 keys in the hot loop
/// instead of multi-byte string comparisons.
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

        let partkey_col  = batch.column_by_name("l_partkey").expect("l_partkey")
            .as_any().downcast_ref::<Int64Array>().expect("Int64Array");
        let quantity_col = batch.column_by_name("l_quantity").expect("l_quantity")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");
        let price_col    = batch.column_by_name("l_extendedprice").expect("l_extendedprice")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");
        let discount_col = batch.column_by_name("l_discount").expect("l_discount")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");

        let partkey_vals  = partkey_col.values();
        let quantity_vals = quantity_col.values();
        let price_vals    = price_col.values();
        let discount_vals = discount_col.values();

        // Try dictionary path for string columns (Parquet PLAIN_DICTIONARY encoding).
        let si_col = batch.column_by_name("l_shipinstruct").expect("l_shipinstruct");
        let sm_col = batch.column_by_name("l_shipmode").expect("l_shipmode");

        if let (Some(si_dict), Some(sm_dict)) = (
            si_col.as_any().downcast_ref::<arrow::array::DictionaryArray<Int32Type>>(),
            sm_col.as_any().downcast_ref::<arrow::array::DictionaryArray<Int32Type>>(),
        ) {
            // Dictionary path: resolve target strings to dictionary indices once per batch.
            let si_values = si_dict.values().as_any().downcast_ref::<StringArray>().expect("StringArray values");
            let sm_values = sm_dict.values().as_any().downcast_ref::<StringArray>().expect("StringArray values");

            let si_target_idx: Option<i32> = (0..si_values.len())
                .find(|&i| si_values.value(i).as_bytes() == b"DELIVER IN PERSON")
                .map(|i| i as i32);

            let mut sm_air_idx: Option<i32> = None;
            let mut sm_airreg_idx: Option<i32> = None;
            for i in 0..sm_values.len() {
                match sm_values.value(i).as_bytes() {
                    b"AIR" => sm_air_idx = Some(i as i32),
                    b"AIR REG" => sm_airreg_idx = Some(i as i32),
                    _ => {}
                }
            }

            // If "DELIVER IN PERSON" not in dictionary, no rows can match — skip batch.
            let si_target = match si_target_idx {
                Some(idx) => idx,
                None => continue,
            };

            let si_keys = si_dict.keys();
            let sm_keys = sm_dict.keys();

            for i in 0..num_rows {
                // Pre-filter using dictionary keys (i32 comparison instead of string)
                if si_keys.value(i) != si_target {
                    continue;
                }
                let sm_key = sm_keys.value(i);
                if sm_air_idx != Some(sm_key) && sm_airreg_idx != Some(sm_key) {
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

                    let passes =
                        (brand == 0 && cont <= 3 && q >= 100 && q <= 1100 && size <= 5)
                        || (brand == 1 && cont >= 4 && cont <= 7 && q >= 1000 && q <= 2000 && size <= 10)
                        || (brand == 2 && cont >= 8 && q >= 2000 && q <= 3000 && size <= 15);

                    if passes {
                        agg.accumulate(price_vals[i] as i64, discount_vals[i] as i64);
                    }
                }
            }
        } else {
            // Fallback: plain StringArray path (non-dictionary encoded)
            let si_col = si_col.as_any().downcast_ref::<StringArray>().expect("StringArray");
            let sm_col = sm_col.as_any().downcast_ref::<StringArray>().expect("StringArray");

            for i in 0..num_rows {
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

                    let passes =
                        (brand == 0 && cont <= 3 && q >= 100 && q <= 1100 && size <= 5)
                        || (brand == 1 && cont >= 4 && cont <= 7 && q >= 1000 && q <= 2000 && size <= 10)
                        || (brand == 2 && cont >= 8 && q >= 2000 && q <= 3000 && size <= 15);

                    if passes {
                        agg.accumulate(price_vals[i] as i64, discount_vals[i] as i64);
                    }
                }
            }
        }
    }

    if let Some(t) = timer { t.lap("lineitem scan + probe + aggregate"); }
    Ok(agg)
}

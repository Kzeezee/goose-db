use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use crate::encoding;
use crate::operators::hash_table::{DirectTable, HashTableEntry};
use crate::operators::scan;
use crate::timer::Timer;

/// Pipeline 1: Scan part.parquet → inline filter → encode → build direct-address lookup table.
///
/// Single-pass over all 200K part rows: all 4 columns decoded once, filter+encode+insert fused
/// into one loop. Avoids the double-decode cost of Parquet's RowFilter API.
///
/// Filter order: p_size first (cheapest int check, eliminates ~70% of rows), then brand
/// (eliminates ~97% of remaining), then container.
pub fn build_part_table(data_path: &str, mut timer: Option<&mut Timer>) -> Result<DirectTable, Box<dyn std::error::Error>> {
    let max_partkey = scan::part_row_count(data_path)?;
    let mut table = DirectTable::new(max_partkey);

    let reader = scan::scan_part(data_path)?;
    if let Some(t) = timer.as_deref_mut() { t.lap("part metadata + alloc"); }

    for batch_result in reader {
        let batch = batch_result?;
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            continue;
        }

        let partkey   = batch.column_by_name("p_partkey").expect("p_partkey")
            .as_any().downcast_ref::<Int64Array>().expect("Int64Array");
        let brand     = batch.column_by_name("p_brand").expect("p_brand")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let size      = batch.column_by_name("p_size").expect("p_size")
            .as_any().downcast_ref::<Int32Array>().expect("Int32Array");
        let container = batch.column_by_name("p_container").expect("p_container")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");

        for i in 0..num_rows {
            // Inline filter: cheapest check first (p_size eliminates ~70%)
            let s = size.value(i);
            if s < 1 || s > 15 {
                continue;
            }
            let brand_idx = encoding::brand_to_idx(brand.value(i).as_bytes());
            if brand_idx == u8::MAX {
                continue;
            }
            let cont_idx = encoding::container_to_idx(container.value(i).as_bytes());
            if cont_idx == u8::MAX {
                continue;
            }
            table.insert(HashTableEntry {
                p_partkey: partkey.value(i),
                p_brand_idx: brand_idx,
                p_size: s as u8,
                p_container_idx: cont_idx,
                _padding: [0; 5],
            });
        }
    }

    if let Some(t) = timer { t.lap("part scan + table build"); }
    Ok(table)
}

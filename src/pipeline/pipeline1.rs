use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use crate::encoding;
use crate::operators::hash_table::{DirectTable, HashTableEntry};
use crate::operators::scan;
use crate::timer::Timer;

/// Pipeline 1: Scan part.parquet → encode → build direct-address lookup table.
///
/// TPC-H p_partkey values are dense integers in [1, 200K*SF], so we use a flat
/// Vec indexed by (partkey - 1) instead of a hash table. This gives O(1) probes
/// with a single array access and no hashing or collision handling.
pub fn build_part_table(data_path: &str, mut timer: Option<&mut Timer>) -> Result<DirectTable, Box<dyn std::error::Error>> {
    // Read total part row count from metadata only (fast, no data decoded).
    // For TPC-H, row count == max_partkey since keys are sequential [1, N].
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

        // Cache column pointers once per batch
        let partkey = batch.column_by_name("p_partkey").expect("p_partkey")
            .as_any().downcast_ref::<Int64Array>().expect("Int64Array");
        let brand = batch.column_by_name("p_brand").expect("p_brand")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let size = batch.column_by_name("p_size").expect("p_size")
            .as_any().downcast_ref::<Int32Array>().expect("Int32Array");
        let container = batch.column_by_name("p_container").expect("p_container")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");

        for i in 0..num_rows {
            // RowFilter already validated brand/size/container — just encode and insert.
            table.insert(HashTableEntry {
                p_partkey: partkey.value(i),
                p_brand_idx: encoding::brand_to_idx(brand.value(i).as_bytes()),
                p_size: size.value(i) as u8,
                p_container_idx: encoding::container_to_idx(container.value(i).as_bytes()),
                _padding: [0; 5],
            });
        }
    }

    if let Some(t) = timer { t.lap("part scan + table build"); }
    Ok(table)
}

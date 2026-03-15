use arrow::array::{Array, Decimal128Array, Int32Array, RecordBatch, StringArray};
use crate::encoding;
use crate::types::masks::FilterMask;

/// Build a filter mask for part table rows.
/// Passes: p_brand IN {Brand#12, Brand#23, Brand#34}
///     AND p_size BETWEEN 1 AND 15
///     AND p_container IN {all 12 valid containers}
pub fn build_part_filter_mask(batch: &RecordBatch) -> FilterMask {
    let brand = batch.column_by_name("p_brand")
        .expect("p_brand column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("p_brand as StringArray");

    let size = batch.column_by_name("p_size")
        .expect("p_size column")
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("p_size as Int32Array");

    let container = batch.column_by_name("p_container")
        .expect("p_container column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("p_container as StringArray");

    let mut mask = FilterMask::new();
    let num_rows = batch.num_rows();

    for i in 0..num_rows {
        let s = size.value(i);
        if s < 1 || s > 15 {
            continue;
        }
        let b = brand.value(i).as_bytes();
        if encoding::brand_to_idx(b) == u8::MAX {
            continue;
        }
        let c = container.value(i).as_bytes();
        if encoding::container_to_idx(c) == u8::MAX {
            continue;
        }
        mask.set_bit(i);
    }

    mask
}

/// Build a pre-join filter mask for lineitem table rows.
/// Passes: l_shipmode IN {'AIR', 'AIR REG'}
///     AND l_shipinstruct = 'DELIVER IN PERSON'
///     AND l_quantity_raw <= 3000
pub fn build_lineitem_pre_filter_mask(batch: &RecordBatch) -> FilterMask {
    let shipmode = batch.column_by_name("l_shipmode")
        .expect("l_shipmode column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("l_shipmode as StringArray");

    let shipinstruct = batch.column_by_name("l_shipinstruct")
        .expect("l_shipinstruct column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("l_shipinstruct as StringArray");

    let quantity = batch.column_by_name("l_quantity")
        .expect("l_quantity column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("l_quantity as Decimal128Array");

    let mut mask = FilterMask::new();
    let num_rows = batch.num_rows();

    for i in 0..num_rows {
        // Check shipinstruct first (most selective for short-circuit)
        let si = shipinstruct.value(i).as_bytes();
        if si != b"DELIVER IN PERSON" {
            continue;
        }

        let sm = shipmode.value(i).as_bytes();
        if sm != b"AIR" && sm != b"AIR REG" {
            continue;
        }

        let q = quantity.value(i) as i64;
        if q > 3000 {
            continue;
        }

        mask.set_bit(i);
    }

    mask
}

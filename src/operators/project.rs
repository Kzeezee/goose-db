use arrow::array::{Array, Decimal128Array, Int64Array, RecordBatch};
use crate::types::batches::LineitemFilteredBatch;
use crate::types::masks::FilterMask;

/// Extract surviving rows from a lineitem batch into a compact LineitemFilteredBatch.
/// Only copies the 4 numeric columns needed for probe+filter+aggregate.
pub fn compact_lineitem(
    batch: &RecordBatch,
    mask: &FilterMask,
    out: &mut LineitemFilteredBatch,
) {
    out.clear();

    let partkey = batch.column_by_name("l_partkey")
        .expect("l_partkey column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("l_partkey as Int64Array");

    let quantity = batch.column_by_name("l_quantity")
        .expect("l_quantity column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("l_quantity as Decimal128Array");

    let price = batch.column_by_name("l_extendedprice")
        .expect("l_extendedprice column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("l_extendedprice as Decimal128Array");

    let discount = batch.column_by_name("l_discount")
        .expect("l_discount column")
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .expect("l_discount as Decimal128Array");

    for idx in mask.iter_set_bits() {
        out.push(
            partkey.value(idx),
            quantity.value(idx) as i64,
            price.value(idx) as i64,
            discount.value(idx) as i64,
        );
    }
}

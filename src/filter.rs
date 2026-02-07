//! Vectorized date filtering using Arrow compute kernels

use arrow::array::{Array, Date32Array, RecordBatch, Scalar};
use arrow::compute;

use crate::reader::FILTER_DATE_DAYS;

/// Apply the filter: l_shipdate <= '1998-09-02'
/// Returns a filtered RecordBatch containing only qualifying rows
pub fn apply_date_filter(batch: &RecordBatch) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Find the l_shipdate column
    let shipdate_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "l_shipdate")
        .ok_or("l_shipdate column not found")?;
    
    let shipdate_col = batch.column(shipdate_idx);
    let shipdate_array = shipdate_col
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or("l_shipdate is not Date32")?;
    
    // Create a scalar for comparison
    let scalar_date = Scalar::new(Date32Array::from(vec![FILTER_DATE_DAYS]));
    
    // Create the filter mask using SIMD-optimized comparison
    // l_shipdate <= 1998-09-02 (days since epoch = 10471)
    let filter_mask = compute::kernels::cmp::lt_eq(
        shipdate_array,
        &scalar_date,
    )?;
    
    // Apply the filter to all columns at once
    let filtered = compute::filter_record_batch(batch, &filter_mask)?;
    
    Ok(filtered)
}

/// Get the number of rows that pass the filter (for statistics)
pub fn count_matching_rows(batch: &RecordBatch) -> Result<usize, Box<dyn std::error::Error>> {
    let shipdate_idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "l_shipdate")
        .ok_or("l_shipdate column not found")?;
    
    let shipdate_col = batch.column(shipdate_idx);
    let shipdate_array = shipdate_col
        .as_any()
        .downcast_ref::<Date32Array>()
        .ok_or("l_shipdate is not Date32")?;
    
    let scalar_date = Scalar::new(Date32Array::from(vec![FILTER_DATE_DAYS]));
    
    let filter_mask = compute::kernels::cmp::lt_eq(
        shipdate_array,
        &scalar_date,
    )?;
    
    // Count true values in the mask
    Ok(filter_mask.true_count())
}

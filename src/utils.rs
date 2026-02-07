//! Utility functions for data processing

use arrow::array::{Array, Float64Array, RecordBatch};
use arrow::compute::cast;

/// Optimized helper to get a Decimal128 column by name and convert to Float64 using Arrow cast kernel
pub fn get_f64_column(batch: &RecordBatch, name: &str) -> Result<Float64Array, Box<dyn std::error::Error>> {
    let idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .ok_or_else(|| format!("Column {} not found", name))?;
    
    let col = batch.column(idx);
    
    // Check if it's already F64, otherwise try to cast
    if col.data_type() == &arrow::datatypes::DataType::Float64 {
        return Ok(col.as_any().downcast_ref::<Float64Array>().unwrap().clone());
    }

    // Use optimized Arrow cast kernel
    // Since Decimal128 -> Float64 is supported, this should be very fast (SIMD)
    let cast_array = cast(col, &arrow::datatypes::DataType::Float64)?;
    
    Ok(cast_array.as_any().downcast_ref::<Float64Array>().unwrap().clone())
}

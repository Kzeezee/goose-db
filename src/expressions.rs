//! Vectorized expression evaluation using Arrow SIMD kernels

use arrow::array::{Array, Float64Array, RecordBatch, AsArray, Decimal128Array};
use arrow::compute::kernels::numeric;
use arrow::datatypes::Float64Type;
use std::sync::Arc;

/// Computed expressions for TPC-H Q1
pub struct ComputedExpressions {
    /// l_extendedprice * (1 - l_discount)
    pub disc_price: Float64Array,
    /// l_extendedprice * (1 - l_discount) * (1 + l_tax)  
    pub charge: Float64Array,
}

/// Evaluate expressions using SIMD-optimized Arrow kernels
/// Computes:
///   disc_price = l_extendedprice * (1 - l_discount)
///   charge = disc_price * (1 + l_tax)
pub fn evaluate_expressions(batch: &RecordBatch) -> Result<ComputedExpressions, Box<dyn std::error::Error>> {
    // Get column references
    let price = get_f64_column(batch, "l_extendedprice")?;
    let discount = get_f64_column(batch, "l_discount")?;
    let tax = get_f64_column(batch, "l_tax")?;
    
    let len = batch.num_rows();
    
    // Build scalar arrays for the constant 1.0
    let ones: Float64Array = vec![1.0f64; len].into();
    
    // Vectorized: (1 - discount)
    let one_minus_discount_arc = numeric::sub(&ones, &discount)?;
    let one_minus_discount = one_minus_discount_arc.as_primitive::<Float64Type>().clone();
    
    // Vectorized: price * (1 - discount)
    let disc_price_arc = numeric::mul(&price, &one_minus_discount)?;
    let disc_price = disc_price_arc.as_primitive::<Float64Type>().clone();
    
    // Vectorized: (1 + tax)
    let one_plus_tax_arc = numeric::add(&ones, &tax)?;
    let one_plus_tax = one_plus_tax_arc.as_primitive::<Float64Type>().clone();
    
    // Vectorized: disc_price * (1 + tax)
    let charge_arc = numeric::mul(&disc_price, &one_plus_tax)?;
    let charge = charge_arc.as_primitive::<Float64Type>().clone();
    
    Ok(ComputedExpressions {
        disc_price,
        charge,
    })
}

/// Helper to get a Decimal128 column by name and convert to Float64
fn get_f64_column(batch: &RecordBatch, name: &str) -> Result<Float64Array, Box<dyn std::error::Error>> {
    let idx = batch
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == name)
        .ok_or_else(|| format!("Column {} not found", name))?;
    
    let col = batch.column(idx);
    let arr = col
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .ok_or_else(|| format!("Column {} is not Decimal128", name))?;
    
    Ok(decimal_to_f64(arr))
}

/// Convert Decimal128Array to Float64Array
/// Assumes scale of 2 for DECIMAL(15,2)
fn decimal_to_f64(arr: &Decimal128Array) -> Float64Array {
    let scale = 10_f64.powi(arr.scale() as i32);
    Float64Array::from_iter_values(
        arr.iter().map(|v| {
            v.map(|d| d as f64 / scale).unwrap_or(0.0)
        })
    )
}

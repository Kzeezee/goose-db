//! Vectorized expression evaluation using Arrow SIMD kernels

use arrow::array::{Array, Float64Array, AsArray};
use arrow::compute::kernels::numeric;
use arrow::datatypes::Float64Type;

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
pub fn evaluate_expressions(
    price: &Float64Array,
    discount: &Float64Array,
    tax: &Float64Array,
) -> Result<ComputedExpressions, Box<dyn std::error::Error>> {
    let len = price.len();
    
    // Build scalar arrays for the constant 1.0
    // Note: Creating a scalar array for every batch might still be slightly costly, 
    // but likely negligible compared to allocations. Ideally pre-allocate or loop.
    let ones: Float64Array = vec![1.0f64; len].into();
    
    // Vectorized: (1 - discount)
    let one_minus_discount_arc = numeric::sub(&ones, discount)?;
    let one_minus_discount = one_minus_discount_arc.as_primitive::<Float64Type>();
    
    // Vectorized: price * (1 - discount)
    let disc_price_arc = numeric::mul(price, one_minus_discount)?;
    let disc_price = disc_price_arc.as_primitive::<Float64Type>().clone();
    
    // Vectorized: (1 + tax)
    let one_plus_tax_arc = numeric::add(&ones, tax)?;
    let one_plus_tax = one_plus_tax_arc.as_primitive::<Float64Type>();
    
    // Vectorized: disc_price * (1 + tax)
    let charge_arc = numeric::mul(&disc_price, one_plus_tax)?;
    let charge = charge_arc.as_primitive::<Float64Type>().clone();
    
    Ok(ComputedExpressions {
        disc_price,
        charge,
    })
}

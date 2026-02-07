//! Query orchestration - ties together all components

use crate::aggregator::{Aggregator, QueryResult};
use crate::expressions::evaluate_expressions;
use crate::filter::apply_date_filter;
use crate::reader::read_lineitem;

/// Execute TPC-H Query 1
/// 
/// Returns the query results sorted by (l_returnflag, l_linestatus)
pub fn execute_tpch_q1(data_path: &str) -> Result<Vec<QueryResult>, Box<dyn std::error::Error>> {
    // Initialize aggregator with perfect hash array
    let mut aggregator = Aggregator::new();
    
    // Read parquet file with column projection (no caching)
    let reader = read_lineitem(data_path)?;
    
    // Process batches sequentially
    for batch_result in reader {
        let batch = batch_result?;
        
        // Skip empty batches
        if batch.num_rows() == 0 {
            continue;
        }
        
        // Apply vectorized date filter: l_shipdate <= '1998-09-02'
        let filtered = apply_date_filter(&batch)?;
        
        // Skip if no rows pass filter
        if filtered.num_rows() == 0 {
            continue;
        }
        
        // Evaluate expressions with SIMD kernels
        let expressions = evaluate_expressions(&filtered)?;
        
        // Aggregate into perfect hash array
        aggregator.aggregate_batch(&filtered, &expressions.disc_price, &expressions.charge)?;
    }
    
    // Get sorted results
    let results = aggregator.get_results();
    
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    // Integration test would go here with sample data
}

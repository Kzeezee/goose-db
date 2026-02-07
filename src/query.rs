//! Query orchestration - ties together all components

use crate::aggregator::{Aggregator, QueryResult};

use crate::reader::read_lineitem;
use arrow::array::Array;

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
        
        // Create filter mask: l_shipdate <= '1998-09-02'
        let mask = crate::filter::create_date_filter_mask(&batch)?;
        
        // Skip if everything filtered out (optimization)
        if mask.true_count() == 0 {
            continue;
        }

        // Get typed arrays from ORIGINAL batch (no copy)
        // Since we are filtering inside the loop, we work with the full batch arrays
        let returnflag = batch
            .column(batch.schema().index_of("l_returnflag")?)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("l_returnflag is not String")?;
            
        let linestatus = batch
            .column(batch.schema().index_of("l_linestatus")?)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .ok_or("l_linestatus is not String")?;
        
        let quantity = crate::utils::get_f64_column(&batch, "l_quantity")?;
        let price = crate::utils::get_f64_column(&batch, "l_extendedprice")?;
        let discount = crate::utils::get_f64_column(&batch, "l_discount")?;
        let tax = crate::utils::get_f64_column(&batch, "l_tax")?;
        
        // Aggregate into perfect hash array using the mask
        // Expressions (disc_price, charge) are computed on the fly inside aggregate_batch
        aggregator.aggregate_batch(
            &mask,
            returnflag,
            linestatus,
            &quantity,
            &price,
            &discount,
            &tax,
        )?;
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

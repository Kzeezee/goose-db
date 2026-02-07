//! Parquet reader with column projection and row group pruning

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::fs::File;

/// Columns we need for TPC-H Q1
pub const REQUIRED_COLUMNS: &[&str] = &[
    "l_returnflag",
    "l_linestatus", 
    "l_quantity",
    "l_extendedprice",
    "l_discount",
    "l_tax",
    "l_shipdate",
];

/// The filter date: 1998-09-02 as days since epoch
/// 1998-09-02 = days since 1970-01-01 = 10471
pub const FILTER_DATE_DAYS: i32 = 10471;

/// Read parquet file with column projection
/// Returns an iterator over record batches
pub fn read_lineitem(path: &str) -> Result<LineitemReader, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    
    // Get arrow schema and projection indices FIRST
    let arrow_schema = builder.schema().clone();
    
    // Find indices of required columns
    let projection_indices: Vec<usize> = REQUIRED_COLUMNS
        .iter()
        .map(|col_name| {
            arrow_schema
                .fields()
                .iter()
                .position(|f| f.name() == *col_name)
                .expect(&format!("Column {} not found", col_name))
        })
        .collect();
    
    // Row Group Skipping: Filter out row groups that don't match our predicate
    
    // Get the parquet schema to find column indices - SCOPE 1
    let shipdate_idx = {
        let parquet_schema = builder.parquet_schema();
        parquet_schema
            .columns()
            .iter()
            .position(|c| c.name() == "l_shipdate")
            .expect("l_shipdate not found in parquet schema")
    };

    // 2. Iterate over row groups and check statistics - SCOPE 2
    let row_groups_to_read = {
        let metadata = builder.metadata();
        let mut groups = Vec::new();

        for (i, rg) in metadata.row_groups().iter().enumerate() {
            if let Some(stats) = rg.column(shipdate_idx).statistics() {
                // valid way for deprecated min_bytes:
                let min_val = stats.min_bytes();
                 
                if min_val.len() == 4 {
                    let min_days = i32::from_le_bytes(min_val.try_into().unwrap());
                    
                    if min_days > FILTER_DATE_DAYS {
                        continue;
                    }
                }
            }
            groups.push(i);
        }
        groups
    };
    
    // Apply the row group filter - consumes builder
    builder = builder.with_row_groups(row_groups_to_read);

    // Get schema again from new builder for projection
    let parquet_schema = builder.parquet_schema();
    
    // Create projection mask
    let projection = ProjectionMask::roots(parquet_schema, projection_indices.clone());
    
    // Build reader with projection and reasonable batch size
    let reader = builder
        .with_projection(projection)
        .with_batch_size(8192)
        .build()?;
    
    Ok(LineitemReader {
        inner: reader,
        schema: arrow_schema,
    })
}

pub struct LineitemReader {
    inner: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    schema: SchemaRef,
}

impl Iterator for LineitemReader {
    type Item = Result<RecordBatch, ArrowError>;
    
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl LineitemReader {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_filter_date() {
        // Verify our date constant
        use chrono::NaiveDate;
        let date = NaiveDate::from_ymd_opt(1998, 9, 2).unwrap();
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let days = (date - epoch).num_days() as i32;
        assert_eq!(days, FILTER_DATE_DAYS);
    }
}

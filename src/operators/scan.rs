use crate::config::BATCH_SIZE;
use crate::timer::Timer;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use std::fs::File;
use std::path::Path;

/// Read only the Parquet file metadata to get total row count — no data is decoded.
/// For TPC-H part, row count == max_partkey (keys are dense [1, N]).
pub fn part_row_count(data_path: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let path = Path::new(data_path).join("part.parquet");
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    Ok(builder.metadata().file_metadata().num_rows() as usize)
}

fn col_idx(schema: &SchemaDescriptor, name: &str) -> usize {
    schema
        .columns()
        .iter()
        .position(|c| c.name() == name)
        .unwrap_or_else(|| panic!("Column '{}' not found in Parquet schema", name))
}

/// Scan lineitem.parquet — single-pass, all 6 needed columns, no RowFilter.
///
/// Reads physical types directly (INT64 for DECIMAL columns) by skipping the
/// embedded Arrow schema metadata. Filtering is done inline by the caller.
///
/// Columns in result batch: l_partkey, l_quantity, l_shipinstruct, l_shipmode,
///                          l_extendedprice, l_discount  (all Int64Array or StringArray)
pub fn scan_lineitem(
    data_path: &str,
    mut timer: Option<&mut Timer>,
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, Box<dyn std::error::Error>> {
    let path = Path::new(data_path).join("lineitem.parquet");
    let file = File::open(&path)?;
    if let Some(t) = timer.as_deref_mut() { t.lap("lineitem file open"); }

    // skip_arrow_metadata: skips the embedded Arrow IPC schema in the Parquet file metadata.
    // NOTE: DECIMAL(15,2) columns are still returned as Decimal128Array due to Parquet logical
    // type annotations — skip_arrow_metadata alone does not override this.
    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    if let Some(t) = timer.as_deref_mut() { t.lap("lineitem metadata parse (try_new)"); }

    let schema = builder.parquet_schema();
    let si_idx   = col_idx(schema, "l_shipinstruct");
    let sm_idx   = col_idx(schema, "l_shipmode");
    let qty_idx  = col_idx(schema, "l_quantity");
    let pk_idx   = col_idx(schema, "l_partkey");
    let ep_idx   = col_idx(schema, "l_extendedprice");
    let disc_idx = col_idx(schema, "l_discount");

    // Project all 6 needed columns in one pass — no RowFilter, no double decode.
    let main_mask = ProjectionMask::roots(schema, [pk_idx, qty_idx, si_idx, sm_idx, ep_idx, disc_idx]);

    let reader = builder
        .with_projection(main_mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;
    if let Some(t) = timer { t.lap("lineitem reader build"); }

    Ok(reader.into_iter())
}

/// Scan part.parquet — single-pass, all 4 needed columns, no RowFilter.
///
/// Skips the embedded Arrow IPC schema metadata. Filtering is done inline by the caller.
pub fn scan_part(
    data_path: &str,
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, Box<dyn std::error::Error>> {
    let path = Path::new(data_path).join("part.parquet");
    let file = File::open(&path)?;

    let options = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options)?;
    let schema = builder.parquet_schema();

    let pk_idx    = col_idx(schema, "p_partkey");
    let brand_idx = col_idx(schema, "p_brand");
    let size_idx  = col_idx(schema, "p_size");
    let cont_idx  = col_idx(schema, "p_container");

    // Project all 4 columns — no RowFilter, caller does inline filtering.
    let main_mask = ProjectionMask::roots(schema, [pk_idx, brand_idx, size_idx, cont_idx]);

    let reader = builder
        .with_projection(main_mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    Ok(reader.into_iter())
}

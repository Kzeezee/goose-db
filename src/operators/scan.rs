use crate::config::BATCH_SIZE;
use crate::timer::Timer;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

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
/// Uses `with_schema()` to preserve dictionary encoding on string columns (l_shipinstruct,
/// l_shipmode), enabling u32 key comparison instead of multi-byte string comparison in the
/// hot loop. DECIMAL(15,2) columns are returned as Decimal128Array.
pub fn scan_lineitem(
    data_path: &str,
    mut timer: Option<&mut Timer>,
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, Box<dyn std::error::Error>> {
    let path = Path::new(data_path).join("lineitem.parquet");
    let file = File::open(&path)?;
    if let Some(t) = timer.as_deref_mut() { t.lap("lineitem file open"); }

    // First pass: get auto-derived Arrow schema to learn column types.
    let options_probe = ArrowReaderOptions::new().with_skip_arrow_metadata(true);
    let builder_probe = ParquetRecordBatchReaderBuilder::try_new_with_options(file, options_probe)?;
    let original_schema = builder_probe.schema().clone();
    let parquet_schema = builder_probe.parquet_schema().clone();

    // Modify schema: request Dictionary(Int32, Utf8) for string columns to preserve
    // Parquet dictionary encoding. This avoids decoding to full StringArray.
    let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let modified_fields: Vec<Arc<Field>> = original_schema.fields().iter().map(|f| {
        match f.name().as_str() {
            "l_shipinstruct" | "l_shipmode" => {
                Arc::new(
                    Field::new(f.name(), dict_type.clone(), f.is_nullable())
                        .with_metadata(f.metadata().clone()),
                )
            }
            _ => f.clone(),
        }
    }).collect();
    let modified_schema = Arc::new(Schema::new_with_metadata(
        modified_fields,
        original_schema.metadata().clone(),
    ));

    if let Some(t) = timer.as_deref_mut() { t.lap("lineitem metadata + schema override"); }

    // Second pass: re-open file with modified schema.
    let file2 = File::open(&path)?;
    let options = ArrowReaderOptions::new().with_schema(modified_schema);
    let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(file2, options)?;

    let si_idx   = col_idx(&parquet_schema, "l_shipinstruct");
    let sm_idx   = col_idx(&parquet_schema, "l_shipmode");
    let qty_idx  = col_idx(&parquet_schema, "l_quantity");
    let pk_idx   = col_idx(&parquet_schema, "l_partkey");
    let ep_idx   = col_idx(&parquet_schema, "l_extendedprice");
    let disc_idx = col_idx(&parquet_schema, "l_discount");

    let main_mask = ProjectionMask::roots(&parquet_schema, [pk_idx, qty_idx, si_idx, sm_idx, ep_idx, disc_idx]);

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

    let main_mask = ProjectionMask::roots(schema, [pk_idx, brand_idx, size_idx, cont_idx]);

    let reader = builder
        .with_projection(main_mask)
        .with_batch_size(BATCH_SIZE)
        .build()?;

    Ok(reader.into_iter())
}

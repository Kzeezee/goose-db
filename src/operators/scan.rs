use crate::config::BATCH_SIZE;
use crate::timer::Timer;
use arrow::array::{BooleanBuilder, Decimal128Array, Int32Array, StringArray};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter};
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

/// Scan lineitem.parquet with predicate pushdown via RowFilter.
///
/// Single combined predicate reads l_shipinstruct, l_shipmode, l_quantity in one pass.
/// Passes rows where: l_shipinstruct = 'DELIVER IN PERSON'
///                AND l_shipmode IN {'AIR', 'AIR REG'}
///                AND l_quantity_raw <= 3000
///
/// Main projection: l_partkey, l_quantity, l_extendedprice, l_discount
/// l_shipinstruct and l_shipmode are NOT in the main projection.
pub fn scan_lineitem(
    data_path: &str,
    mut timer: Option<&mut Timer>,
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, Box<dyn std::error::Error>> {
    let path = Path::new(data_path).join("lineitem.parquet");
    let file = File::open(&path)?;
    if let Some(t) = timer.as_deref_mut() { t.lap("lineitem file open"); }
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    if let Some(t) = timer.as_deref_mut() { t.lap("lineitem metadata parse (try_new)"); }
    let schema = builder.parquet_schema();

    let si_idx   = col_idx(schema, "l_shipinstruct");
    let sm_idx   = col_idx(schema, "l_shipmode");
    let qty_idx  = col_idx(schema, "l_quantity");
    let pk_idx   = col_idx(schema, "l_partkey");
    let ep_idx   = col_idx(schema, "l_extendedprice");
    let disc_idx = col_idx(schema, "l_discount");

    // Single predicate reads all 3 filter columns together (one pass, not three)
    let pred_mask = ProjectionMask::roots(schema, [si_idx, sm_idx, qty_idx]);
    let main_mask = ProjectionMask::roots(schema, [pk_idx, qty_idx, ep_idx, disc_idx]);

    let predicate = ArrowPredicateFn::new(pred_mask, |batch| {
        let si  = batch.column_by_name("l_shipinstruct").expect("l_shipinstruct")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let sm  = batch.column_by_name("l_shipmode").expect("l_shipmode")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let qty = batch.column_by_name("l_quantity").expect("l_quantity")
            .as_any().downcast_ref::<Decimal128Array>().expect("Decimal128Array");

        let n = batch.num_rows();
        let mut builder = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            let passes = si.value(i).as_bytes() == b"DELIVER IN PERSON"
                && { let b = sm.value(i).as_bytes(); b == b"AIR" || b == b"AIR REG" }
                && qty.value(i) as i64 <= 3000;
            builder.append_value(passes);
        }
        Ok(builder.finish())
    });

    let reader = builder
        .with_projection(main_mask)
        .with_row_filter(RowFilter::new(vec![Box::new(predicate)]))
        .with_batch_size(BATCH_SIZE)
        .build()?;
    // NOTE: build() eagerly evaluates the RowFilter predicate on ALL rows,
    // reading 3 filter columns for 1.5M rows to produce a RowSelection.
    // This is where most of the lineitem I/O cost is incurred.
    if let Some(t) = timer { t.lap("lineitem predicate scan (build)"); }

    Ok(reader.into_iter())
}

/// Scan part.parquet with predicate pushdown via RowFilter.
///
/// RowFilter predicate: p_brand IN {Brand#12/23/34} AND p_size 1-15 AND p_container IN {12 values}
/// Main projection: p_partkey, p_brand, p_size, p_container
pub fn scan_part(
    data_path: &str,
) -> Result<impl Iterator<Item = Result<RecordBatch, ArrowError>>, Box<dyn std::error::Error>> {
    let path = Path::new(data_path).join("part.parquet");
    let file = File::open(&path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let schema = builder.parquet_schema();

    let pk_idx    = col_idx(schema, "p_partkey");
    let brand_idx = col_idx(schema, "p_brand");
    let size_idx  = col_idx(schema, "p_size");
    let cont_idx  = col_idx(schema, "p_container");

    // Single predicate batching all three part filters
    let filter_mask = ProjectionMask::roots(schema, [brand_idx, size_idx, cont_idx]);
    let part_pred = ArrowPredicateFn::new(filter_mask, |batch| {
        // Predicate batch has only brand/size/container; access by name since order may vary
        let brand = batch.column_by_name("p_brand").expect("p_brand")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");
        let size = batch.column_by_name("p_size").expect("p_size")
            .as_any().downcast_ref::<Int32Array>().expect("Int32Array");
        let cont = batch.column_by_name("p_container").expect("p_container")
            .as_any().downcast_ref::<StringArray>().expect("StringArray");

        let n = batch.num_rows();
        let mut builder = BooleanBuilder::with_capacity(n);
        for i in 0..n {
            let s = size.value(i);
            let passes = s >= 1 && s <= 15
                && matches!(brand.value(i).as_bytes(),
                    b"Brand#12" | b"Brand#23" | b"Brand#34")
                && matches!(cont.value(i).as_bytes(),
                    b"SM CASE" | b"SM BOX" | b"SM PACK" | b"SM PKG"
                    | b"MED BAG" | b"MED BOX" | b"MED PKG" | b"MED PACK"
                    | b"LG CASE" | b"LG BOX" | b"LG PACK" | b"LG PKG");
            builder.append_value(passes);
        }
        Ok(builder.finish())
    });

    let main_mask = ProjectionMask::roots(schema, [pk_idx, brand_idx, size_idx, cont_idx]);

    let reader = builder
        .with_projection(main_mask)
        .with_row_filter(RowFilter::new(vec![Box::new(part_pred)]))
        .with_batch_size(BATCH_SIZE)
        .build()?;

    Ok(reader.into_iter())
}

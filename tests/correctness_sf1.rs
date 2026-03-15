/// Integration test: run Q19 on SF=1 data and verify the result matches DuckDB exactly.
///
/// Expected result: 3083843.0578
/// Verified against: DuckDB 1.x, PRAGMA threads=1, same lineitem.parquet + part.parquet
///
/// To run: cargo test --test correctness_sf1
/// (requires data/sf1/lineitem.parquet and data/sf1/part.parquet to exist)
use goosedb::pipeline;

#[test]
fn q19_sf1_result_matches_duckdb() {
    let data_path = "data/sf1";

    // Skip if data doesn't exist (CI without data files)
    if !std::path::Path::new(data_path).join("lineitem.parquet").exists() {
        eprintln!("Skipping: data/sf1/lineitem.parquet not found");
        return;
    }

    let table = pipeline::pipeline1::build_part_table(data_path, None)
        .expect("pipeline1 failed");

    let agg = pipeline::pipeline2::probe_and_aggregate(data_path, &table, None)
        .expect("pipeline2 failed");

    let revenue = agg.finalise();

    // DuckDB result at SF=1 (exact, integer arithmetic, 4 decimal places)
    let expected = 3083843.0578_f64;

    assert!(
        (revenue - expected).abs() < 0.001,
        "Revenue mismatch: got {:.4}, expected {:.4}",
        revenue,
        expected
    );

    // Verify to 4 decimal places (matches DuckDB DECIMAL(38,4) output)
    assert_eq!(
        format!("{:.4}", revenue),
        "3083843.0578",
        "4-decimal-place formatted output mismatch"
    );
}

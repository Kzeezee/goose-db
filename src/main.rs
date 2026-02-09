use std::time::Instant;
use goose_db::query::execute_tpch_q1;

/// Configure your data path here
const DATA_PATH: &str = "/home/kez/school/y2s2/cs464-advanceddb/proj/goose-db/data/lineitem.parquet";

/// Number of benchmark runs
const NUM_RUNS: usize = 10;

fn main() {
    println!("TPC-H Query 1 Processor");
    println!("=======================");
    println!("Data path: {}", DATA_PATH);
    println!();

    // Warmup run (not counted)
    println!("Warmup run...");
    let _ = execute_tpch_q1(DATA_PATH);
    println!();

    // Benchmark runs
    let mut times = Vec::with_capacity(NUM_RUNS);
    
    for i in 1..=NUM_RUNS {
        let start = Instant::now();
        let result = execute_tpch_q1(DATA_PATH).expect("Query execution failed");
        let elapsed = start.elapsed();
        times.push(elapsed.as_secs_f64() * 1000.0); // Convert to ms
        
        if i == NUM_RUNS {
            // Print results on last run
            println!("Query Results:");
            println!("{:-<100}", "");
            println!(
                "{:<12} {:<12} {:>15} {:>18} {:>18} {:>18} {:>12} {:>12} {:>10} {:>12}",
                "returnflag", "linestatus", "sum_qty", "sum_base_price", 
                "sum_disc_price", "sum_charge", "avg_qty", "avg_price", 
                "avg_disc", "count"
            );
            println!("{:-<100}", "");
            
            for row in &result {
                println!(
                    "{:<12} {:<12} {:>15.2} {:>18.2} {:>18.2} {:>18.2} {:>12.2} {:>12.2} {:>10.2} {:>12}",
                    row.returnflag as char,
                    row.linestatus as char,
                    row.sum_qty,
                    row.sum_base_price,
                    row.sum_disc_price,
                    row.sum_charge,
                    row.avg_qty,
                    row.avg_price,
                    row.avg_disc,
                    row.count
                );
            }
            println!();
        }
    }

    // Statistics
    let mean = times.iter().sum::<f64>() / times.len() as f64;
    let variance = times.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / times.len() as f64;
    let stddev = variance.sqrt();
    let min = times.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = times.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    println!("Performance ({} runs):", NUM_RUNS);
    println!("{:-<40}", "");
    println!("  Mean:   {:.2} ms", mean);
    println!("  Stddev: {:.2} ms", stddev);
    println!("  Min:    {:.2} ms", min);
    println!("  Max:    {:.2} ms", max);
}

use clap::Parser;
use goosedb::config::Args;
use goosedb::pipeline;
use goosedb::timer::Timer;
use std::error::Error;
use std::time::Instant;

fn execute_query(data_path: &str, mut t: Option<&mut Timer>) -> Result<f64, Box<dyn Error>> {
    let table = pipeline::pipeline1::build_part_table(data_path, t.as_deref_mut())?;
    let agg = pipeline::pipeline2::probe_and_aggregate(data_path, &table, t.as_deref_mut())?;
    Ok(agg.finalise())
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    let mut result = 0.0_f64;
    let mut timings = Vec::new();

    let total_runs = if args.bench { args.runs } else { 1 };

    for run in 0..total_runs {
        // Only collect operator timing on the last run (avoids noise in earlier runs)
        let is_last = run + 1 == total_runs;
        let mut t = if args.timing && is_last { Some(Timer::new()) } else { None };

        let start = Instant::now();
        result = execute_query(&args.data, t.as_mut())?;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        if args.bench {
            if run == 0 {
                eprintln!("Run {} (warmup): {:.2} ms", run + 1, elapsed_ms);
            } else {
                eprintln!("Run {}: {:.2} ms", run + 1, elapsed_ms);
                timings.push(elapsed_ms);
            }
        }

        if let Some(ref t) = t {
            eprintln!("Operator breakdown (run {}):", run + 1);
            t.report();
        }
    }

    if args.bench && !timings.is_empty() {
        let mean_ms = timings.iter().sum::<f64>() / timings.len() as f64;
        eprintln!("Mean (runs 2-{}): {:.2} ms", total_runs, mean_ms);
    }

    // Write CSV output (4 decimal places to match DuckDB DECIMAL(38,4) output)
    let mut wtr = csv::Writer::from_path(&args.out)?;
    wtr.write_record(&["revenue"])?;
    wtr.write_record(&[format!("{:.4}", result)])?;
    wtr.flush()?;

    println!("{:.4}", result);

    Ok(())
}

use clap::Parser;

/// Batch size for vectorised processing. 4096 balances L2 cache fit with batch overhead.
/// With 6 Int64/String columns at 4096 rows ≈ 192KB numeric data — fits in L2.
pub const BATCH_SIZE: usize = 4096;

/// Number of u64 words in a FilterMask bitmask.
pub const MASK_WORDS: usize = BATCH_SIZE / 64;

#[derive(Parser, Debug)]
#[command(name = "goosedb", about = "TPC-H Query 19 specialised processor")]
pub struct Args {
    /// Path to directory containing lineitem.parquet and part.parquet
    #[arg(long)]
    pub data: String,

    /// Path for output CSV file
    #[arg(long)]
    pub out: String,

    /// Enable benchmark mode (run query multiple times)
    #[arg(long, default_value_t = false)]
    pub bench: bool,

    /// Number of runs in benchmark mode (run 1 is warmup)
    #[arg(long, default_value_t = 1)]
    pub runs: usize,

    /// Print operator-level timing breakdown (only on last run)
    #[arg(long, default_value_t = false)]
    pub timing: bool,
}

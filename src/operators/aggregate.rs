/// Ungrouped aggregate state for revenue accumulation.
/// Uses i128 to prevent overflow; single f64 conversion at output.
pub struct AggregateState {
    pub accumulator: i128,
    pub row_count: u64,
}

impl AggregateState {
    pub fn new() -> Self {
        Self {
            accumulator: 0,
            row_count: 0,
        }
    }

    /// Accumulate revenue from raw DECIMAL(15,2) integer values.
    /// revenue_scaled = extendedprice_raw * (100 - discount_raw)
    #[inline]
    pub fn accumulate(&mut self, extendedprice_raw: i64, discount_raw: i64) {
        self.accumulator += extendedprice_raw as i128 * (100 - discount_raw) as i128;
        self.row_count += 1;
    }

    /// Convert accumulated integer sum to final f64 revenue.
    /// Scale factor is 10_000 (100 * 100 from the two DECIMAL(15,2) values).
    pub fn finalise(&self) -> f64 {
        self.accumulator as f64 / 10_000.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_accumulation() {
        let mut agg = AggregateState::new();
        // price = $10.00 (raw 1000), discount = 5% (raw 5)
        // revenue = 1000 * (100 - 5) = 95000 scaled units
        // final = 95000 / 10000 = $9.50
        agg.accumulate(1000, 5);
        assert_eq!(agg.row_count, 1);
        assert_eq!(agg.accumulator, 95000);
        assert!((agg.finalise() - 9.50).abs() < 1e-10);
    }

    #[test]
    fn test_multiple_accumulations() {
        let mut agg = AggregateState::new();
        agg.accumulate(1000, 5);  // 95000
        agg.accumulate(2000, 10); // 2000 * 90 = 180000
        assert_eq!(agg.row_count, 2);
        assert_eq!(agg.accumulator, 275000);
        assert!((agg.finalise() - 27.50).abs() < 1e-10);
    }

    #[test]
    fn test_zero_discount() {
        let mut agg = AggregateState::new();
        agg.accumulate(5000, 0); // 5000 * 100 = 500000 -> $50.00
        assert!((agg.finalise() - 50.00).abs() < 1e-10);
    }
}

/// Compact batch of lineitem rows that survived the pre-join filter.
/// Reused across batches to avoid repeated allocation.
pub struct LineitemFilteredBatch {
    pub l_partkey: Vec<i64>,
    pub l_quantity_raw: Vec<i64>,
    pub l_extendedprice_raw: Vec<i64>,
    pub l_discount_raw: Vec<i64>,
    pub count: u16,
}

impl LineitemFilteredBatch {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            l_partkey: Vec::with_capacity(cap),
            l_quantity_raw: Vec::with_capacity(cap),
            l_extendedprice_raw: Vec::with_capacity(cap),
            l_discount_raw: Vec::with_capacity(cap),
            count: 0,
        }
    }

    pub fn clear(&mut self) {
        self.l_partkey.clear();
        self.l_quantity_raw.clear();
        self.l_extendedprice_raw.clear();
        self.l_discount_raw.clear();
        self.count = 0;
    }

    #[inline]
    pub fn push(&mut self, partkey: i64, quantity_raw: i64, price_raw: i64, discount_raw: i64) {
        self.l_partkey.push(partkey);
        self.l_quantity_raw.push(quantity_raw);
        self.l_extendedprice_raw.push(price_raw);
        self.l_discount_raw.push(discount_raw);
        self.count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_clear() {
        let mut batch = LineitemFilteredBatch::with_capacity(16);
        batch.push(100, 200, 300, 400);
        batch.push(101, 201, 301, 401);
        assert_eq!(batch.count, 2);
        assert_eq!(batch.l_partkey[0], 100);
        assert_eq!(batch.l_discount_raw[1], 401);

        batch.clear();
        assert_eq!(batch.count, 0);
        assert!(batch.l_partkey.is_empty());
        // Capacity preserved after clear
        assert!(batch.l_partkey.capacity() >= 16);
    }
}

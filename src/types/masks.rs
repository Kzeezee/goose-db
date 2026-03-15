use crate::config::MASK_WORDS;

pub struct FilterMask {
    pub bitmask: [u64; MASK_WORDS],
    pub set_count: u16,
}

impl FilterMask {
    pub fn new() -> Self {
        Self {
            bitmask: [0u64; MASK_WORDS],
            set_count: 0,
        }
    }

    #[inline]
    pub fn set_bit(&mut self, idx: usize) {
        let word = idx / 64;
        let bit = idx % 64;
        // Only increment count if bit was not already set
        if self.bitmask[word] & (1u64 << bit) == 0 {
            self.bitmask[word] |= 1u64 << bit;
            self.set_count += 1;
        }
    }

    #[inline]
    pub fn is_set(&self, idx: usize) -> bool {
        let word = idx / 64;
        let bit = idx % 64;
        self.bitmask[word] & (1u64 << bit) != 0
    }

    #[inline]
    pub fn count(&self) -> u16 {
        self.set_count
    }

    /// Iterate over indices of set bits efficiently using trailing_zeros.
    pub fn iter_set_bits(&self) -> SetBitIterator {
        SetBitIterator {
            bitmask: &self.bitmask,
            word_idx: 0,
            current_word: self.bitmask[0],
            base: 0,
        }
    }
}

pub struct SetBitIterator<'a> {
    bitmask: &'a [u64; MASK_WORDS],
    word_idx: usize,
    current_word: u64,
    base: usize,
}

impl<'a> Iterator for SetBitIterator<'a> {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        loop {
            if self.current_word != 0 {
                let tz = self.current_word.trailing_zeros() as usize;
                // Clear the lowest set bit
                self.current_word &= self.current_word - 1;
                return Some(self.base + tz);
            }
            self.word_idx += 1;
            if self.word_idx >= MASK_WORDS {
                return None;
            }
            self.current_word = self.bitmask[self.word_idx];
            self.base = self.word_idx * 64;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_mask() {
        let mask = FilterMask::new();
        assert_eq!(mask.count(), 0);
        assert_eq!(mask.iter_set_bits().count(), 0);
    }

    #[test]
    fn test_set_and_get() {
        let mut mask = FilterMask::new();
        mask.set_bit(0);
        mask.set_bit(63);
        mask.set_bit(64);
        mask.set_bit(2047);

        assert!(mask.is_set(0));
        assert!(mask.is_set(63));
        assert!(mask.is_set(64));
        assert!(mask.is_set(2047));
        assert!(!mask.is_set(1));
        assert!(!mask.is_set(100));
        assert_eq!(mask.count(), 4);
    }

    #[test]
    fn test_double_set() {
        let mut mask = FilterMask::new();
        mask.set_bit(5);
        mask.set_bit(5);
        assert_eq!(mask.count(), 1);
    }

    #[test]
    fn test_iter_set_bits() {
        let mut mask = FilterMask::new();
        mask.set_bit(3);
        mask.set_bit(100);
        mask.set_bit(2000);

        let bits: Vec<usize> = mask.iter_set_bits().collect();
        assert_eq!(bits, vec![3, 100, 2000]);
    }
}

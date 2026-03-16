/// Direct-address lookup table for part table entries.
///
/// TPC-H p_partkey values are dense sequential integers in [1, 200_000 * SF].
/// A flat Vec indexed by (p_partkey - 1) eliminates hashing, collision probing,
/// and the second indirection through a buckets array — single array access per probe.
///
/// Memory: 200K * 16B = ~3.2MB at SF=1, ~16MB at SF=5.
/// SF=1 and SF=2 fit comfortably in L3; SF=5 may exceed L3 on smaller CPUs.
pub struct DirectTable {
    /// Indexed by (p_partkey - 1). brand_idx == u8::MAX means slot is empty.
    slots: Vec<HashTableEntry>,
}

impl DirectTable {
    /// Allocate a table for partkeys in [1, max_partkey].
    pub fn new(max_partkey: usize) -> Self {
        let empty = HashTableEntry {
            p_partkey: 0,
            p_brand_idx: u8::MAX, // sentinel: empty slot
            p_size: 0,
            p_container_idx: 0,
            _padding: [0; 5],
        };
        Self {
            slots: vec![empty; max_partkey],
        }
    }

    #[inline]
    pub fn insert(&mut self, entry: HashTableEntry) {
        let idx = (entry.p_partkey - 1) as usize;
        self.slots[idx] = entry;
    }

    /// O(1) probe: one array access, no hashing or collision handling.
    #[inline]
    pub fn get(&self, partkey: i64) -> Option<&HashTableEntry> {
        let idx = (partkey - 1) as usize;
        if idx < self.slots.len() {
            let e = unsafe { self.slots.get_unchecked(idx) };
            if e.p_brand_idx != u8::MAX {
                return Some(e);
            }
        }
        None
    }

    pub fn len(&self) -> usize {
        self.slots.iter().filter(|e| e.p_brand_idx != u8::MAX).count()
    }
}

// ---------------------------------------------------------------------------

/// FxHash open-addressing hash table for part table entries.

#[repr(C)]
#[derive(Clone, Copy)]
pub struct HashTableEntry {
    pub p_partkey: i64,
    pub p_brand_idx: u8,
    pub p_size: u8,
    pub p_container_idx: u8,
    pub _padding: [u8; 5],
}

const EMPTY: u32 = u32::MAX;

/// FxHash constant (same as used in rustc-hash for 64-bit).
const FX_SEED: u64 = 0x517cc1b727220a95;

/// Fast hash for i64 keys — replicates FxHash's multiply-xor-shift.
#[inline]
fn fx_hash_i64(key: i64) -> u64 {
    let k = key as u64;
    k.wrapping_mul(FX_SEED)
}

pub struct HashTable {
    entries: Vec<HashTableEntry>,
    buckets: Vec<u32>,
    capacity_mask: u32,
}

impl HashTable {
    /// Create a hash table sized for `estimated_size` entries (load factor <= 0.5).
    pub fn new(estimated_size: usize) -> Self {
        let capacity = (estimated_size * 2).next_power_of_two().max(16);
        Self {
            entries: Vec::with_capacity(estimated_size),
            buckets: vec![EMPTY; capacity],
            capacity_mask: (capacity - 1) as u32,
        }
    }

    /// Build a hash table from a pre-collected vec of entries.
    pub fn from_entries(entries: Vec<HashTableEntry>) -> Self {
        let capacity = (entries.len() * 2).next_power_of_two().max(16);
        let mut buckets = vec![EMPTY; capacity];
        let mask = (capacity - 1) as u32;

        for (idx, entry) in entries.iter().enumerate() {
            let mut bucket = (fx_hash_i64(entry.p_partkey) as u32) & mask;
            loop {
                if buckets[bucket as usize] == EMPTY {
                    buckets[bucket as usize] = idx as u32;
                    break;
                }
                bucket = (bucket + 1) & mask;
            }
        }

        Self {
            entries,
            buckets,
            capacity_mask: mask,
        }
    }

    /// Probe for a partkey. Returns reference to the entry if found.
    #[inline]
    pub fn get(&self, partkey: i64) -> Option<&HashTableEntry> {
        let mut bucket = (fx_hash_i64(partkey) as u32) & self.capacity_mask;
        loop {
            let idx = self.buckets[bucket as usize];
            if idx == EMPTY {
                return None;
            }
            let entry = unsafe { self.entries.get_unchecked(idx as usize) };
            if entry.p_partkey == partkey {
                return Some(entry);
            }
            bucket = (bucket + 1) & self.capacity_mask;
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(partkey: i64, brand: u8, size: u8, container: u8) -> HashTableEntry {
        HashTableEntry {
            p_partkey: partkey,
            p_brand_idx: brand,
            p_size: size,
            p_container_idx: container,
            _padding: [0; 5],
        }
    }

    #[test]
    fn test_insert_and_probe() {
        let entries: Vec<HashTableEntry> = (0..1000)
            .map(|i| make_entry(i, (i % 3) as u8, (i % 15 + 1) as u8, (i % 12) as u8))
            .collect();

        let ht = HashTable::from_entries(entries);
        assert_eq!(ht.len(), 1000);

        // Probe every inserted key
        for i in 0..1000_i64 {
            let entry = ht.get(i).expect("should find key");
            assert_eq!(entry.p_partkey, i);
            assert_eq!(entry.p_brand_idx, (i % 3) as u8);
            assert_eq!(entry.p_size, (i % 15 + 1) as u8);
            assert_eq!(entry.p_container_idx, (i % 12) as u8);
        }
    }

    #[test]
    fn test_probe_miss() {
        let entries = vec![make_entry(42, 0, 5, 3)];
        let ht = HashTable::from_entries(entries);
        assert!(ht.get(42).is_some());
        assert!(ht.get(99).is_none());
        assert!(ht.get(0).is_none());
    }

    #[test]
    fn test_load_factor() {
        let n = 1000;
        let entries: Vec<HashTableEntry> = (0..n)
            .map(|i| make_entry(i, 0, 1, 0))
            .collect();
        let ht = HashTable::from_entries(entries);
        // capacity_mask + 1 = capacity, should be >= 2 * n
        let capacity = (ht.capacity_mask + 1) as usize;
        assert!(capacity >= 2 * n as usize);
    }
}

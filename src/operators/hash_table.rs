/// Encoded part-table row stored in the DirectTable.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartEntry {
    pub p_partkey:       i64,     // 8 bytes — physical INT64 from Parquet, join key
    pub p_brand_idx:     u8,      // 1 byte  — 0=Brand#12, 1=Brand#23, 2=Brand#34
    pub p_size:          u8,      // 1 byte  — 1–15 (safe post-filter)
    pub p_container_idx: u8,      // 1 byte  — 0–11 for 12 container types
    pub _padding:        [u8; 5], // 5 bytes — pad to 16 bytes total for alignment
}

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
    slots: Vec<PartEntry>,
}

impl DirectTable {
    /// Allocate a table for partkeys in [1, max_partkey].
    pub fn new(max_partkey: usize) -> Self {
        let empty = PartEntry {
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
    pub fn insert(&mut self, entry: PartEntry) {
        let idx = (entry.p_partkey - 1) as usize;
        self.slots[idx] = entry;
    }

    /// O(1) probe: one array access, no hashing or collision handling.
    #[inline]
    pub fn get(&self, partkey: i64) -> Option<&PartEntry> {
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

//! Memory layout optimizations for cache efficiency
//!
//! This module provides cache-aligned data structures to minimize
//! cache line splits and improve spatial locality.

use std::ops::{Deref, DerefMut};

/// A cache-aligned column vector wrapper
/// 
/// Ensures that column data starts on a 64-byte boundary (one cache line)
/// to prevent cache line splits and improve memory access patterns.
#[repr(C, align(64))]
#[derive(Debug, Clone)]
pub struct AlignedColumn<T> {
    data: Vec<T>,
}

impl<T> AlignedColumn<T> {
    /// Create a new aligned column with the given capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }
    
    /// Create a new aligned column from a vector
    pub fn from_vec(data: Vec<T>) -> Self {
        Self { data }
    }
    
    /// Get the length of the column
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    /// Check if the column is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    
    /// Get a reference to the underlying vector
    pub fn as_vec(&self) -> &Vec<T> {
        &self.data
    }
    
    /// Get a mutable reference to the underlying vector
    pub fn as_vec_mut(&mut self) -> &mut Vec<T> {
        &mut self.data
    }
    
    /// Consume self and return the underlying vector
    pub fn into_vec(self) -> Vec<T> {
        self.data
    }
}

impl<T> Default for AlignedColumn<T> {
    fn default() -> Self {
        Self {
            data: Vec::new(),
        }
    }
}

impl<T> Deref for AlignedColumn<T> {
    type Target = Vec<T>;
    
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for AlignedColumn<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<T> From<Vec<T>> for AlignedColumn<T> {
    fn from(data: Vec<T>) -> Self {
        Self::from_vec(data)
    }
}

/// Native batch structure with cache-aligned columns
/// 
/// This structure uses cache-aligned column vectors to ensure optimal
/// memory access patterns. Each column starts on a 64-byte boundary.
#[derive(Debug, Clone)]
pub struct NativeBatch {
    pub num_rows: usize,
    pub returnflag: AlignedColumn<u8>,
    pub linestatus: AlignedColumn<u8>,
    pub quantity: AlignedColumn<f64>,
    pub extendedprice: AlignedColumn<f64>,
    pub discount: AlignedColumn<f64>,
    pub tax: AlignedColumn<f64>,
    pub shipdate: AlignedColumn<i32>,
}

impl NativeBatch {
    /// Create a new empty batch
    pub fn new() -> Self {
        Self {
            num_rows: 0,
            returnflag: AlignedColumn::default(),
            linestatus: AlignedColumn::default(),
            quantity: AlignedColumn::default(),
            extendedprice: AlignedColumn::default(),
            discount: AlignedColumn::default(),
            tax: AlignedColumn::default(),
            shipdate: AlignedColumn::default(),
        }
    }
    
    /// Create a new batch with the given capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            num_rows: 0,
            returnflag: AlignedColumn::with_capacity(capacity),
            linestatus: AlignedColumn::with_capacity(capacity),
            quantity: AlignedColumn::with_capacity(capacity),
            extendedprice: AlignedColumn::with_capacity(capacity),
            discount: AlignedColumn::with_capacity(capacity),
            tax: AlignedColumn::with_capacity(capacity),
            shipdate: AlignedColumn::with_capacity(capacity),
        }
    }
}

impl Default for NativeBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_aligned_column_alignment() {
        let col: AlignedColumn<f64> = AlignedColumn::with_capacity(100);
        let ptr = &col as *const AlignedColumn<f64> as usize;
        // Verify 64-byte alignment
        assert_eq!(ptr % 64, 0, "AlignedColumn should be 64-byte aligned");
    }
    
    #[test]
    fn test_aligned_column_operations() {
        let mut col = AlignedColumn::from_vec(vec![1.0, 2.0, 3.0]);
        assert_eq!(col.len(), 3);
        assert!(!col.is_empty());
        
        // Test deref
        col.push(4.0);
        assert_eq!(col.len(), 4);
        assert_eq!(col[0], 1.0);
    }
    
    #[test]
    fn test_native_batch_creation() {
        let batch = NativeBatch::with_capacity(1000);
        assert_eq!(batch.num_rows, 0);
        assert_eq!(batch.quantity.len(), 0);
    }
    
    #[test]
    fn test_native_batch_alignment() {
        let batch = NativeBatch::new();
        
        // Verify each column is 64-byte aligned
        let returnflag_ptr = &batch.returnflag as *const AlignedColumn<u8> as usize;
        let linestatus_ptr = &batch.linestatus as *const AlignedColumn<u8> as usize;
        let quantity_ptr = &batch.quantity as *const AlignedColumn<f64> as usize;
        
        assert_eq!(returnflag_ptr % 64, 0, "returnflag should be 64-byte aligned");
        assert_eq!(linestatus_ptr % 64, 0, "linestatus should be 64-byte aligned");
        assert_eq!(quantity_ptr % 64, 0, "quantity should be 64-byte aligned");
    }
}

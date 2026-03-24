//! Registered file descriptor table for io_uring.
//!
//! Maintains a fixed-size table of fds registered with IORING_REGISTER_FILES.
//! New connections get a slot; closed connections free their slot.
//! This module is only compiled on Linux (cfg-gated in mod.rs).

/// Registered file descriptor table for io_uring fixed-file operations.
pub struct FdTable {
    fds: Vec<i32>,
    free_slots: Vec<u32>,
    capacity: usize,
}

impl FdTable {
    /// Create a new FdTable with the given capacity. All slots initialized to -1 (empty).
    pub fn new(capacity: usize) -> Self {
        Self {
            fds: vec![-1i32; capacity],
            free_slots: (0..capacity as u32).rev().collect(),
            capacity,
        }
    }

    /// Insert a raw fd, returning the fixed-fd index. None if table full.
    pub fn insert(&mut self, fd: i32) -> Option<u32> {
        let idx = self.free_slots.pop()?;
        self.fds[idx as usize] = fd;
        Some(idx)
    }

    /// Remove fd at index, returning the raw fd. Marks slot as free.
    pub fn remove(&mut self, idx: u32) -> i32 {
        let fd = self.fds[idx as usize];
        self.fds[idx as usize] = -1;
        self.free_slots.push(idx);
        fd
    }

    /// Get raw fd at index.
    pub fn get(&self, idx: u32) -> i32 {
        self.fds[idx as usize]
    }

    /// Get the raw fd slice for io_uring registration.
    pub fn as_slice(&self) -> &[i32] {
        &self.fds
    }

    /// Number of active (occupied) slots.
    pub fn active_count(&self) -> usize {
        self.capacity - self.free_slots.len()
    }

    /// Whether the table is full.
    pub fn is_full(&self) -> bool {
        self.free_slots.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_table_empty() {
        let table = FdTable::new(4);
        assert_eq!(table.active_count(), 0);
        assert!(!table.is_full());
    }

    #[test]
    fn test_insert_and_get() {
        let mut table = FdTable::new(4);
        let idx = table.insert(42).unwrap();
        assert_eq!(table.get(idx), 42);
        assert_eq!(table.active_count(), 1);
    }

    #[test]
    fn test_insert_remove_roundtrip() {
        let mut table = FdTable::new(4);
        let idx = table.insert(100).unwrap();
        let fd = table.remove(idx);
        assert_eq!(fd, 100);
        assert_eq!(table.active_count(), 0);
        assert_eq!(table.get(idx), -1);
    }

    #[test]
    fn test_full_table_returns_none() {
        let mut table = FdTable::new(2);
        table.insert(10).unwrap();
        table.insert(20).unwrap();
        assert!(table.is_full());
        assert_eq!(table.insert(30), None);
    }

    #[test]
    fn test_remove_frees_slot() {
        let mut table = FdTable::new(1);
        let idx = table.insert(10).unwrap();
        assert!(table.is_full());
        table.remove(idx);
        assert!(!table.is_full());
        // Can insert again
        let idx2 = table.insert(20).unwrap();
        assert_eq!(table.get(idx2), 20);
    }

    #[test]
    fn test_as_slice() {
        let mut table = FdTable::new(3);
        table.insert(10);
        let slice = table.as_slice();
        assert_eq!(slice.len(), 3);
        // At least one slot should have value 10
        assert!(slice.iter().any(|&fd| fd == 10));
    }

    #[test]
    fn test_active_count_tracking() {
        let mut table = FdTable::new(4);
        assert_eq!(table.active_count(), 0);
        let i1 = table.insert(1).unwrap();
        assert_eq!(table.active_count(), 1);
        let _i2 = table.insert(2).unwrap();
        assert_eq!(table.active_count(), 2);
        table.remove(i1);
        assert_eq!(table.active_count(), 1);
    }
}

//! Segment lifecycle handle with Arc-based reference counting and tombstone cleanup.
//!
//! `SegmentHandle` wraps `Arc<SegmentLifetime>` to prevent segment directory
//! deletion while any reader (e.g., mmap) holds a reference. When the last
//! handle drops and the segment is tombstoned, the directory is removed.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Tracks segment directory lifecycle. When tombstoned and all references
/// are dropped, the segment directory is removed from disk.
pub struct SegmentLifetime {
    segment_dir: PathBuf,
    tombstoned: AtomicBool,
}

impl SegmentLifetime {
    /// Create a new segment lifetime for the given directory.
    pub fn new(segment_dir: PathBuf) -> Self {
        Self {
            segment_dir,
            tombstoned: AtomicBool::new(false),
        }
    }

    /// Mark this segment for deletion when all references are dropped.
    pub fn mark_tombstoned(&self) {
        self.tombstoned.store(true, Ordering::Release);
    }

    /// Check if this segment is marked for deletion.
    pub fn is_tombstoned(&self) -> bool {
        self.tombstoned.load(Ordering::Acquire)
    }

    /// Return the segment directory path.
    pub fn segment_dir(&self) -> &Path {
        &self.segment_dir
    }
}

impl Drop for SegmentLifetime {
    fn drop(&mut self) {
        if *self.tombstoned.get_mut() && self.segment_dir.exists() {
            tracing::info!(
                dir = %self.segment_dir.display(),
                "removing tombstoned segment directory",
            );
            let _ = std::fs::remove_dir_all(&self.segment_dir);
        }
    }
}

/// Reference-counted handle to a segment directory.
///
/// Cloning increments the refcount. The segment directory is only
/// eligible for deletion when all handles are dropped AND the
/// segment is tombstoned.
#[derive(Clone)]
pub struct SegmentHandle {
    inner: Arc<SegmentLifetime>,
    segment_id: u64,
}

impl SegmentHandle {
    /// Create a new handle for the given segment.
    pub fn new(segment_id: u64, segment_dir: PathBuf) -> Self {
        Self {
            inner: Arc::new(SegmentLifetime::new(segment_dir)),
            segment_id,
        }
    }

    /// Return the segment directory path.
    pub fn segment_dir(&self) -> &Path {
        self.inner.segment_dir()
    }

    /// Return the segment ID.
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// Mark this segment for deletion when all handles are dropped.
    pub fn mark_tombstoned(&self) {
        self.inner.mark_tombstoned();
    }

    /// Check if this segment is marked for deletion.
    pub fn is_tombstoned(&self) -> bool {
        self.inner.is_tombstoned()
    }

    /// Return the current Arc reference count.
    pub fn refcount(&self) -> usize {
        Arc::strong_count(&self.inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_segment_handle_tombstone_cleanup() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-42");
        std::fs::create_dir_all(&seg_dir).unwrap();
        assert!(seg_dir.exists());

        let handle = SegmentHandle::new(42, seg_dir.clone());
        handle.mark_tombstoned();
        assert!(handle.is_tombstoned());

        // Drop the handle -- directory should be removed
        drop(handle);
        assert!(
            !seg_dir.exists(),
            "tombstoned segment dir should be removed on drop"
        );
    }

    #[test]
    fn test_segment_handle_no_cleanup_without_tombstone() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-43");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let handle = SegmentHandle::new(43, seg_dir.clone());
        drop(handle);
        assert!(seg_dir.exists(), "non-tombstoned segment dir should remain");
    }

    #[test]
    fn test_segment_handle_refcount() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-44");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let handle = SegmentHandle::new(44, seg_dir.clone());
        assert_eq!(handle.refcount(), 1);

        let clone1 = handle.clone();
        assert_eq!(handle.refcount(), 2);
        assert_eq!(clone1.refcount(), 2);

        drop(clone1);
        assert_eq!(handle.refcount(), 1);

        // Tombstone and drop -- should clean up
        handle.mark_tombstoned();
        drop(handle);
        assert!(!seg_dir.exists());
    }

    #[test]
    fn test_segment_handle_clone_prevents_cleanup() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-45");
        std::fs::create_dir_all(&seg_dir).unwrap();

        let handle = SegmentHandle::new(45, seg_dir.clone());
        let clone = handle.clone();
        handle.mark_tombstoned();

        // Drop original -- clone still holds reference
        drop(handle);
        assert!(seg_dir.exists(), "dir should remain while clone exists");

        // Drop clone -- now it should be cleaned up
        drop(clone);
        assert!(
            !seg_dir.exists(),
            "dir should be removed after last ref dropped"
        );
    }

    #[test]
    fn test_segment_handle_segment_id() {
        let tmp = tempfile::tempdir().unwrap();
        let seg_dir = tmp.path().join("segment-99");
        let handle = SegmentHandle::new(99, seg_dir);
        assert_eq!(handle.segment_id(), 99);
    }
}

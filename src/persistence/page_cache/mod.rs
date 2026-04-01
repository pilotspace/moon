//! PageCache buffer manager with clock-sweep eviction.
//!
//! Manages both 4KB and 64KB page frames with:
//! - Lock-free pin/unpin via packed AtomicU32 state
//! - Clock-sweep eviction respecting pinned frames
//! - WAL-before-data invariant enforcement at flush time
//! - DashMap page table for O(1) page lookup

pub mod eviction;
pub mod frame;

pub use eviction::ClockSweep;
pub use frame::{FrameDescriptor, FrameState};

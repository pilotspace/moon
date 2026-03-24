//! NUMA-aware thread pinning and memory placement.
//!
//! On Linux: pins the current thread to a specific CPU core via core_affinity,
//! and detects NUMA node topology from sysfs for diagnostics.
//! On macOS/other: all functions are no-ops (developer machine is macOS/aarch64).

/// Pin the current thread to the given core ID.
///
/// On Linux, uses `core_affinity::set_for_current`. On non-Linux, this is a no-op.
/// Called in the shard thread spawn closure BEFORE building the Tokio runtime,
/// ensuring all subsequent allocations go to the local NUMA node.
pub fn pin_to_core(core_id: usize) {
    #[cfg(target_os = "linux")]
    {
        use core_affinity::CoreId;
        let id = CoreId { id: core_id };
        if core_affinity::set_for_current(id) {
            tracing::info!("Shard {} pinned to core {}", core_id, core_id);
        } else {
            tracing::warn!("Shard {} failed to pin to core {}", core_id, core_id);
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = core_id;
        tracing::debug!("Thread pinning not available on this platform (shard {})", core_id);
    }
}

/// Detect which NUMA node owns the given CPU core (Linux only).
///
/// Reads `/sys/devices/system/node/nodeN/cpulist` to find the node.
/// Returns `None` on non-Linux or if detection fails.
#[cfg(target_os = "linux")]
pub fn detect_numa_node(cpu_id: usize) -> Option<usize> {
    let node_dir = std::path::Path::new("/sys/devices/system/node");
    if !node_dir.exists() {
        return None;
    }
    let entries = std::fs::read_dir(node_dir).ok()?;
    for entry in entries {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let name_str = name.to_str()?;
        if !name_str.starts_with("node") {
            continue;
        }
        let node_id: usize = name_str.strip_prefix("node")?.parse().ok()?;
        let cpulist_path = entry.path().join("cpulist");
        let cpulist = std::fs::read_to_string(&cpulist_path).ok()?;
        if cpu_in_cpulist(cpu_id, cpulist.trim()) {
            return Some(node_id);
        }
    }
    None
}

/// Parse a Linux cpulist string (e.g., "0-3,8-11") and check if cpu_id is in range.
#[cfg(target_os = "linux")]
fn cpu_in_cpulist(cpu_id: usize, cpulist: &str) -> bool {
    for range_str in cpulist.split(',') {
        let range_str = range_str.trim();
        if let Some((start_s, end_s)) = range_str.split_once('-') {
            if let (Ok(start), Ok(end)) = (start_s.parse::<usize>(), end_s.parse::<usize>()) {
                if cpu_id >= start && cpu_id <= end {
                    return true;
                }
            }
        } else if let Ok(single) = range_str.parse::<usize>() {
            if cpu_id == single {
                return true;
            }
        }
    }
    false
}

#[cfg(target_os = "linux")]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cpu_in_cpulist_single() {
        assert!(cpu_in_cpulist(0, "0"));
        assert!(cpu_in_cpulist(3, "3"));
        assert!(!cpu_in_cpulist(1, "0"));
    }

    #[test]
    fn test_cpu_in_cpulist_range() {
        assert!(cpu_in_cpulist(0, "0-3"));
        assert!(cpu_in_cpulist(2, "0-3"));
        assert!(cpu_in_cpulist(3, "0-3"));
        assert!(!cpu_in_cpulist(4, "0-3"));
    }

    #[test]
    fn test_cpu_in_cpulist_mixed() {
        assert!(cpu_in_cpulist(0, "0-3,8-11"));
        assert!(cpu_in_cpulist(9, "0-3,8-11"));
        assert!(!cpu_in_cpulist(5, "0-3,8-11"));
    }
}

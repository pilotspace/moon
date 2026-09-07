pub mod file_ext;
pub mod prefix_map;

/// Per-run scratch paths for lib tests (moon#822). Test-only: it must not be
/// reachable from shipping code, and `#[cfg(test)]` is what enforces that.
#[cfg(test)]
pub(crate) mod test_temp;

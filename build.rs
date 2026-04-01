//! Build script for CUDA toolkit detection.
//!
//! Sets `cfg` flags consumed by `src/vector/gpu/`:
//! - `has_cuda_toolkit`: nvcc found in PATH or CUDA_HOME/CUDA_PATH set
//! - `cuda_12_plus`: detected toolkit version >= 12.0

use std::process::Command;

fn main() {
    // Rerun if environment changes
    println!("cargo:rerun-if-env-changed=CUDA_HOME");
    println!("cargo:rerun-if-env-changed=CUDA_PATH");

    if let Some(version) = detect_cuda_version() {
        println!("cargo:rustc-cfg=has_cuda_toolkit");
        if version.0 >= 12 {
            println!("cargo:rustc-cfg=cuda_12_plus");
        }
    }
}

/// Attempt to detect CUDA toolkit version by running `nvcc --version`.
///
/// Returns `Some((major, minor))` if successful, `None` otherwise.
fn detect_cuda_version() -> Option<(u32, u32)> {
    // Try nvcc from CUDA_HOME or CUDA_PATH first, then fall back to PATH
    let nvcc_paths = cuda_home_nvcc()
        .into_iter()
        .chain(std::iter::once("nvcc".to_string()));

    for nvcc in nvcc_paths {
        if let Some(ver) = run_nvcc_version(&nvcc) {
            return Some(ver);
        }
    }
    None
}

/// Build nvcc path from CUDA_HOME or CUDA_PATH environment variables.
fn cuda_home_nvcc() -> Vec<String> {
    let mut paths = Vec::new();
    for var in &["CUDA_HOME", "CUDA_PATH"] {
        if let Ok(home) = std::env::var(var) {
            let p = std::path::Path::new(&home).join("bin").join("nvcc");
            if let Some(s) = p.to_str() {
                paths.push(s.to_string());
            }
        }
    }
    paths
}

/// Run `nvcc --version` and parse the version line.
///
/// Example output line: `Cuda compilation tools, release 12.4, V12.4.131`
fn run_nvcc_version(nvcc: &str) -> Option<(u32, u32)> {
    let output = Command::new(nvcc).arg("--version").output().ok()?;

    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    // Look for "release X.Y" pattern
    for line in stdout.lines() {
        if let Some(pos) = line.find("release ") {
            let after = &line[pos + 8..];
            let version_str: String = after
                .chars()
                .take_while(|c| *c == '.' || c.is_ascii_digit())
                .collect();
            let mut parts = version_str.split('.');
            let major = parts.next().and_then(|s| s.parse::<u32>().ok())?;
            let minor = parts
                .next()
                .and_then(|s| s.parse::<u32>().ok())
                .unwrap_or(0);
            return Some((major, minor));
        }
    }
    None
}

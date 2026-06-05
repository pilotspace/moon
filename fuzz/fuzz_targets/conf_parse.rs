#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::config::conf_file::parse_conf_contents;

// Fuzz the moon.conf parser.
//
// Exercises comment stripping (inline `#` only after whitespace),
// double-quote stripping, key normalisation (`_` → `-`), bool-flag
// mapping (yes/true/1 vs no/false/0), and the error paths
// (EmptyValue / InvalidBoolValue). Any panic is a bug.
fuzz_target!(|data: &[u8]| {
    // Conf files are UTF-8 text — skip invalid UTF-8.
    let Ok(contents) = std::str::from_utf8(data) else {
        return;
    };

    // Must never panic on any input; returning an error is fine.
    let _ = parse_conf_contents(contents);
});

#![no_main]
use libfuzzer_sys::fuzz_target;

use moon::acl::rules;
use moon::acl::table::AclUser;

/// Fuzz the ACL rule parser.
///
/// Exercises all rule prefix paths (+, -, ~, &, >, <, #, !, %R~, %W~,
/// on, off, nopass, resetpass, resetkeys, resetchannels, reset).
/// Any panic is a bug.
fuzz_target!(|data: &[u8]| {
    // ACL rules are UTF-8 strings — skip invalid UTF-8
    let Ok(rule_str) = std::str::from_utf8(data) else {
        return;
    };

    let mut user = AclUser::new_default_nopass();
    // Should not panic on any input string
    rules::apply_rule(&mut user, rule_str);
});

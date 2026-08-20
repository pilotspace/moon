pub mod io;
// `pub` rather than `pub(crate)`: the key-position walker is a contract shared
// by ACL, cache invalidation and command introspection, and it parses attacker
// -controlled argv. `fuzz/fuzz_targets/acl_keyspec.rs` drives it directly
// (moon#576) — a bounds bug here is a remote panic in three places at once.
pub mod keyspec;
pub mod log;
pub mod rules;
pub mod table;

pub use io::{acl_load, acl_save, acl_table_from_config, user_to_acl_line};
pub use log::{AclLog, AclLogEntry};
pub use table::{AclTable, AclUser, CommandPermissions, KeyPattern};

//! The per-script thread-local state the `redis.call` bridge reads: the
//! database pointer, ACL and tracking identity, read-only and OOM modes, and
//! the write flag.

use std::cell::{Cell, RefCell};

use crate::acl::ScriptAcl;

thread_local! {
    /// Raw pointer to the current shard's Database during script execution.
    pub(super) static CURRENT_DB: Cell<*mut ()> = const { Cell::new(std::ptr::null_mut()) };
    /// Current database index (for SELECT within scripts).
    pub(super) static CURRENT_DB_IDX: Cell<usize> = const { Cell::new(0) };
    /// Total number of databases.
    pub(super) static CURRENT_DB_COUNT: Cell<usize> = const { Cell::new(1) };
    /// Whether this script execution has performed any write commands.
    pub(super) static SCRIPT_HAD_WRITE: Cell<bool> = const { Cell::new(false) };
    /// Whether this script is running in read-only mode (FCALL_RO).
    pub(super) static SCRIPT_READ_ONLY: Cell<bool> = const { Cell::new(false) };
    /// moon#1241: how the CURRENT script's own commands meet the OOM gate —
    /// see [`set_script_oom_mode`]. Reset to the compat-EVAL default by
    /// [`set_script_db`] and [`clear_script_db`].
    pub(super) static SCRIPT_OOM_MODE: Cell<ScriptOomMode> = const { Cell::new(ScriptOomMode::Compat) };
    /// moon#569: the ACL identity every `redis.call`/`redis.pcall` of the
    /// CURRENTLY RUNNING script is authorized against.
    ///
    /// Same lifetime and the same single-threaded-shard argument as
    /// `CURRENT_DB`: installed by [`set_script_db`] before the VM runs and
    /// reset by [`clear_script_db`] on every exit path. It resets to
    /// [`ScriptAcl::deny`], not to "no identity = allow", so a script that
    /// somehow reaches a VM outside `set_script_db` refuses every command
    /// instead of inheriting the previous script's caller.
    pub(super) static SCRIPT_ACL: RefCell<ScriptAcl> = RefCell::new(ScriptAcl::deny());
    /// moon#1089: the CLIENT TRACKING identity of the connection running the
    /// CURRENT script, taken from its `ScriptAcl` by [`set_script_db`] and
    /// reset by [`clear_script_db`]. `Copy`, so reading it costs nothing.
    pub(super) static SCRIPT_CALLER: Cell<crate::tracking::ScriptCaller> =
        const { Cell::new(crate::tracking::ScriptCaller { client_id: 0, track_reads: false, noloop: false }) };
}

/// Set the thread-local database pointer and caller identity before script
/// execution.
///
/// `acl` is a REQUIRED parameter rather than a separate optional setter
/// precisely so no execution path can forget it: adding a new script runner
/// forces an explicit authorization decision at compile time (moon#569).
pub fn set_script_db(
    db: &mut crate::storage::Database,
    db_idx: usize,
    db_count: usize,
    acl: &ScriptAcl,
) {
    CURRENT_DB.with(|c| c.set(db as *mut _ as *mut ()));
    CURRENT_DB_IDX.with(|c| c.set(db_idx));
    CURRENT_DB_COUNT.with(|c| c.set(db_count));
    SCRIPT_HAD_WRITE.with(|c| c.set(false));
    SCRIPT_OOM_MODE.with(|c| c.set(ScriptOomMode::Compat));
    SCRIPT_CALLER.with(|c| c.set(acl.caller()));
    SCRIPT_ACL.with(|c| *c.borrow_mut() = acl.clone());
}

/// Clear the thread-local database pointer after script execution.
///
/// Also where the script's CLIENT TRACKING effects take hold (moon#1089):
/// every exit path of a script comes through here, so what its
/// `redis.call`s recorded is applied under one tracking lock, in order.
pub fn clear_script_db() {
    CURRENT_DB.with(|c| c.set(std::ptr::null_mut()));
    SCRIPT_READ_ONLY.with(|c| c.set(false));
    SCRIPT_OOM_MODE.with(|c| c.set(ScriptOomMode::Compat));
    // Back to fail-closed: nothing may run until the next `set_script_db`.
    SCRIPT_ACL.with(|c| *c.borrow_mut() = ScriptAcl::deny());
    SCRIPT_CALLER
        .with(|c| c.replace(crate::tracking::ScriptCaller::default()))
        .finish_script();
}

/// moon#1241: which of the CURRENT script's own commands pass the OOM gate
/// while the shard is over budget (eviction runs either way).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptOomMode {
    /// EVAL — redis's compat mode (a `#!` shebang body does not compile
    /// here), where only `denyoom` commands are refused: shrink-only ones
    /// (DEL, UNLINK, HDEL, ...) pass. The default [`set_script_db`] sets.
    Compat,
    /// A FUNCTION registered with `allow-oom`: every command passes, as
    /// redis's `SCRIPT_ALLOW_OOM` lets it (measured against 7.0.15: an
    /// `allow-oom` function's SET answers +OK over maxmemory).
    AllowOom,
    /// A FUNCTION without `allow-oom`: redis 7.0 refuses the call as a whole
    /// under OOM, so every write in it is refused here.
    Deny,
}

/// Set the CURRENT script's [`ScriptOomMode`], after [`set_script_db`].
pub fn set_script_oom_mode(mode: ScriptOomMode) {
    SCRIPT_OOM_MODE.with(|c| c.set(mode));
}

/// Set the read-only flag for the current script execution (FCALL_RO).
pub fn set_script_read_only(read_only: bool) {
    SCRIPT_READ_ONLY.with(|c| c.set(read_only));
}

/// Check whether the current script execution is in read-only mode.
pub fn is_script_read_only() -> bool {
    SCRIPT_READ_ONLY.with(|c| c.get())
}

/// Check whether the current script execution has performed any write commands.
pub fn script_had_write() -> bool {
    SCRIPT_HAD_WRITE.with(|c| c.get())
}

/// moon#831: read AND reset the write flag of the script that just ran.
///
/// The script arms call this once, right after the VM returns, to decide
/// whether the reply must wait for the batch-end `fsync_barrier` under
/// `appendfsync always`. `set_script_db` resets the flag at the START of
/// every script, so a plain read would be exact for a script that ran —
/// but an arm that answers WITHOUT running the VM (`NOSCRIPT`, a parse
/// error) would otherwise read the previous script's value. Consuming it
/// here makes a stale `true` impossible by construction.
///
/// The flag is set on every `WRITE`-flagged `redis.call`, before the OOM
/// gate and before execution: a superset of "an effect record was
/// emitted". Over-arming costs one fsync that was already owed to the
/// batch; under-arming is the moon#831 defect. The superset is the safe
/// side.
pub fn take_script_had_write() -> bool {
    SCRIPT_HAD_WRITE.with(|c| c.replace(false))
}

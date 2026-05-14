//! Compile-time assertion: `ShardSlice` must be `!Send` and `!Sync`.
//!
//! This test exists purely to make the compiler enforce the thread-local
//! ownership invariant. If anyone accidentally adds `unsafe impl Send for
//! ShardSlice` or removes the `PhantomData<Rc<()>>` marker, this file will
//! fail to compile — catching the regression at build time.
//!
//! The `assert_not_impl_any!` macro from `static-assertions` expands to a
//! trait bound that the compiler rejects if the type DOES implement the trait.

use moon::shard::slice::ShardSlice;
use static_assertions::assert_not_impl_any;

assert_not_impl_any!(ShardSlice: Send, Sync);

/// Placeholder test function — the real assertion happens at compile time via
/// the `assert_not_impl_any!` macro above. If that macro compiles, the test
/// is already passing.
#[test]
fn ensures_compile_time_assertion_present() {
    // No runtime logic needed. Compile = pass.
}

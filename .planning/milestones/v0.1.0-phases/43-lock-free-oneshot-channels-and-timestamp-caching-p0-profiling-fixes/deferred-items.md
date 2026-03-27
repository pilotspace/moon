# Deferred Items - Phase 43

## Pre-existing Build Errors (Out of Scope)

1. **shard/mod.rs:389 and :426** - `Self::drain_spsc_shared()` takes 13 arguments but 12 supplied. Pre-existing error in both tokio and monoio build paths. Not related to oneshot channel changes.

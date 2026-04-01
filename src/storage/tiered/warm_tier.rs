//! HOT->WARM transition protocol for vector segments.
//!
//! Implements the staging-directory atomic transition: write .mpf files
//! to a staging directory, fsync, update manifest, rename to final location.

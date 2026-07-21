//! Subcommands that require a Lance dataset (`ds.lance().is_some()`).
//!
//! Their `run()` functions error with `Error::NotLance` if invoked against a
//! non-Lance adapter. Format-agnostic commands stay one directory up.

pub mod branches;
pub mod diff;
pub mod fragments;
pub mod index_stats;
pub mod indices;
pub mod search;
pub mod stat;
pub mod tags;
pub mod versions;

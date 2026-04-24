//! Lance-format adapter for the `Dataset` trait.
//!
//! Everything Lance-specific lives here so the rest of the crate can be
//! extended with new input formats without touching commands or output code.

mod adapter;

pub use adapter::{LanceDataset, write_dataset};

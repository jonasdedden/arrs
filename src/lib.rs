pub mod cli;
pub mod commands;
pub mod dataset;
pub mod error;
pub mod indices;
pub mod lance;
pub mod output;
pub mod projection;

#[cfg(test)]
mod test_support;

pub use error::{Error, Result};

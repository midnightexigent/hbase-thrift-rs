pub mod client;
pub mod table;

#[allow(clippy::all, dead_code)]
pub mod hbase;

pub use thrift::{self, Error, Result};

pub mod client;
pub mod error;
pub mod scan;

#[allow(clippy::all, dead_code)]
pub mod hbase;

pub use error::Error;

type Result<T> = std::result::Result<T, Error>;

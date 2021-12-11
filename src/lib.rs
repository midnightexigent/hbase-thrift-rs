pub mod client;
pub mod table;

#[allow(clippy::all, dead_code)]
pub mod hbase;

type Client<IP, OP> = hbase::HbaseSyncClient<IP, OP>;

pub use thrift::{self, Error, Result};

use hbase_thrift::hbase::{HbaseSyncClient, THbaseSyncClient};
use r2d2::Pool;
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{
        ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TTcpChannel, WriteHalf,
    },
};
use thrift_pool::{MakeThriftConnectionFromAddrs, ThriftConnectionManager};

type Client = HbaseSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>,
>;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager = ThriftConnectionManager::new(MakeThriftConnectionFromAddrs::<Client, _>::new(
        "localhost:9090",
    ));
    let pool = Pool::builder().build(manager)?;
    let mut conn = pool.get()?;

    let tables: Vec<_> = conn
        .get_table_names()?
        .into_iter()
        .map(String::from_utf8)
        .collect();

    println!("pooled connection returned: {:?}", tables);
    Ok(())
}

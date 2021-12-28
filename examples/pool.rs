use hbase_thrift::hbase::{HbaseSyncClient, THbaseSyncClient};
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{
        ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TTcpChannel, WriteHalf,
    },
};
use thrift_pool::{r2d2::Pool, MakeBinaryProtocol, MakeBufferedTransport, ThriftConnectionManager};

type Client = HbaseSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>,
>;

type ConnectionManager = ThriftConnectionManager<
    Client,
    String,
    MakeBinaryProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>,
    MakeBinaryProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>,
    MakeBufferedTransport<ReadHalf<TTcpChannel>>,
    MakeBufferedTransport<WriteHalf<TTcpChannel>>,
>;
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manager: ConnectionManager = ThriftConnectionManager::new(
        "localhost:9090".to_string(),
        MakeBinaryProtocol::default(),
        MakeBinaryProtocol::default(),
        MakeBufferedTransport::default(),
        MakeBufferedTransport::default(),
    );
    let pool = Pool::builder().build(manager)?;
    let mut conn = pool.get()?;

    let tables = conn.get_table_names()?;

    println!("pooled connection returned: {:?}", tables);
    Ok(())
}

use hbase_thrift::hbase::{HbaseSyncClient, THbaseSyncClient};
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel},
};

fn main() -> Result<(), thrift::Error> {
    let mut channel = TTcpChannel::new();
    channel.open("localhost:9090")?;
    let (i_chan, o_chan) = channel.split()?;

    let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(i_chan), true);
    let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(o_chan), true);

    let mut client = HbaseSyncClient::new(i_prot, o_prot);

    println!(
        "tables before : {:?}",
        client
            .get_table_names()?
            .into_iter()
            .map(|v| String::from_utf8(v).unwrap())
            .collect::<Vec<_>>()
    );

    client.disable_table("test-table".into())?;
    client.delete_table("test-table".into())?;

    println!(
        "tables after : {:?}",
        client
            .get_table_names()?
            .into_iter()
            .map(|v| String::from_utf8(v).unwrap())
            .collect::<Vec<_>>()
    );

    Ok(())
}

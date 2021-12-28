use hbase_thrift::{
    hbase::{HbaseSyncClient, THbaseSyncClient},
    BatchMutationBuilder, MutationBuilder, THbaseSyncClientExt,
};
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

    let mut mutation_builder = MutationBuilder::default();
    mutation_builder.column("data", "foo");
    mutation_builder.value("bar");
    client.put(
        "test-table",
        vec![<BatchMutationBuilder>::default()
            .mutation(mutation_builder)
            .build()],
        None,
        None,
    )?;

    println!("successfully put data in test-table:data");

    Ok(())
}

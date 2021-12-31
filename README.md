# HBase Thrift

This library is like [happybase](https://github.com/python-happybase/happybase), but in rust. It provides a way to interact with [HBase](https://hbase.apache.org/)'s thrift interface 

It provides (some) wrappers that make it easier to interact with the generated code from HBase's [Thrift Spec](https://github.com/apache/hbase/blob/master/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/thrift/Hbase.thrift)

Additionnaly, it provides connection pools through [thrift-pool](https://crates.io/crates/thrift-pool) : see the [pool example](./examples/pool.rs)

## Example

```rust
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
    let tables = client.get_table_names()?;

    println!(
        "tables: {:?}",
        tables
            .into_iter()
            .map(|v| String::from_utf8(v).unwrap())
            .collect::<Vec<_>>()
    );

    Ok(())
}

```

Other examples are under the [examples](./examples) directory

Also see [vector-http-sink-hbase](https://github.com/midnightexigent/vector-http-sink-hbase) which motivated the creation of this library

[Documentation](https://docs.rs/hbase-thrift/0.7.5/hbase_thrift/)

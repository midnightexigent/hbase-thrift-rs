#[allow(clippy::all, dead_code)]
pub mod hbase;

pub use thrift;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{
    ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TTcpChannel, WriteHalf,
};
pub use thrift::{Error, Result};

type ClientInputProtocol = TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>;
type ClientOutputProtocol = TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>;

#[cfg(test)]
mod tests {

    use super::hbase::*;
    use super::thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
    use super::thrift::transport::{
        ReadHalf, TBufferChannel, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel,
        TTcpChannel, WriteHalf,
    };
    use super::Result;
    use std::collections::BTreeMap;
    use std::net::ToSocketAddrs;

    fn new_client() -> thrift::Result<HbaseSyncClient<ClientInputProtocol, ClientOutputProtocol>> {
        let mut c = TTcpChannel::new();

        // open the underlying TCP stream
        c.open("localhost:9090")?;

        // clone the TCP channel into two halves, one which
        // we'll use for reading, the other for writing
        let (i_chan, o_chan) = c.split()?;

        // wrap the raw sockets (slow) with a buffered transport of some kind

        let i_tran = TBufferedReadTransport::new(i_chan);
        let o_tran = TBufferedWriteTransport::new(o_chan);

        // now create the protocol implementations
        let i_prot = TBinaryInputProtocol::new(i_tran, true);
        let o_prot = TBinaryOutputProtocol::new(o_tran, true);

        // we're done!

        Ok(HbaseSyncClient::new(i_prot, o_prot))
    }
    #[test]
    fn connect() -> Result<()> {
        new_client()?;
        Ok(())
    }
    #[test]
    fn create_table() -> Result<()> {
        let mut client = new_client()?;

        client.create_table(
            "emp".into(),
            vec![ColumnDescriptor {
                name: Some("personal data".into()),
                compression: Some("NONE".to_string()),
                bloom_filter_type: Some("NONE".to_string()),
                max_versions: Some(3),
                ..Default::default()
            }],
        )?;
        Ok(())
    }
    #[test]
    fn put() -> Result<()> {
        let mut client = new_client()?;
        let mutation = Mutation {
            column: Some("personal data:name".into()),
            value: Some("bob".into()),
            is_delete: Some(false),
            ..Default::default()
        };
        client.mutate_row("emp".into(), "1".into(), vec![mutation], BTreeMap::new())?;
        Ok(())
    }
    #[test]
    fn scan() -> Result<()> {
        let mut client = new_client()?;
        let scanner_id = client.scanner_open("emp".into(), "".into(), vec![], BTreeMap::new())?;
        loop {
            let rows = client.scanner_get(scanner_id)?;
            if rows.is_empty() {
                break;
            }
            for row in rows {
                println!("{:?}", row);
            }
        }
        Ok(())
    }
}

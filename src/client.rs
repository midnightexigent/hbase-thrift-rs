use crate::{
    hbase::{THBaseServiceSyncClient, TScan},
    scan::Scan, Result,
};
use std::net::ToSocketAddrs;
use thrift::protocol::{
    TCompactInputProtocol, TCompactOutputProtocol, TInputProtocol, TOutputProtocol,
};
use thrift::transport::{
    ReadHalf, TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel, WriteHalf,
};

pub struct Client<IP: TInputProtocol, OP: TOutputProtocol>(THBaseServiceSyncClient<IP, OP>);

impl<IP: TInputProtocol, OP: TOutputProtocol> Client<IP, OP> {
    pub fn new(i_prot: impl Into<IP>, o_prot: impl Into<OP>) -> Self {
        Self(THBaseServiceSyncClient::new(i_prot.into(), o_prot.into()))
    }
    pub fn scan(&mut self, table: impl Into<Vec<u8>>, tscan: TScan, num_rows: i32) -> Result<Scan> {
        Scan::new(table, tscan, num_rows, &mut self.0)
    }
}

impl
    Client<
        TCompactInputProtocol<TFramedReadTransport<ReadHalf<TTcpChannel>>>,
        TCompactOutputProtocol<TFramedWriteTransport<WriteHalf<TTcpChannel>>>,
    >
{
    pub fn compact_framed(remote_address: impl ToSocketAddrs) -> Result<Self> {
        let mut c = TTcpChannel::new();
        c.open(remote_address)?;

        let (i_chan, o_chan) = c.split()?;

        let i_prot = TCompactInputProtocol::new(TFramedReadTransport::new(i_chan));
        let o_prot = TCompactOutputProtocol::new(TFramedWriteTransport::new(o_chan));

        Ok(Self(THBaseServiceSyncClient::new(i_prot, o_prot)))
    }
}

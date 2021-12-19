#[allow(clippy::all, dead_code)]
pub mod hbase;
pub use thrift::{self, Error, Result};
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{
        ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel,
        WriteHalf,
    },
};

use easy_ext::ext;
use hbase::{BatchMutation, HbaseSyncClient, Mutation, THbaseSyncClient, Text};
use std::collections::BTreeMap;
use std::net::ToSocketAddrs;

pub type Attributes = BTreeMap<Text, Text>;

pub type Client = HbaseSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>,
>;
pub fn client(addrs: impl ToSocketAddrs) -> Result<Client> {
    let mut channel = TTcpChannel::new();
    channel.open(addrs)?;
    let (i_chan, o_chan) = channel.split()?;
    let i_prot = TBinaryInputProtocol::new(TBufferedReadTransport::new(i_chan), true);
    let o_prot = TBinaryOutputProtocol::new(TBufferedWriteTransport::new(o_chan), true);
    Ok(HbaseSyncClient::new(i_prot, o_prot))
}

#[ext(THbaseSyncClientExt)]
pub impl<H: THbaseSyncClient + Sized> H {
    fn table(&mut self, table_name: impl Into<Vec<u8>>) -> Table<'_, Self> {
        Table::new(table_name, self)
    }
}
pub struct Table<'a, H: THbaseSyncClient> {
    name: Text,
    client: &'a mut H,
}

impl<'a, H: THbaseSyncClient> Table<'a, H> {
    pub fn new(name: impl Into<Vec<u8>>, client: &'a mut H) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }
    pub fn put(
        &mut self,
        row_batches: Vec<BatchMutation>,
        timestamp: Option<i64>,
        attributes: Attributes,
    ) -> Result<()> {
        if let Some(timestamp) = timestamp {
            self.client
                .mutate_rows_ts(self.name.clone(), row_batches, timestamp, attributes)
        } else {
            self.client
                .mutate_rows(self.name.clone(), row_batches, attributes)
        }
    }
}

#[derive(Debug, Clone)]
pub struct MutationBuilder<T: Into<Text> + Clone> {
    pub is_delete: bool,
    pub write_to_wal: bool,
    pub column: Option<(String, String)>,
    pub value: Option<T>,
}

impl<T: Into<Text> + Clone> MutationBuilder<T> {
    pub fn is_delete(&mut self, is_delete: bool) -> &mut Self {
        self.is_delete = is_delete;
        self
    }
    pub fn column(&mut self, column_family: String, column_qualifier: String) -> &mut Self {
        self.column = Some((column_family, column_qualifier));
        self
    }
    pub fn value(&mut self, value: T) -> &mut Self {
        self.value = Some(value);
        self
    }
    pub fn write_to_wal(&mut self, write_to_wal: bool) -> &mut Self {
        self.write_to_wal = write_to_wal;
        self
    }
    pub fn build(&self) -> Mutation {
        Mutation {
            column: self
                .column
                .as_ref()
                .map(|(column_family, column_qualifier)| {
                    format!("{}:{}", column_family, column_qualifier).into()
                }),
            value: self.value.clone().map(Into::into),
            is_delete: Some(self.is_delete),
            write_to_w_a_l: Some(self.write_to_wal),
        }
    }
}
impl<T: Into<Text> + Clone> Default for MutationBuilder<T> {
    fn default() -> Self {
        Self {
            is_delete: false,
            write_to_wal: true,
            value: None,
            column: None,
        }
    }
}

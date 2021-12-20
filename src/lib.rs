#[allow(clippy::all, dead_code)]
pub mod hbase;
use thiserror::Error as ThisError;
pub use thrift;
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{
        ReadHalf, TBufferedReadTransport, TBufferedWriteTransport, TIoChannel, TTcpChannel,
        WriteHalf,
    },
};

use easy_ext::ext;
use hbase::{BatchMutation, HbaseSyncClient, Mutation, THbaseSyncClient};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    marker::PhantomData,
    net::ToSocketAddrs,
};

pub type Attributes = BTreeMap<Vec<u8>, Vec<u8>>;

pub type Client = HbaseSyncClient<
    TBinaryInputProtocol<TBufferedReadTransport<ReadHalf<TTcpChannel>>>,
    TBinaryOutputProtocol<TBufferedWriteTransport<WriteHalf<TTcpChannel>>>,
>;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    Thift(#[from] thrift::Error),

    #[error("table {0} is not enabled")]
    TableNotEnabled(String),

    #[error("table {0} does not exist")]
    TableNotFound(String),
}
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
    fn table_exists(&mut self, table_name: &str) -> Result<bool> {
        let table_name: Vec<u8> = table_name.into();
        Ok(self.get_table_names()?.into_iter().any(|x| x == table_name))
    }
    fn put(
        &mut self,
        table_name: &str,
        row_batches: Vec<BatchMutation>,
        timestamp: Option<i64>,
        attributes: Option<Attributes>,
    ) -> Result<()> {
        let attributes = attributes.unwrap_or_default();
        let result = if let Some(timestamp) = timestamp {
            self.mutate_rows_ts(table_name.into(), row_batches, timestamp, attributes)
        } else {
            self.mutate_rows(table_name.into(), row_batches, attributes)
        };
        Ok(result?)
    }
}

#[derive(Debug, Clone)]
pub struct MutationBuilder {
    pub is_delete: bool,
    pub write_to_wal: bool,
    pub column: Option<(String, String)>,
    pub value: Option<Vec<u8>>,
}

impl MutationBuilder {
    pub fn is_delete(&mut self, is_delete: bool) -> &mut Self {
        self.is_delete = is_delete;
        self
    }
    pub fn column(&mut self, column_family: String, column_qualifier: String) -> &mut Self {
        self.column = Some((column_family, column_qualifier));
        self
    }
    pub fn value(&mut self, value: impl Into<Vec<u8>>) -> &mut Self {
        self.value = Some(value.into());
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
            value: self.value.clone(),
            is_delete: Some(self.is_delete),
            write_to_w_a_l: Some(self.write_to_wal),
        }
    }
}
impl Default for MutationBuilder {
    fn default() -> Self {
        Self {
            is_delete: false,
            write_to_wal: true,
            value: None,
            column: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BatchMutationBuilder<H: Hasher + Default = DefaultHasher> {
    pub row: Option<Vec<u8>>,
    pub mutations: Option<Vec<MutationBuilder>>,
    phantom: PhantomData<H>,
}
impl<H: Hasher + Default> BatchMutationBuilder<H> {
    pub fn build(&self) -> BatchMutation {
        match self {
            Self {
                row: None,
                mutations: Some(mutations),
                ..
            } => {
                let mut hasher = H::default();
                for mutation in mutations {
                    mutation
                        .column
                        .as_ref()
                        .map(|(_, col_qualifier)| col_qualifier)
                        .hash(&mut hasher);
                    mutation.value.hash(&mut hasher);
                }

                BatchMutation {
                    mutations: Some(mutations.iter().map(|mutation| mutation.build()).collect()),
                    row: Some(hasher.finish().to_be_bytes().to_vec()),
                }
            }
            x => BatchMutation {
                mutations: x
                    .mutations
                    .as_ref()
                    .map(|mutations| mutations.iter().map(|mutation| mutation.build()).collect()),
                row: x.row.clone(),
            },
        }
    }
    pub fn mutation(&mut self, mutation: MutationBuilder) -> &mut Self {
        if let Some(ref mut mutations) = self.mutations {
            mutations.push(mutation);
        } else {
            self.mutations = Some(vec![mutation]);
        }
        self
    }
    pub fn mutations(&mut self, mutations: Vec<MutationBuilder>) -> &mut Self {
        self.mutations = Some(mutations);
        self
    }
    pub fn row(&mut self, row: impl Into<Vec<u8>>) -> &mut Self {
        self.row = Some(row.into());
        self
    }
}

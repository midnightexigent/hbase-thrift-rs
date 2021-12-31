#[allow(clippy::all, dead_code)]
pub mod hbase;
pub use thrift;
pub use thrift_pool;

use easy_ext::ext;
use hbase::{BatchMutation, HbaseSyncClient, Mutation, THbaseSyncClient};
use std::{
    collections::{hash_map::DefaultHasher, BTreeMap},
    hash::{Hash, Hasher},
    marker::PhantomData,
};
use thrift::protocol::{TInputProtocol, TOutputProtocol};
use thrift_pool::{FromProtocol, ThriftConnection};

pub type Attributes = BTreeMap<Vec<u8>, Vec<u8>>;

impl<IP: TInputProtocol, OP: TOutputProtocol> FromProtocol for HbaseSyncClient<IP, OP> {
    type InputProtocol = IP;
    type OutputProtocol = OP;

    fn from_protocol(
        input_protocol: Self::InputProtocol,
        output_protocol: Self::OutputProtocol,
    ) -> Self {
        Self::new(input_protocol, output_protocol)
    }
}
impl<IP: TInputProtocol, OP: TOutputProtocol> ThriftConnection for HbaseSyncClient<IP, OP> {
    type Error = thrift::Error;
    fn is_valid(&mut self) -> std::result::Result<(), Self::Error> {
        let _ = self.get_table_names()?;
        Ok(())
    }
}

#[ext(THbaseSyncClientExt)]
pub impl<H: THbaseSyncClient> H {
    fn table_exists(&mut self, table_name: &str) -> thrift::Result<bool> {
        let table_name: Vec<u8> = table_name.into();
        Ok(self.get_table_names()?.into_iter().any(|x| x == table_name))
    }
    fn put(
        &mut self,
        table_name: &str,
        row_batches: Vec<BatchMutation>,
        timestamp: Option<i64>,
        attributes: Option<Attributes>,
    ) -> thrift::Result<()> {
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
    pub fn column(
        &mut self,
        column_family: impl Into<String>,
        column_qualifier: impl Into<String>,
    ) -> &mut Self {
        self.column = Some((column_family.into(), column_qualifier.into()));
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

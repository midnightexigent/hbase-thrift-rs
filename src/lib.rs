#[allow(clippy::all, dead_code)]
pub mod hbase;

use hbase::{BatchMutation, Mutation, THbaseSyncClient, Text};
pub use thrift::{self, Error, Result};

use easy_ext::ext;

#[ext(THbaseSyncClientExt)]
pub impl<H: THbaseSyncClient + Sized> H {
    fn table(&mut self, table_name: impl Into<Vec<u8>>) -> Table<'_, Self> {
        Table::new(table_name, self)
    }
}
pub struct Table<'a, H: THbaseSyncClient> {
    name: Vec<u8>,
    client: &'a mut H,
}

impl<'a, H: THbaseSyncClient> Table<'a, H> {
    pub fn new(name: impl Into<Vec<u8>>, client: &'a mut H) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }
    pub fn put(&mut self) -> Result<()> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MutationBuilder<T: Into<Text>> {
    pub is_delete: bool,
    pub write_to_wal: bool,
    pub column: Option<(String, String)>,
    pub value: Option<T>,
}

impl<T: Into<Text>> MutationBuilder<T> {
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
}
impl<T: Into<Text>> Default for MutationBuilder<T> {
    fn default() -> Self {
        Self {
            is_delete: false,
            write_to_wal: true,
            value: None,
            column: None,
        }
    }
}

impl<T: Into<Text>> From<MutationBuilder<T>> for Mutation {
    fn from(mutation_builder: MutationBuilder<T>) -> Self {
        Self {
            column: mutation_builder
                .column
                .map(|(column_family, column_qualifier)| {
                    format!("{}:{}", column_family, column_qualifier).into()
                }),
            value: mutation_builder.value.map(Into::into),
            is_delete: Some(mutation_builder.is_delete),
            write_to_w_a_l: Some(mutation_builder.write_to_wal),
        }
    }
}

use crate::{
    hbase::{Mutation, THbaseSyncClient},
    Result,
};
use std::collections::BTreeMap;

pub struct Table<'a> {
    name: Vec<u8>,
    client: &'a mut dyn THbaseSyncClient,
}

impl<'a> Table<'a> {
    pub fn new(name: impl Into<Vec<u8>>, client: &'a mut dyn THbaseSyncClient) -> Self {
        Self {
            name: name.into(),
            client,
        }
    }

    pub fn put(
        &mut self,
        row: impl Into<Vec<u8>>,
        mutations: impl Into<Vec<Mutation>>,
        attributes: impl Into<BTreeMap<Vec<u8>, Vec<u8>>>,
    ) -> Result<()> {
        self.client.mutate_row(
            self.name.clone(),
            row.into(),
            mutations.into(),
            attributes.into(),
        )?;
        Ok(())
    }
}

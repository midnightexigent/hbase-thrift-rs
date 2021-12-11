use crate::{hbase::THbaseSyncClient, table::Table};
use easy_ext::ext;

#[ext(THbaseSyncClientExt)]
pub impl dyn THbaseSyncClient {
    fn table(&mut self, table_name: impl Into<Vec<u8>>) -> Table<'_> {
        Table::new(table_name, self)
    }
}

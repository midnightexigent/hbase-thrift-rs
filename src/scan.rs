use crate::{
    hbase::{TResult, TScan, TTHBaseServiceSyncClient},
    Error, Result,
};

pub struct Scan<'a> {
    hbase: &'a mut dyn TTHBaseServiceSyncClient,
    scanner_id: i32,
    num_rows: i32,
    current: std::vec::IntoIter<TResult>,
}
impl<'a> Scan<'a> {
    pub fn new(
        table: impl Into<Vec<u8>>,
        tscan: TScan,
        num_rows: i32,
        hbase: &'a mut dyn TTHBaseServiceSyncClient,
    ) -> Result<Self> {
        let scanner_id = hbase.open_scanner(table.into(), tscan)?;
        let current = hbase.get_scanner_rows(scanner_id, num_rows)?.into_iter();
        Ok(Self {
            hbase,
            scanner_id,
            current,
            num_rows,
        })
    }
}
impl Drop for Scan<'_> {
    fn drop(&mut self) {
        if let Err(err) = self.hbase.close_scanner(self.scanner_id) {
            log::warn!("failed to close scanner: {}", err)
        }
    }
}
impl Iterator for Scan<'_> {
    type Item = Result<TResult>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.current.next() {
            Some(Ok(next))
        } else {
            match self.hbase.get_scanner_rows(self.scanner_id, self.num_rows) {
                Ok(scanner_rows) => {
                    self.current = scanner_rows.into_iter();
                    self.current.next().map(Ok)
                }
                Err(err) => Some(Err(Error::Thrift(err))),
            }
        }
    }
}

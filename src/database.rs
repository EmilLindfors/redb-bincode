use std::path::Path;

use bincode::{Decode, Encode};
use redb::{ReadableTableMetadata, TableHandle, TableStats, TransactionError, UntypedTableHandle};

use super::tx::{ReadTransaction, WriteTransaction};
use crate::tx;

pub struct Database(redb::Database);

impl Database {
    /// Creates a new database with the given name and cache size.
    /// If the cache size is not provided, the default cache size is 4GB.
    pub fn new(name: impl AsRef<Path>, cache_size: Option<usize>) -> Self {
        let db = redb::Database::builder()
            .set_cache_size(cache_size.unwrap_or(4 * 1024 * 1024 * 1024))
            .create(name)
            .unwrap();
        Database(db)
    }

    fn table_iterator(&self) -> Result<impl Iterator<Item = UntypedTableHandle>, redb::Error> {
        Ok(self.begin_read()?.as_raw().list_tables()?)
    }

    pub fn table_stats(&self) -> Result<Vec<(String, TableStats)>, redb::Error> {
        let mut res = Vec::new();
        for table in self.begin_read()?.list_tables()? {
            let name = table.name().to_string();
            let stats = self.begin_read()?.as_raw().open_untyped_table(table)?;
            res.push((name, stats.stats()?));
        }

        Ok(res)
    }

    pub fn delete_table(&self, name: &str) -> Result<bool, redb::Error> {
        for table in self.table_iterator()? {
            if table.name() == name {
                return Ok(self.begin_write()?.as_raw().delete_table(table)?);
            }
        }
        Ok(false)
    }

    /// Start a read transaction.
    pub fn begin_read(&self) -> Result<tx::ReadTransaction, TransactionError> {
        Ok(ReadTransaction::from(self.0.begin_read()?))
    }

    /// Start a write transaction.
    pub fn begin_write(&self) -> Result<tx::WriteTransaction, TransactionError> {
        Ok(WriteTransaction::from(self.0.begin_write()?))
    }
}

impl From<redb::Database> for Database {
    fn from(value: redb::Database) -> Self {
        Self(value)
    }
}

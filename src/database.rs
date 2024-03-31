use std::path::Path;

use redb::TransactionError;

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

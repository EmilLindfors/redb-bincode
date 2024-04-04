use crate::Database;

pub trait Readable<K, V>
where
    K: ?Sized + bincode::Encode + bincode::Decode,
    V: ?Sized + bincode::Encode + bincode::Decode,
{
    fn get<'a>(db: &Database, table: &'a str, key: &'a K) -> Result<Option<V>, redb::Error>;

    fn get_many<'a>(
        db: &Database,
        table: &'a str,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Vec<(K, V)>, redb::Error>
    where
        K: bincode::Decode,
        V: bincode::Decode;

    fn get_many_where<'a, F>(
        db: &Database,
        table: &'a str,
        start: Option<usize>,
        end: Option<usize>,
        f: F,
    ) -> Result<Vec<(K, V)>, redb::Error>
    where
        K: bincode::Decode + 'a,
        V: bincode::Decode + 'a,
        F: FnMut((&K, &V)) -> bool;
}

impl<K: bincode::Encode + bincode::Decode, T: bincode::Encode + bincode::Decode> Readable<K, T>
    for T
{
    fn get<'a>(db: &Database, table: &'a str, key: &'a K) -> Result<Option<T>, redb::Error> {
        let txn = db.begin_read()?;
        let table = txn.open_table::<K, T>(table)?;
        let result = table
            .get(key)?
            .map(|v| v.value())
            .transpose()
            .map_err(|_| {
                redb::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "not found",
                ))
            })?;
        Ok(result)
    }

    fn get_many<'a>(
        db: &Database,
        table: &'a str,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Vec<(K, T)>, redb::Error>
    where
        K: bincode::Decode,
        T: bincode::Decode,
    {
        let txn = db.begin_read()?;
        let table = txn.open_table::<K, T>(table)?;
        Ok(table.get_many(start, end)?)
    }

    /// Get all entries that match the given predicate.
    /// Returns a vector of the entries, but does not remove them.
    /// To remove the entries, use `extract_many_where`.
    fn get_many_where<'a, F>(
        db: &Database,
        table: &'a str,
        start: Option<usize>,
        end: Option<usize>,
        f: F,
    ) -> Result<Vec<(K, T)>, redb::Error>
    where
        K: bincode::Decode + 'a,
        T: bincode::Decode + 'a,
        F: FnMut((&K, &T)) -> bool,
    {
        let txn = db.begin_read()?;
        let table = txn.open_table::<K, T>(table)?;
        table.get_many_where(start, end, f)
    }
}

pub trait Writeable<K, V>
where
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    fn insert(&self, db: &Database, table: &str, key: &K) -> Result<(), redb::Error>;
    fn extract(db: &Database, table: &str, key: &K) -> Result<Option<V>, redb::Error>;
    fn extract_many_where<F>(
        db: &Database,
        table: &str,
        f: F,
    ) -> Result<Vec<Option<(K, V)>>, redb::Error>
    where
        F: Fn((K, V)) -> bool;
    //&'a V: bincode::Decode;
}

impl<
        K: bincode::Encode + bincode::Decode + 'static,
        T: bincode::Encode + bincode::Decode + 'static,
    > Writeable<K, T> for T
{
    fn insert(&self, db: &Database, table: &str, key: &K) -> Result<(), redb::Error> {
        let txn = db.begin_write()?;
        {
            let mut table = txn.open_table::<K, T>(table)?;
            table.insert(key, self)?;
        }
        txn.commit()?;
        Ok(())
    }

    fn extract(db: &Database, table: &str, key: &K) -> Result<Option<T>, redb::Error> {
        let txn = db.begin_write()?;
        let v = {
            let mut table = txn.open_table::<K, T>(table)?;
            let v = table
                .remove(key)?
                .map(|v| v.value())
                .transpose()
                .map_err(|_| {
                    redb::Error::Io(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "not found",
                    ))
                })?;
            v
        };
        txn.commit()?;
        Ok(v)
    }

    /// Remove all entries that match the given predicate.
    /// Returns a vector of the removed entries.
    fn extract_many_where<F>(db: &Database, table: &str, f: F) -> Result<Vec<Option<(K, T)>>, redb::Error>
    where
        F: FnMut((K, T)) -> bool,
        //&'a T: bincode::Decode,
    {
        let txn = db.begin_write()?;
        let res = {
            let mut table = txn.open_table::<K, T>(table)?;

            let res = table.remove_where(f)?;

            res
        };
        txn.commit()?;

        Ok(res)
    }
}

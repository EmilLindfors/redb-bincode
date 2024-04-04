use std::borrow::Borrow;
use std::cell::UnsafeCell;
use std::fmt;
use std::marker::PhantomData;
pub use redb::StorageError;
use redb::{ReadableTable, ReadableTableMetadata};

pub const BINCODE_CONFIG: bincode::config::Configuration<bincode::config::BigEndian> =
    bincode::config::standard()
        .with_big_endian()
        .with_variable_int_encoding();

thread_local! {
    pub static ENCODE_KEY: std::cell::UnsafeCell<Vec<u8>> = const { std::cell::UnsafeCell::new(vec![]) };
    pub static ENCODE_VALUE: std::cell::UnsafeCell<Vec<u8>> = const { std::cell::UnsafeCell::new(vec![]) };
}

unsafe fn with_encode_key_buf<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    // https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html#memory-layout
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut<T>(ptr: &UnsafeCell<T>) -> &mut T {
        unsafe { &mut *ptr.get() }
    }

    ENCODE_KEY.with(|buf| {
        let buf = unsafe { get_mut(buf) };
        let res = f(buf);
        buf.clear();
        res
    })
}
unsafe fn with_encode_value_buf<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    // https://doc.rust-lang.org/std/cell/struct.UnsafeCell.html#memory-layout
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut<T>(ptr: &UnsafeCell<T>) -> &mut T {
        unsafe { &mut *ptr.get() }
    }

    ENCODE_VALUE.with(|buf| {
        let buf = unsafe { get_mut(buf) };
        let res = f(buf);
        buf.clear();
        res
    })
}

mod sort;
pub use sort::*;

mod database;
pub use database::*;

mod tx;
pub use tx::*;

mod traits;
pub use traits::*;

pub struct AccessGuard<'a, V> {
    inner: redb::AccessGuard<'a, &'static [u8]>,
    _v: PhantomData<V>,
}

impl<'a, V> From<redb::AccessGuard<'a, &'_ [u8]>> for AccessGuard<'a, V> {
    fn from(inner: redb::AccessGuard<'a, &'_ [u8]>) -> Self {
        Self {
            inner,
            _v: PhantomData,
        }
    }
}

impl<'a, V> AccessGuard<'a, V>
where
    V: bincode::Decode,
{
    pub fn value(&self) -> Result<V, bincode::error::DecodeError> {
        bincode::decode_from_slice(self.inner.value(), BINCODE_CONFIG).map(|v| v.0)
    }
}

/// A read-only table.
pub struct ReadOnlyTable<K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
{
    inner: redb::ReadOnlyTable<sort::SortKey<S>, &'static [u8]>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V, S> ReadOnlyTable<K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    /// Returns the underlying redb table.
    pub fn as_raw(&self) -> &redb::ReadOnlyTable<sort::SortKey<S>, &'static [u8]> {
        &self.inner
    }

    /// Get a value from the table by key.
    pub fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'static, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        unsafe {
            Ok(with_encode_key_buf(|buf| {
                let size = bincode::encode_into_std_write(key, buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");
                self.inner.get(&buf[..size])
            })?
            .map(AccessGuard::from))
        }
    }

    /// Get a range of values from the table.
    /// The range is inclusive on the start and exclusive on the end.
    pub fn get_many(
        &self,
        start: Option<usize>,
        end: Option<usize>,
    ) -> Result<Vec<(K, V)>, redb::Error> {
        let mut res = vec![];
        let mut i = 0;

        let mut iter = self.inner.iter()?;
        while let Some(r) = iter.next() {
            if let Some(start) = start {
                if i < start {
                    i += 1;
                    continue;
                }
            }

            if let Some(end) = end {
                if i >= end {
                    break;
                }
            }

            let (key, value) = r?;

            let key = bincode::decode_from_slice(key.value(), BINCODE_CONFIG)
                .map(|v| v.0)
                .map_err(|e| {
                    redb::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?;
            let value = bincode::decode_from_slice(value.value(), BINCODE_CONFIG)
                .map(|v| v.0)
                .map_err(|e| {
                    redb::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?;
            res.push((key, value));

            i += 1;
        }
        Ok(res)
    }

    pub fn get_many_where<'a, F>(
        &self,
        start: Option<usize>,
        end: Option<usize>,
        mut f: F,
    ) -> Result<Vec<(K, V)>, redb::Error>
    where
        F: FnMut((&K, &V)) -> bool,
    {
        let mut res = vec![];
        let mut i = 0;

        let mut iter = self.inner.iter()?;
        while let Some(r) = iter.next() {
            if let Some(start) = start {
                if i < start {
                    i += 1;
                    continue;
                }
            }

            if let Some(end) = end {
                if i >= end {
                    break;
                }
            }

            let (key, value) = r?;

            let key = bincode::decode_from_slice(key.value(), BINCODE_CONFIG)
                .map(|v| v.0)
                .map_err(|e| {
                    redb::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?;
            let value = bincode::decode_from_slice(value.value(), BINCODE_CONFIG)
                .map(|v| v.0)
                .map_err(|e| {
                    redb::Error::Io(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
                })?;

            if f((&key, &value)) {
                res.push((key, value));
            }

            i += 1;
        }
        Ok(res)
    }

    /// Get metadata about the table.
    pub fn stats(&self) -> Result<redb::TableStats, redb::StorageError> {
        self.inner.stats()
    }
}

/// A mutable table in the database.
pub struct Table<'txn, K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
{
    inner: redb::Table<'txn, sort::SortKey<S>, &'static [u8]>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<'txn, K, V, S> Table<'txn, K, V, S>
where
    S: SortOrder + fmt::Debug + 'static,
    K: bincode::Encode + bincode::Decode,
    V: bincode::Encode + bincode::Decode,
{
    pub fn as_raw(&self) -> &redb::Table<sort::SortKey<S>, &'static [u8]> {
        &self.inner
    }
    pub fn as_raw_mut(&mut self) -> &'txn mut redb::Table<'_, sort::SortKey<S>, &'static [u8]> {
        &mut self.inner
    }

    /// Get a value from the table by key.
    pub fn get<Q>(&self, key: &Q) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<Q>,
        Q: bincode::Encode + ?Sized,
    {
        unsafe {
            Ok(with_encode_key_buf(|buf| {
                let size = bincode::encode_into_std_write(key, buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");
                self.inner.get(&buf[..size])
            })?
            .map(AccessGuard::from))
        }
    }

    /// Inserts a key and value into the table.
    /// Returns the previous value, if any.
    pub fn insert<KQ, VQ>(
        &mut self,
        key: &KQ,
        value: &VQ,
    ) -> Result<Option<AccessGuard<'_, V>>, StorageError>
    where
        K: Borrow<KQ>,
        V: Borrow<VQ>,
        KQ: bincode::Encode + ?Sized,
        VQ: bincode::Encode + ?Sized,
    {
        Ok(unsafe {
            with_encode_key_buf(|key_buf| {
                let key_size = bincode::encode_into_std_write(key, key_buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");

                with_encode_value_buf(|value_buf| {
                    let value_size =
                        bincode::encode_into_std_write(value, value_buf, BINCODE_CONFIG)
                            .expect("encoding can't fail");

                    self.inner
                        .insert(&key_buf[..key_size], &value_buf[..value_size])
                })
            })
        }?
        .map(AccessGuard::from))
    }

    /// Remove a value from the table by key.
    /// Returns the value that was removed, if any.
    pub fn remove<KQ>(&mut self, key: &KQ) -> Result<Option<AccessGuard<'_, V>>, redb::Error>
    where
        K: Borrow<KQ>,
        KQ: bincode::Encode + ?Sized,
    {
        Ok(unsafe {
            with_encode_key_buf(|key_buf| {
                let key_size = bincode::encode_into_std_write(key, key_buf, BINCODE_CONFIG)
                    .expect("encoding can't fail");
                self.inner.remove(&key_buf[..key_size])
            })
        }?
        .map(AccessGuard::from))
    }

    /// Remove a range of values from the table with a given predicate.
    /// Returns a vector of the removed entries.
    pub fn remove_where<'a, F: FnMut((K, V)) -> bool>(
        &mut self,
        mut predicate: F,
    ) -> Result<Vec<Option<(K, V)>>, StorageError>
    where
        //&'a K: bincode::Decode,
        //&'a V: bincode::Decode + 'a,
        V: bincode::Decode + bincode::Encode,
        K: bincode::Decode + bincode::Encode,
    {
        let res = self
            .inner
            .extract_if(|key, value| {
                let (key, _): (K, usize) = bincode::decode_from_slice(key, BINCODE_CONFIG).unwrap();
                let (value, _): (V, usize) =
                    bincode::decode_from_slice(value, BINCODE_CONFIG).unwrap();
                predicate((key, value))
            })?
            .into_iter()
            .map(|d| {
                let (k, v) = d.unwrap();
                let key: Result<(K, usize), bincode::error::DecodeError> =
                    bincode::decode_from_slice(k.value(), BINCODE_CONFIG);
                let value: Result<(V, usize), bincode::error::DecodeError> =
                    bincode::decode_from_slice(v.value(), BINCODE_CONFIG);

                if let Ok((k, _)) = key {
                    if let Ok((v, _)) = value {
                        Some((k, v))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        Ok(res)
    }
}

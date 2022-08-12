use std::sync::Arc;

use crossbeam_epoch::pin;

use super::{page::*, stats::Stats, tree::*, Options, Result};
use crate::util::Sequencer;

#[derive(Clone)]
pub struct Map {
    raw: RawMap,
    lsn: Arc<Sequencer>,
}

impl Map {
    pub fn open(opts: Options) -> Result<Self> {
        let raw = RawMap::open(opts)?;
        raw.set_min_lsn(u64::MAX);
        let lsn = Arc::new(Sequencer::new(0));
        Ok(Self { raw, lsn })
    }

    pub fn get<F>(&self, key: &[u8], func: F) -> Result<()>
    where
        F: FnMut(Option<&[u8]>),
    {
        self.raw.get(key, u64::MAX, func)
    }

    pub fn iter(&self) -> Iter {
        self.raw.iter()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let lsn = self.lsn.inc();
        self.raw.put(key, lsn, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let lsn = self.lsn.inc();
        self.raw.delete(key, lsn)
    }

    /// Returns statistics of this map.
    pub fn stats(&self) -> Stats {
        self.raw.stats()
    }
}

#[derive(Clone)]
pub struct RawMap {
    tree: Arc<Tree>,
}

impl RawMap {
    pub fn open(opts: Options) -> Result<Self> {
        let tree = Tree::open(opts)?;
        Ok(Self {
            tree: Arc::new(tree),
        })
    }

    pub fn get<F>(&self, key: &[u8], lsn: u64, f: F) -> Result<()>
    where
        F: FnMut(Option<&[u8]>),
    {
        let guard = &pin();
        let key = Key::new(key, lsn);
        self.tree.get(key, guard).map(f)
    }

    pub fn iter(&self) -> Iter {
        Iter::new(self.tree.clone())
    }

    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.tree.insert(key, value, guard)
    }

    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.tree.insert(key, value, guard)
    }

    /// Returns statistics of this map.
    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }

    /// Returns the minimal valid LSN for reads.
    pub fn min_lsn(&self) -> u64 {
        self.tree.min_lsn()
    }

    /// Updates the minimal valid LSN for reads.
    ///
    /// Entries with smaller LSNs will be dropped later.
    pub fn set_min_lsn(&self, lsn: u64) {
        self.tree.set_min_lsn(lsn);
    }
}
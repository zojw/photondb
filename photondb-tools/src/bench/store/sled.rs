use std::sync::Arc;

use async_trait::async_trait;
use photondb::{env::Photon, Stats};
use sled::Db;

use super::Store;
use crate::bench::{Args, Result};

#[derive(Clone)]
pub(crate) struct SeldStore {
    db: Db,
}

#[async_trait]
impl Store for SeldStore {
    async fn open_table(config: Arc<Args>, _env: &Photon) -> Self {
        let db = sled::open(&config.db).unwrap();
        Self { db }
    }

    async fn put(&self, key: &[u8], _lsn: u64, value: &[u8]) -> Result<()> {
        self.db.insert(key, value).unwrap();
        Ok(())
    }

    async fn get(&self, key: &[u8], _lsn: u64) -> Result<Option<Vec<u8>>> {
        let r = self.db.get(key).unwrap();
        Ok(r.map(|ivec| ivec.to_vec()))
    }

    fn stats(&self) -> Option<Stats> {
        None
    }
}

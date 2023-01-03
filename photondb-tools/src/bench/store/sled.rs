use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use photondb::{env::Env, TableStats};
use sled::{Config, Db};

use super::Store;
use crate::bench::{Args, Result};

#[derive(Clone)]
pub(crate) struct SeldStore<E: Env + Sync + Send + 'static> {
    db: Db,
    _mark: PhantomData<E>,
}

#[async_trait]
impl<E: Env + Sync + Send + 'static> Store<E> for SeldStore<E>
where
    <E as photondb::env::Env>::JoinHandle<()>: Sync,
{
    async fn open_table(config: Arc<Args>, _env: &E) -> Self {
        let scfg = Config::new()
            .path(&config.db)
            .cache_capacity(config.cache_size + config.write_buffer_size);
        let db = scfg.open().unwrap();
        Self {
            db,
            _mark: PhantomData,
        }
    }

    async fn put(&self, key: &[u8], _lsn: u64, value: &[u8]) -> Result<()> {
        self.db.insert(key, value).unwrap();
        Ok(())
    }

    async fn get(&self, key: &[u8], _lsn: u64) -> Result<Option<Vec<u8>>> {
        let r = self.db.get(key).unwrap().map(|iv| iv.to_vec());
        Ok(r)
    }

    async fn flush(&self) {
        // self.db.flush().unwrap();
    }

    async fn wait_for_reclaiming(&self) {
        self.db.flush().unwrap();
    }

    async fn close(self) -> Result<(), Self> {
        Ok(())
    }

    fn stats(&self) -> Option<TableStats> {
        None
    }
}

impl<E: Env + Sync + Send + 'static> std::fmt::Debug for SeldStore<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SeldStore").finish()
    }
}

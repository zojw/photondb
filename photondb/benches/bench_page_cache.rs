use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use criterion::*;
use moka::future::Cache as MCache;
use photondb::{Cache, ClockCache};
use photonio::runtime::Runtime;
use rand::{rngs::SmallRng, RngCore, SeedableRng};

#[async_trait]
pub trait CacheTest: Sync + Send {
    async fn try_get_with(&self, addr: u64) -> (u64, Arc<Vec<u8>>);
}

pub struct MokaCache {
    inner: MCache<u64, Arc<Vec<u8>>>,
}

impl MokaCache {
    pub fn new(capacity: usize) -> Self {
        let cache: MCache<u64, Arc<Vec<u8>>> = MCache::builder()
            .initial_capacity(capacity / 16)
            .max_capacity(capacity as u64)
            .build();
        Self { inner: cache }
    }
}

#[async_trait]
impl CacheTest for MokaCache {
    async fn try_get_with(&self, addr: u64) -> (u64, Arc<Vec<u8>>) {
        let v = self
            .inner
            .try_get_with(addr, async move { get_fake_page(addr).await })
            .await
            .unwrap();
        (addr, v)
    }
}

async fn get_fake_page(_addr: u64) -> photondb::Result<Arc<Vec<u8>>> {
    Ok(Arc::new(vec![1u8; 8 << 10]))
}

pub struct ClockCacheImpl {
    inner: Arc<ClockCache<Arc<Vec<u8>>>>,
}

impl ClockCacheImpl {
    pub fn new(capacity: usize) -> Self {
        let inner = Arc::new(ClockCache::new((8 << 10) * capacity, 8 << 10, -1, false));
        Self { inner }
    }
}

#[async_trait]
impl CacheTest for ClockCacheImpl {
    async fn try_get_with(&self, addr: u64) -> (u64, Arc<Vec<u8>>) {
        let e = match self.inner.lookup(addr) {
            Some(e) => e,
            None => {
                let v = Arc::new(vec![1u8; 8 << 10]);
                let charge = v.len();
                self.inner.insert(addr, Some(v), charge).unwrap().unwrap()
            }
        };
        let v = e.value().to_owned();
        drop(e);
        (addr, v)
    }
}

fn bench_page_cache(c: &mut Criterion) {
    let t = 32;
    println!("bench with {t} threads");
    println!("start bench clock");
    let cache = Arc::new(ClockCacheImpl::new(2048));
    do_bench(cache, c, t);
    println!("start bench moka");
    let cache2 = Arc::new(MokaCache::new(2048));
    do_bench(cache2, c, t);
}

fn do_bench<C: CacheTest + 'static>(test_cache: Arc<C>, c: &mut Criterion, threads: usize) {
    let pool = photonio::runtime::Builder::new()
        .num_threads(threads)
        .build()
        .unwrap();
    let current = Runtime::new().unwrap();
    let mut handles = vec![];
    for i in 0..8 {
        let cache = test_cache.clone();
        let handle = pool.spawn(async move {
            let seed = 11223u64 + i;
            let mut rng = SmallRng::seed_from_u64(seed);
            let t = Instant::now();
            for _ in 0..10000 {
                let file_id = rng.next_u64() % 4096;
                let offset = rng.next_u64() % 2048;
                let addr = file_id << 32 | offset;
                let (k, _) = cache.try_get_with(addr).await;
                assert_eq!(k, addr);
            }
            println!("100000 keys cost: {:?}", t.elapsed());
        });
        handles.push(handle);
    }
    current.block_on(async move {
        for h in handles {
            h.await.unwrap();
        }
    });

    c.bench_function("cache", |bencher| {
        bencher.iter(|| {
            let cache = test_cache.clone();
            let f = async move {
                let seed = 11223u64;
                let mut rng = SmallRng::seed_from_u64(seed);
                for _ in 0..1000 {
                    let file_id = rng.next_u64() % 4096;
                    let offset = rng.next_u64() % 4096 * 4096;
                    let addr = file_id << 32 | offset;
                    let (k, _) = cache.try_get_with(addr).await;
                    assert_eq!(k, addr);
                }
            };
            current.block_on(f);
        })
    });
}

criterion_group!(benches, bench_page_cache);
criterion_main!(benches);

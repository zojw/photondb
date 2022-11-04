//! A high-performance storage engine for modern hardware and platforms.
//!
//! PhotonDB is designed from scratch to leverage the power of modern multi-core
//! chips, storage devices, operating systems, and programming languages.
//!
//! Features:
//!
//! - Latch-free data structures, scale to many cores.
//! - Log-structured persistent stores, optimized for flash storage.
//! - Asynchronous APIs and efficient file IO, powered by io_uring on Linux.
//!
//! This crate provides three sets of APIs:
//!
//! - [`Raw`]: a set of low-level APIs that can run with different environments.
//! - [`Std`]: a set of synchronous APIs based on the raw APIs that doesn't
//!   require a runtime to run.
//! - [`Photon`]: a set of asynchronous APIs based on the raw APIs that must run
//!   with the [PhotonIO] runtime.
//!
//! The [`Photon`] APIs are the default APIs that are re-exported to the
//! top-level module.
//!
//! [`Raw`]: crate::raw
//! [`Std`]: crate::std
//! [`Photon`]: crate::photon
//! [PhotonIO]: https://crates.io/crates/photonio

#![warn(missing_docs, unreachable_pub)]
#![feature(
    io_error_more,
    type_alias_impl_trait,
    hash_drain_filter,
    pointer_is_aligned
)]

mod error;
pub use error::{Error, Result};

pub mod env;
pub mod raw;
pub mod std;

pub mod photon;
pub use photon::Table;

mod tree;
pub use tree::{Options, ReadOptions, Stats, WriteOptions};

mod page;
mod page_store;
mod util;

#[cfg(test)]
mod tests {
    use ::std::{env::temp_dir, sync::Arc};

    use libc::rand;
    use tempdir::TempDir;

    use super::*;

    async fn must_put(table: &Table, i: u64, lsn: u64) {
        let buf = i.to_be_bytes();
        table.put(&buf, lsn, &buf).await.unwrap()
    }

    async fn must_get(table: &Table, i: u64, lsn: u64, expect: Option<u64>) {
        let buf = i.to_be_bytes();
        table
            .get(&buf, lsn, |v| match expect {
                Some(expect) => {
                    assert_eq!(v, Some(expect.to_be_bytes().as_slice()));
                }
                None => assert!(v.is_none()),
            })
            .await
            .unwrap();
    }

    #[photonio::test]
    async fn crud() {
        let path = TempDir::new("crud").unwrap();
        let options = Options {
            page_size: 64,
            page_chain_length: 2,
            ..Default::default()
        };
        let table = Table::open(path, options).await.unwrap();
        const N: u64 = 1024;
        for i in 0..N {
            must_put(&table, i, i).await;
            must_get(&table, i, i, Some(i)).await;
        }
        for i in 0..N {
            must_get(&table, i, i, Some(i)).await;
        }
    }

    #[photonio::test]
    async fn buf_install_not_successor() {
        use rand::{
            distributions::{Alphanumeric, DistString},
            rngs::SmallRng,
            Rng, SeedableRng,
        };

        use crate::env::Env;
        let fill_rang = |rng: &mut SmallRng, buf: &mut [u8]| {
            rng.fill(buf);
        };

        let env = crate::env::Photon;
        let rand_dir = Alphanumeric.sample_string(&mut rand::thread_rng(), 16);
        let path = temp_dir().join(rand_dir).join("abc");
        let table = Arc::new(
            crate::raw::Table::open(env.to_owned(), path, Options::default())
                .await
                .unwrap(),
        );
        let base_seed = 1667446555451976;

        let mut handles = Vec::new();
        for tid in 0..1 {
            let tid = tid.to_owned();
            let seed = base_seed + tid;
            let mut rng = SmallRng::seed_from_u64(seed);
            let table = table.clone();
            let h = env.spawn_background(async move {
                for _ in 0..100000 {
                    let mut key = vec![0u8; 16];
                    let mut value = vec![0u8; 100];
                    fill_rang(&mut rng, &mut key);
                    fill_rang(&mut rng, &mut value);
                    table.put(&key, 0, &value).await.unwrap();
                }
            });
            handles.push(h)
        }
        env.spawn_background(async {
            for handle in handles {
                handle.await;
            }
        })
        .await;
    }
}

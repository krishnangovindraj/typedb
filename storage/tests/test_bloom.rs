/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use rand::random;
use rand_core::RngCore;
use xoshiro::Xoshiro256Plus;
use test_utils::create_tmp_dir;

const N_BATCHES: usize = 100;
const BATCH_SIZE: usize = 100;
const KEY_SIZE: usize = 8;
const PREFIX_LENGTH: usize = 1;
type KeyType = [u8; KEY_SIZE];

const N_QUERIES: usize = 25;

fn rocks_database_options() -> rocksdb::Options {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    // TODO
    options
}

fn rocks_read_options() -> rocksdb::ReadOptions {
    let mut options = rocksdb::ReadOptions::default();
    // TODO
    options
}

fn rocks_write_options() -> rocksdb::WriteOptions {
    rocksdb::WriteOptions::default()
}

fn generate_key(rng: &mut Xoshiro256Plus) -> KeyType {
    // Rust's inbuilt ThreadRng is secure and slow. Xoshiro is significantly faster.
    // This ~(50 GB/s) is faster than generating 64 random bytes (~6 GB/s) or loading pre-generated (~18 GB/s).
    let mut key= [0; KEY_SIZE];
    let mut z = rng.next_u64();
    for start in (0..KEY_SIZE).step_by(8) {
        key[start..][..8].copy_from_slice(&z.to_le_bytes());
        z = u64::rotate_left(z, 1); // Rotation beats the compression.
    }
    key
}

fn load_data_non_transactional(db: &rocksdb::DB, data: impl Iterator<Item=KeyType>) {
    let mut write_batch = rocksdb::WriteBatch::default();
    data.for_each(|k| write_batch.put(k, [0]));
    db.write_opt(write_batch, &rocks_write_options()).unwrap()
}

fn iterate_prefix(db: &rocksdb::DB, prefix: &[u8; PREFIX_LENGTH]) -> Vec<KeyType> {
    let mut collected = Vec::new();
    let mut iterator = db.raw_iterator_opt(rocks_read_options());
    iterator.seek(&prefix);
    loop {
        iterator.next();
        if Some(prefix.as_slice()) != iterator.key().map(|k| &k[0..PREFIX_LENGTH]) {
            break;
        } else {
            let mut key = [0; KEY_SIZE];
            key.copy_from_slice(&iterator.key().unwrap()[0..KEY_SIZE]);
            collected.push(key);
        }
    }
    collected
}

#[test]
fn test_bloom() {
    let tmp_dir = create_tmp_dir();
    let db = rocksdb::DB::open(&rocks_database_options(), &tmp_dir.to_path_buf()).unwrap();
    println!("Opened database at: {:?}", tmp_dir.to_path_buf().as_path());

    let mut rng = Xoshiro256Plus::from_seed_u64(random());
    for _ in 0..N_BATCHES {
        load_data_non_transactional(&db, (0..BATCH_SIZE).map(|_| generate_key(&mut rng)));
    }
    println!("Loaded {N_BATCHES} batches, each of {BATCH_SIZE} keys. Each key = {KEY_SIZE}.");

    // Flushing hopefully creates an L0 and allows the bloomfilters to be used
    db.flush().unwrap();
    let mut prefix = [0; PREFIX_LENGTH];
    for _ in 0..N_QUERIES {
        prefix.copy_from_slice(&rng.next_u64().to_le_bytes()[0..PREFIX_LENGTH]);
        let keys_with_prefix = iterate_prefix(&db, &prefix);
        println!("- {prefix:?} => {keys_with_prefix:?}");
    }

    drop(tmp_dir);
}
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::time::Duration;
use rand::random;
use rand_core::RngCore;
use rocksdb::{BlockBasedIndexType, BlockBasedOptions, SliceTransform};
use rocksdb::statistics::StatsLevel;
use xoshiro::Xoshiro256Plus;
use test_utils::create_tmp_dir;

const N_BATCHES: usize = 100;
const BATCH_SIZE: usize = 100;
const KEY_SIZE: usize = 8;
const PREFIX_LENGTH: usize = 2; // if we're trying to use our filter, we need this to be high enough that some will miss.
type KeyType = [u8; KEY_SIZE];

const N_QUERIES: usize = 25;

// Less important
const STATS_DUMP_PERIOD_SECS: u32 = 3;

fn rocks_database_options() -> rocksdb::Options {
    let mut options = rocksdb::Options::default();
    options.create_if_missing(true);
    options.enable_statistics();
    options.set_stats_dump_period_sec(STATS_DUMP_PERIOD_SECS);
    options.set_statistics_level(StatsLevel::All);

    let mut block_options = BlockBasedOptions::default();
    block_options.set_bloom_filter(10.0, false);
    block_options.set_whole_key_filtering(false); // Big difference on filter_size
    block_options.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    options.set_block_based_table_factory(&block_options);
    options.set_prefix_extractor(SliceTransform::create_fixed_prefix(PREFIX_LENGTH));
    options
}

fn rocks_read_options() -> rocksdb::ReadOptions {
    let mut options = rocksdb::ReadOptions::default();
    options.set_total_order_seek(false);
    // options.set_prefix_same_as_start(true);
    options
}

fn rocks_write_options() -> rocksdb::WriteOptions {
    rocksdb::WriteOptions::default()
}

fn generate_key(rng: &mut Xoshiro256Plus) -> KeyType {
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
    let db_options = rocks_database_options();
    let db = rocksdb::DB::open(&db_options, &tmp_dir.to_path_buf()).unwrap();
    println!("Opened database at: {:?}", tmp_dir.to_path_buf().as_path());

    let mut rng = Xoshiro256Plus::from_seed_u64(random());
    for _ in 0..N_BATCHES {
        load_data_non_transactional(&db, (0..BATCH_SIZE).map(|_| generate_key(&mut rng)));
    }
    println!("Loaded {N_BATCHES} batches, each of {BATCH_SIZE} keys. Each key = {KEY_SIZE}.");

    // Flushing hopefully creates an L0 and allows the bloomfilters to be used
    db.flush().unwrap();
    println!("Sleeping for flush");
    std::thread::sleep(Duration::from_secs(2 * STATS_DUMP_PERIOD_SECS as u64));

    let mut prefix = [0; PREFIX_LENGTH];
    for _ in 0..N_QUERIES {
        prefix.copy_from_slice(&rng.next_u64().to_le_bytes()[0..PREFIX_LENGTH]);
        let keys_with_prefix = iterate_prefix(&db, &prefix);
        println!("- {prefix:?} => {keys_with_prefix:?}");
    }

    // Dumps the whole log file
    // println!("Sleeping long enough for stats dump (hopefully)");
    // std::thread::sleep(Duration::from_secs(2 * STATS_DUMP_PERIOD_SECS as u64));
    // let stats_bytes =  std::fs::File::open(tmp_dir.join("LOG")).unwrap().bytes().collect::<Result<Vec<_>, _>>().unwrap();
    // println!("{}", String::from_utf8(stats_bytes).unwrap());
    //

    // What we're interested in are those ending with seek.filtered COUNT
    println!("\n\n\n----- Print statistics ----\n\n\n");
    println!("{}", db_options.get_statistics().as_ref().map(|s| s.as_str()).unwrap_or("oops. None!"));

    drop(tmp_dir);
}
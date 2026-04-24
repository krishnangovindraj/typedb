/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{fs::File, io::Read, os::raw::c_int, path::Path, sync::Arc, time::Instant};

use criterion::{Criterion, Throughput, criterion_group, criterion_main, profiler::Profiler};
use database::{
    Database,
    database_manager::DatabaseManager,
    transaction::{TransactionRead, TransactionSchema, TransactionWrite},
};
use executor::{ExecutionInterrupt, batch::Batch, pipeline::stage::StageIterator};
use options::TransactionOptions;
use pprof::ProfilerGuard;
use storage::durability_client::WALClient;
use test_utils::{create_tmp_dir, init_logging};

const DB_NAME: &str = "bench_debug";
const RESOURCE_PATH: &str = "tests/benchmarks/debug";
const SCHEMA_FILENAME: &str = "schema.tql";
const DATA_FILENAME: &str = "data.tql";
const QUERY_FILENAME: &str = "query.tql";

fn load_schema_tql(database: Arc<Database<WALClient>>, schema_tql: &Path) {
    let mut contents = Vec::new();
    File::open(schema_tql).unwrap().read_to_end(&mut contents).unwrap();
    let schema_str = String::from_utf8(contents).unwrap();
    if schema_str.is_empty() {
        return;
    }
    let schema_query = typeql::parse_query(schema_str.as_str()).unwrap().into_structure().into_schema();

    let tx = TransactionSchema::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionSchema {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    } = tx;
    let mut inner_snapshot =
        Arc::try_unwrap(snapshot).unwrap_or_else(|_| panic!("Expected unique ownership of snapshot"));
    query_manager
        .execute_schema(
            &mut inner_snapshot,
            &type_manager,
            &thing_manager,
            &function_manager,
            schema_query,
            &schema_str,
        )
        .unwrap();
    let tx = TransactionSchema::from_parts(
        Arc::new(inner_snapshot),
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    );
    tx.commit().1.unwrap();
}

fn load_data_tql(database: Arc<Database<WALClient>>, data_tql: &Path) {
    let mut contents = Vec::new();

    File::open(data_tql).unwrap().read_to_end(&mut contents).unwrap();
    let data_str = String::from_utf8(contents).unwrap();
    if data_str.is_empty() {
        return;
    }
    let data_query = typeql::parse_query(data_str.as_str()).unwrap().into_structure().into_pipeline();
    let tx = TransactionWrite::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionWrite {
        snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    } = tx;
    let write_pipeline = query_manager
        .prepare_write_pipeline(
            Arc::try_unwrap(snapshot).unwrap_or_else(|_| panic!("Expected unique ownership of snapshot")),
            &type_manager,
            thing_manager.clone(),
            &function_manager,
            &data_query,
            &data_str,
        )
        .unwrap();
    let (_output, context) = write_pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let tx = TransactionWrite::from_parts(
        context.snapshot,
        type_manager,
        thing_manager,
        function_manager,
        query_manager,
        database,
        transaction_options,
        profile,
    );
    tx.commit().1.unwrap();
}

fn setup() -> Arc<Database<WALClient>> {
    let tmp_dir = create_tmp_dir();
    {
        let dbm = DatabaseManager::new(&tmp_dir).unwrap();
        dbm.put_database(DB_NAME).unwrap();
        let database = dbm.database(DB_NAME).unwrap();
        let schema_path = Path::new(RESOURCE_PATH).join(Path::new(SCHEMA_FILENAME));
        let data_path = Path::new(RESOURCE_PATH).join(Path::new(DATA_FILENAME));

        load_schema_tql(database.clone(), &schema_path);
        load_data_tql(database.clone(), &data_path);
    }
    let dbm = DatabaseManager::new(&tmp_dir).unwrap();
    dbm.put_database(DB_NAME).unwrap();

    dbm.database(DB_NAME).unwrap()
}

fn run_query(database: Arc<Database<WALClient>>, query_str: &str) -> Batch {
    let tx = TransactionRead::open(database.clone(), TransactionOptions::default()).unwrap();
    let TransactionRead { snapshot, query_manager, type_manager, thing_manager, function_manager, .. } = &tx;
    let query = typeql::parse_query(query_str).unwrap().into_structure().into_pipeline();
    let pipeline = query_manager
        .prepare_read_pipeline(
            snapshot.clone(),
            type_manager,
            thing_manager.clone(),
            function_manager,
            &query,
            query_str,
        )
        .unwrap();
    let (rows, context) = pipeline.into_rows_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    rows.collect_owned().unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    println!("In criterion benchmark. Process_id is {}", std::process::id());
    // init_logging();
    let mut contents = Vec::new();
    let query_path = Path::new(RESOURCE_PATH).join(Path::new(QUERY_FILENAME));
    File::open(&query_path).unwrap().read_to_end(&mut contents).unwrap();
    let query = String::from_utf8(contents).unwrap();
    if query.is_empty() {
        panic!("Query was empty. File used: {}", query_path.display());
    }

    let database = setup();
    c.bench_function("bench_debug", |b| {
        b.iter(|| {
            let _ = run_query(database.clone(), query.as_str());
        })
    });
}

pub struct FlamegraphProfiler<'a> {
    frequency: c_int,
    active_profiler: Option<ProfilerGuard<'a>>,
}

// You must specify --profile-time  for the profiler to start
impl<'a> FlamegraphProfiler<'a> {
    #[allow(dead_code)]
    pub fn new(frequency: c_int) -> Self {
        Self { frequency, active_profiler: None }
    }
}

impl<'a> Profiler for FlamegraphProfiler<'a> {
    fn start_profiling(&mut self, _benchmark_id: &str, _benchmark_dir: &Path) {
        self.active_profiler = Some(ProfilerGuard::new(self.frequency).unwrap());
    }

    fn stop_profiling(&mut self, _benchmark_id: &str, benchmark_dir: &Path) {
        std::fs::create_dir_all(benchmark_dir).unwrap();
        let flamegraph_path = benchmark_dir.join("flamegraph.svg");
        let flamegraph_file = File::create(flamegraph_path).expect("File system error while creating flamegraph.svg");
        if let Some(profiler) = self.active_profiler.take() {
            profiler.report().build().unwrap().flamegraph(flamegraph_file).expect("Error writing flamegraph");
        }
    }
}

fn profiled() -> Criterion {
    Criterion::default().with_profiler(FlamegraphProfiler::new(10))
}

// TODO: disable profiling when running on mac, since pprof seems to crash sometimes?

criterion_group!(
    name = benches;
    config = profiled();
    targets = criterion_benchmark
);
criterion_main!(benches);

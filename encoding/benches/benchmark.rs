/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

#![deny(unused_must_use)]

use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use durability::wal::WAL;
use encoding::{
    graph::{
        thing::{vertex_generator::ThingVertexGenerator, vertex_object::ObjectVertex},
        type_::vertex::TypeID,
    },
    EncodingKeyspace, Keyable,
};
use resource::constants::snapshot::BUFFER_KEY_INLINE;
use storage::{key_value::StorageKey, snapshot::WriteSnapshot, MVCCStorage};
use test_utils::{create_tmp_dir, init_logging};

fn vertex_generation<D>(
    thing_vertex_generator: Arc<ThingVertexGenerator>,
    type_id: TypeID,
    write_snapshot: &WriteSnapshot<'_, D>,
) -> ObjectVertex<'static> {
    thing_vertex_generator.create_entity(type_id, write_snapshot)
}

fn vertex_generation_to_key<D>(
    thing_vertex_generator: Arc<ThingVertexGenerator>,
    type_id: TypeID,
    write_snapshot: &WriteSnapshot<'_, D>,
) -> StorageKey<'static, { BUFFER_KEY_INLINE }> {
    thing_vertex_generator.create_entity(type_id, write_snapshot).into_storage_key()
}

fn criterion_benchmark(c: &mut Criterion) {
    init_logging();
    let storage_path = create_tmp_dir();
    let storage = MVCCStorage::<WAL>::recover::<EncodingKeyspace>("storage", &storage_path).unwrap();

    let type_id = TypeID::build(0);
    let vertex_generator = Arc::new(ThingVertexGenerator::new());

    let snapshot = storage.open_snapshot_write();
    c.bench_function("vertex_generation", |b| {
        b.iter(|| vertex_generation(vertex_generator.clone(), type_id, &snapshot))
    });

    let snapshot = storage.open_snapshot_write();
    c.bench_function("vertex_generation_to_storage_key", |b| {
        b.iter(|| vertex_generation_to_key(vertex_generator.clone(), type_id, &snapshot))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

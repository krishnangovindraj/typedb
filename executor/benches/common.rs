/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use concept::{
    thing::{statistics::Statistics, thing_manager::ThingManager},
    type_::type_manager::{type_cache::TypeCache, TypeManager},
};
use durability::{wal::WAL, DurabilitySequenceNumber};
use encoding::{
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
    EncodingKeyspace,
};
use storage::{durability_client::WALClient, sequence_number::SequenceNumber, MVCCStorage};
use test_utils::{create_tmp_dir, init_logging, TempDir};

pub fn setup_storage() -> (TempDir, Arc<MVCCStorage<WALClient>>) {
    init_logging();
    let storage_path = create_tmp_dir();
    let wal = WAL::create(&storage_path).unwrap();
    let storage =
        Arc::new(MVCCStorage::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap());
    (storage_path, storage)
}

pub fn load_managers(
    storage: Arc<MVCCStorage<WALClient>>,
    type_cache_at: Option<SequenceNumber>,
) -> (Arc<TypeManager>, ThingManager) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let cache = if let Some(sequence_number) = type_cache_at {
        Some(Arc::new(TypeCache::new(storage.clone(), sequence_number).unwrap()))
    } else {
        None
    };
    let type_manager = Arc::new(TypeManager::new(definition_key_generator, type_vertex_generator, cache));
    let thing_manager = ThingManager::new(
        thing_vertex_generator,
        type_manager.clone(),
        Arc::new(Statistics::new(DurabilitySequenceNumber::MIN)),
    );
    (type_manager, thing_manager)
}

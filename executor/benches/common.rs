/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::path::{Path, PathBuf};
use std::sync::Arc;

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use concept::type_::type_manager::type_cache::TypeCache;
use durability::wal::WAL;
use encoding::{
    graph::{
        definition::definition_key_generator::DefinitionKeyGenerator, thing::vertex_generator::ThingVertexGenerator,
        type_::vertex_generator::TypeVertexGenerator,
    },
    EncodingKeyspace,
};
use storage::{durability_client::WALClient, MVCCStorage};
use storage::sequence_number::SequenceNumber;
use test_utils::{create_tmp_dir, init_logging, TempDir};

pub fn setup_storage() -> (TempDir, Arc<MVCCStorage<WALClient>>) {
    init_logging();
    let storage_path = PathBuf::from("/mnt/disks/localssd/foo/test_sandbox");
    let wal = WAL::create(&storage_path).unwrap();
    let storage = Arc::new(
        MVCCStorage::<WALClient>::create::<EncodingKeyspace>("storage", &storage_path, WALClient::new(wal)).unwrap(),
    );
    println!("Storage path is {:?}", storage_path);
    (create_tmp_dir(), storage)
}

pub fn load_managers(storage: Arc<MVCCStorage<WALClient>>, type_cache_at: Option<SequenceNumber>) -> (Arc<TypeManager>, ThingManager) {
    let definition_key_generator = Arc::new(DefinitionKeyGenerator::new());
    let type_vertex_generator = Arc::new(TypeVertexGenerator::new());
    let thing_vertex_generator = Arc::new(ThingVertexGenerator::load(storage.clone()).unwrap());
    let cache = if let Some(sequence_number) = type_cache_at {
        Some(Arc::new(TypeCache::new(storage.clone(), sequence_number).unwrap()))
    } else {
        None
    };
    let type_manager =
        Arc::new(TypeManager::new(definition_key_generator.clone(), type_vertex_generator.clone(), cache));
    let thing_manager = ThingManager::new(thing_vertex_generator.clone(), type_manager.clone());
    (type_manager, thing_manager)
}

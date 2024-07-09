/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{borrow::Borrow, collections::HashMap, io::Read, iter::zip, sync::Arc};

use bytes::{byte_array::ByteArray, Bytes};
use concept::type_::type_manager::TypeManager;
use encoding::{
    graph::{
        definition::{
            definition_key, definition_key::DefinitionKey, definition_key_generator::DefinitionKeyGenerator,
            function::FunctionDefinition,
        },
        type_::index::{NameToFunctionDefinitionIndex, NameToStructDefinitionIndex},
    },
    value::string_bytes::StringBytes,
    AsBytes, Keyable,
};
use ir::program::function::FunctionID;
use primitive::maybe_owns::MaybeOwns;
use resource::constants::snapshot::{BUFFER_KEY_INLINE, BUFFER_VALUE_INLINE};
use storage::{
    key_range::KeyRange,
    snapshot::{ReadableSnapshot, WritableSnapshot},
};

use crate::{
    function::{Function, StoredFunction},
    function_cache::ReadOnlyFunctionCache,
    ir_builder::IRBuilder,
    FunctionDefinitionError, FunctionReadError,
};

/// Analogy to TypeManager, but specialised just for Functions
pub struct FunctionManager {
    definition_key_generator: Arc<DefinitionKeyGenerator>,
    function_cache: Option<Arc<ReadOnlyFunctionCache>>,
}

impl FunctionManager {
    pub fn new(
        definition_key_generator: Arc<DefinitionKeyGenerator>,
        function_cache: Option<Arc<ReadOnlyFunctionCache>>,
    ) -> Self {
        FunctionManager { definition_key_generator, function_cache }
    }

    pub fn define_functions(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        definitions: Vec<String>,
    ) -> Result<Vec<StoredFunction>, FunctionDefinitionError> {
        let mut functions = Vec::new();
        for definition in &definitions {
            let definition_key = self
                .definition_key_generator
                .create_function(snapshot)
                .map_err(|source| FunctionDefinitionError::Encoding { source })?;
            let function =
                StoredFunction::build(FunctionID::Schema(definition_key), FunctionDefinition::build_ref(&definition))?;
            let index_key =
                NameToStructDefinitionIndex::build::<BUFFER_KEY_INLINE>(StringBytes::build_ref(&function.name))
                    .into_storage_key();
            let existing_opt = snapshot
                .get::<BUFFER_VALUE_INLINE>(index_key.as_reference())
                .map_err(|source| FunctionDefinitionError::SnapshotGet { source })?;
            if let Some(_) = existing_opt {
                Err(FunctionDefinitionError::FunctionAlreadyExists { name: function.name })?;
            } else {
                functions.push(function);
            }
        }

        // TODO: We'd ideally keep an up-to-date cache during schema functions.
        let existing_functions = self.function_cache.as_ref().unwrap();
        let function_ir = IRBuilder::compile_functions(existing_functions, &functions)?; // TODO

        for (function, definition) in zip(functions.iter(), definitions.iter()) {
            let index_key =
                NameToStructDefinitionIndex::build::<BUFFER_KEY_INLINE>(StringBytes::build_ref(&function.name))
                    .into_storage_key();
            let definition_key = &function.function_id;
            snapshot.put_val(index_key.into_owned_array(), ByteArray::copy(definition_key.bytes().bytes()));
            snapshot.put_val(
                definition_key.clone().into_storage_key().into_owned_array(),
                FunctionDefinition::build_ref(&definition).into_bytes().into_array(),
            );
        }
        Ok(functions)
    }

    pub fn lookup_function(
        &self,
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, FunctionReadError> {
        if let Some(cache) = &self.function_cache {
            Ok(cache.lookup_function(name))
        } else {
            Ok(Self::lookup_function_impl(snapshot, name)?)
        }
    }

    pub fn get_function(
        &self,
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'static>,
    ) -> Result<MaybeOwns<StoredFunction>, FunctionReadError> {
        // TODO: return option?
        if let Some(cache) = &self.function_cache {
            Ok(MaybeOwns::Borrowed(cache.get_function(definition_key).unwrap()))
        } else {
            Ok(MaybeOwns::Owned(Self::get_function_impl(snapshot, definition_key)?))
        }
    }

    pub(crate) fn list_functions_impl(
        snapshot: &impl ReadableSnapshot,
    ) -> Result<Vec<StoredFunction>, FunctionReadError> {
        snapshot
            .iterate_range(KeyRange::new_within(
                DefinitionKey::build_prefix(FunctionDefinition::PREFIX),
                DefinitionKey::FIXED_WIDTH_ENCODING,
            ))
            .collect_cloned_vec(|key, value| {
                Function::build(
                    FunctionID::Schema(DefinitionKey::new(Bytes::Reference(key.byte_ref()).into_owned())),
                    FunctionDefinition::new(Bytes::Reference(value).into_owned()),
                )
                .unwrap()
            })
            .map_err(|source| FunctionReadError::SnapshotIterate { source })
    }

    pub(crate) fn lookup_function_impl(
        snapshot: &impl ReadableSnapshot,
        name: &str,
    ) -> Result<Option<DefinitionKey<'static>>, FunctionReadError> {
        let index_key = NameToFunctionDefinitionIndex::build(StringBytes::<BUFFER_KEY_INLINE>::build_ref(name));
        let bytes_opt = snapshot
            .get(index_key.into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::SnapshotGet { source })?;
        Ok(bytes_opt.map(|bytes| DefinitionKey::new(Bytes::Array(bytes))))
    }

    pub(crate) fn get_function_impl(
        snapshot: &impl ReadableSnapshot,
        definition_key: DefinitionKey<'static>,
    ) -> Result<StoredFunction, FunctionReadError> {
        let bytes = snapshot
            .get::<BUFFER_VALUE_INLINE>(definition_key.clone().into_storage_key().as_reference())
            .map_err(|source| FunctionReadError::SnapshotGet { source })?
            .unwrap();
        Ok(Function::build(FunctionID::Schema(definition_key), FunctionDefinition::new(Bytes::Array(bytes))).unwrap())
    }
}

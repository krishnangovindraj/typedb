/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, error::Error, fmt, iter::zip, sync::Arc};

use concept::type_::type_manager::TypeManager;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::{
    inference::{
        type_inference::{infer_types_for_functions, FunctionAnnotations},
        TypeInferenceError,
    },
    program::{
        function::{FunctionIDTrait, FunctionIR},
        program::{CompiledFunctionCache, SchemaFunctionCache},
    },
};
use storage::{sequence_number::SequenceNumber, MVCCStorage, ReadSnapshotOpenError};

use crate::{
    function::{Function, StoredFunction},
    function_manager::FunctionManager,
    ir_builder::IRBuilder,
    FunctionReadError,
};

pub(crate) struct ReadOnlyFunctionCache {
    uncompiled: Box<[Option<StoredFunction>]>,
    compiled: Arc<SchemaFunctionCache>,
    index: HashMap<String, DefinitionKey<'static>>,
}

impl ReadOnlyFunctionCache {
    pub fn empty() -> Self {
        Self { uncompiled: Box::new([]), compiled: Arc::new(SchemaFunctionCache::empty()), index: HashMap::new() }
    }
    pub fn new<D>(
        storage: Arc<MVCCStorage<D>>,
        type_manager: &TypeManager,
        open_sequence_number: SequenceNumber,
    ) -> Result<Self, FunctionCacheCreateError> {
        let snapshot = storage
            .open_snapshot_read_at(open_sequence_number)
            .map_err(|error| FunctionCacheCreateError::SnapshotOpen { source: error })?;

        let functions = FunctionManager::list_functions_impl(&snapshot)
            .map_err(|source| FunctionCacheCreateError::FunctionRead { source })?;

        // Prepare ir & annotations
        let empty_cache = Self::empty();
        let ir = IRBuilder::compile_functions(&empty_cache, &functions).unwrap();
        let mut local_function_cache =
            infer_types_for_functions(ir, &snapshot, &type_manager, &SchemaFunctionCache::empty())
                .map_err(|source| FunctionCacheCreateError::TypeInference { source })?;
        let (mut boxed_ir, mut boxed_annotations) = local_function_cache.into_parts();

        let required_cache_count =
            functions.iter().map(|function| function.function_id.as_usize() + 1).max().unwrap_or(0);
        let mut function_cache = (0..required_cache_count).map(|_| None).collect::<Box<[Option<StoredFunction>]>>();
        let mut ir_cache = (0..required_cache_count).map(|_| None).collect::<Box<[Option<FunctionIR>]>>();
        let mut annotations_cache =
            (0..required_cache_count).map(|_| None).collect::<Box<[Option<FunctionAnnotations>]>>();

        let zipped =
            zip(functions.into_iter(), zip(boxed_ir.into_vec().into_iter(), boxed_annotations.into_vec().into_iter()));
        let mut index = HashMap::new();
        for (function, (ir, annotations)) in zipped {
            index.insert(function.name.clone(), function.function_id.clone());
            let cache_index = function.function_id.as_usize();
            function_cache[cache_index] = Some(function);
            ir_cache[cache_index] = Some(ir);
            annotations_cache[cache_index] = Some(annotations);
        }
        let compiled_cache = SchemaFunctionCache::new(ir_cache, annotations_cache);
        Ok(Self { uncompiled: function_cache, compiled: Arc::new(compiled_cache), index })
    }

    pub(crate) fn lookup_function(&self, name: &str) -> Option<DefinitionKey<'static>> {
        self.index.get(name).map(|key| key.clone())
    }

    pub(crate) fn get_function(
        &self,
        definition_key: DefinitionKey<'static>,
    ) -> Option<&Function<DefinitionKey<'static>>> {
        self.uncompiled[definition_key.as_usize()].as_ref()
    }

    pub(crate) fn get_function_ir(&self, definition_key: DefinitionKey<'static>) -> Option<&FunctionIR> {
        self.compiled.get_function_ir(definition_key)
    }

    pub(crate) fn get_function_annotations(
        &self,
        definition_key: DefinitionKey<'static>,
    ) -> Option<&FunctionAnnotations> {
        self.compiled.get_function_annotations(definition_key)
    }

    pub(crate) fn ir_cache(&self) -> &SchemaFunctionCache {
        &self.compiled
    }
}

#[derive(Debug)]
pub enum FunctionCacheCreateError {
    SnapshotOpen { source: ReadSnapshotOpenError },
    FunctionRead { source: FunctionReadError },
    TypeInference { source: TypeInferenceError },
}

impl fmt::Display for FunctionCacheCreateError {
    fn fmt(&self, _f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            _ => todo!(),
        }
    }
}

impl Error for FunctionCacheCreateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotOpen { source } => Some(source),
            Self::TypeInference { source } => Some(source),
            Self::FunctionRead { source } => Some(source),
        }
    }
}

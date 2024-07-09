/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt::Display, sync::Arc};

use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    inference::type_inference::{FunctionAnnotations, TypeAnnotations},
    program::{
        block::FunctionalBlock,
        function::{FunctionID, FunctionIDTrait, FunctionIR},
    },
};

pub struct Program {
    entry: FunctionalBlock,
    functions: Vec<FunctionIR>,
}

impl Program {
    pub fn new(entry: FunctionalBlock, functions: Vec<FunctionIR>) -> Self {
        // TODO: verify exactly the required functions are provided
        // TODO: ^ Why? I've since interpreted it as the query-local functions
        debug_assert!(Self::all_variables_categorised(&entry));
        Self { entry, functions }
    }

    pub fn entry(&self) -> &FunctionalBlock {
        &self.entry
    }

    pub fn entry_mut(&mut self) -> &mut FunctionalBlock {
        &mut self.entry
    }

    pub(crate) fn functions(&self) -> &Vec<FunctionIR> {
        &self.functions
    }

    pub(crate) fn into_parts(self) -> (FunctionalBlock, Vec<FunctionIR>) {
        let Self { entry, functions } = self;
        (entry, functions)
    }

    pub fn compile(match_: &typeql::query::stage::Match) -> Self {
        let _entry = FunctionalBlock::from_match(match_);
        todo!()
    }

    fn all_variables_categorised(block: &FunctionalBlock) -> bool {
        let context = block.context();
        let mut variables = context.get_variables();
        variables.all(|var| context.get_variable_category(var).is_some())
    }
}

pub struct AnnotatedProgram {
    pub(crate) entry: FunctionalBlock,
    pub(crate) entry_annotations: TypeAnnotations,
    pub(crate) local_functions: LocalFunctionCache,
    pub(crate) schema_functions: Arc<SchemaFunctionCache>,
}

impl AnnotatedProgram {
    pub(crate) fn new(
        entry: FunctionalBlock,
        entry_annotations: TypeAnnotations,
        local_functions: LocalFunctionCache,
        schema_functions: Arc<SchemaFunctionCache>,
    ) -> Self {
        Self { entry, entry_annotations, local_functions, schema_functions }
    }

    fn get_function_ir(&self, function_id: FunctionID) -> Option<&FunctionIR> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get_function_ir(definition_key),
            FunctionID::QueryLocal(index) => self.local_functions.get_function_ir(index),
        }
    }

    fn get_function_annotations(&self, function_id: FunctionID) -> Option<&FunctionAnnotations> {
        match function_id {
            FunctionID::Schema(definition_key) => self.schema_functions.get_function_annotations(definition_key),
            FunctionID::QueryLocal(index) => self.local_functions.get_function_annotations(index),
        }
    }
}

pub struct SchemaFunctionCache {
    ir: Box<[Option<FunctionIR>]>,
    annotations: Box<[Option<FunctionAnnotations>]>,
}

impl SchemaFunctionCache {
    pub fn new(ir: Box<[Option<FunctionIR>]>, annotations: Box<[Option<FunctionAnnotations>]>) -> Self {
        Self { ir, annotations }
    }

    pub fn empty() -> Self {
        Self { ir: Box::new([]), annotations: Box::new([]) }
    }
}

impl CompiledFunctionCache for SchemaFunctionCache {
    type KeyType = DefinitionKey<'static>;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR> {
        self.ir.get(id.as_usize())?.as_ref()
    }

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations> {
        self.annotations.get(id.as_usize())?.as_ref()
    }
}

pub trait CompiledFunctionCache {
    type KeyType;
    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR>;

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations>;
}

// Also used during compilation
pub struct LocalFunctionCache {
    ir: Box<[FunctionIR]>,
    annotations: Box<[FunctionAnnotations]>,
}

impl LocalFunctionCache {
    pub fn new(ir: Box<[FunctionIR]>, annotations: Box<[FunctionAnnotations]>) -> Self {
        Self { ir, annotations }
    }

    pub fn iter_ir(&self) -> impl Iterator<Item = &FunctionIR> {
        self.ir.iter()
    }

    pub fn into_parts(self) -> (Box<[FunctionIR]>, Box<[FunctionAnnotations]>) {
        let Self { ir, annotations } = self;
        (ir, annotations)
    }
}

impl CompiledFunctionCache for LocalFunctionCache {
    type KeyType = usize;

    fn get_function_ir(&self, id: Self::KeyType) -> Option<&FunctionIR> {
        self.ir.get(id)
    }

    fn get_function_annotations(&self, id: Self::KeyType) -> Option<&FunctionAnnotations> {
        self.annotations.get(id)
    }
}

// TODO: Absorb into IR cache
type QueryFunctionAnnotationsCache = Box<[Option<TypeAnnotations>]>;
type SchemaFunctionAnnotationsCache = Box<[Option<TypeAnnotations>]>;

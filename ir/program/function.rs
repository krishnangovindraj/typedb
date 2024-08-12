/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};

use crate::program::block::{FunctionalBlock, MultiBlockContext};

pub type PlaceholderTypeQLReturnOperation = String;

#[derive(Debug, Clone)]
pub struct Function {
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
    block: FunctionalBlock,
    block_context: MultiBlockContext,
    return_operation: ReturnOperation,
}

impl Function {
    pub fn new<'a>(
        block: FunctionalBlock,
        block_context: MultiBlockContext,
        arguments: Vec<Variable>,
        return_operation: ReturnOperation,
    ) -> Self {
        Self { block, block_context, arguments, return_operation }
    }

    pub fn arguments(&self) -> &Vec<Variable> {
        &self.arguments
    }
    pub fn block(&self) -> &FunctionalBlock {
        &self.block
    }
    pub fn context(&self) -> &MultiBlockContext {
        &self.block_context
    }
    pub fn return_operation(&self) -> &ReturnOperation {
        &self.return_operation
    }
}

#[derive(Debug, Clone)]
pub enum ReturnOperation {
    Stream(Vec<Variable>),
    Single(Vec<Reducer>),
}

impl ReturnOperation {
    pub fn output_annotations(
        &self,
        function_variable_annotations: &HashMap<Variable, Arc<HashSet<Type>>>,
    ) -> Vec<BTreeSet<Type>> {
        match self {
            ReturnOperation::Stream(vars) => {
                let inputs = vars.iter().map(|v| function_variable_annotations.get(v).unwrap());
                inputs
                    .map(|types_as_arced_hashset| BTreeSet::from_iter(types_as_arced_hashset.iter().map(|t| t.clone())))
                    .collect()
            }
            ReturnOperation::Single(_) => {
                todo!()
            }
        }
    }
}

impl ReturnOperation {
    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_) => true,
            Self::Single(_) => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum Reducer {
    Count(ReducerInput),
    Sum(ReducerInput),
    // First, Any etc.
}

#[derive(Debug, Clone)]
pub enum ReducerInput {
    Variable,
    Reducer,
}

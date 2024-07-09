/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    borrow::Borrow,
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fmt::{Display, Formatter},
    iter::zip,
    sync::Arc,
};

use answer::{variable::Variable, Type};
use encoding::graph::definition::definition_key::DefinitionKey;

use crate::{
    inference::type_inference::TypeAnnotations,
    pattern::{function_call::FunctionCall, variable_category::VariableOptionality},
    program::block::FunctionalBlock,
    PatternDefinitionError,
};

pub type PlaceholderTypeQLReturnOperation = String;

pub struct FunctionIR {
    // Variable categories for args & return can be read from the block's context.
    arguments: Vec<Variable>,
    block: FunctionalBlock,
    return_operation: ReturnOperationIR,
}

impl FunctionIR {
    pub fn new<'a>(
        block: FunctionalBlock,
        arguments: impl Iterator<Item = &'a str>,
        return_operation: ReturnOperationIR, // TODO: There's an asymmetry because return_operation & FunctionalBlock are already compiled. Arguments is not.
    ) -> Result<Self, PatternDefinitionError> {
        let mut argument_variables = Vec::new();
        {
            let context = block.context();
            for arg in arguments {
                let var = context.get_variable(arg).ok_or_else(|| PatternDefinitionError::FunctionArgumentUnused {
                    argument_variable: arg.to_string(),
                })?;
                argument_variables.push(var);
            }
        }
        Ok(Self { arguments: argument_variables, block, return_operation: return_operation })
    }

    pub fn create_call(
        called_id: FunctionID,
        called_ir: &FunctionIR,
        caller_variables: Vec<Variable>,
    ) -> Result<FunctionCall<Variable>, PatternDefinitionError> {
        // TODO: Consider optional arguments
        let mut call_variable_mapping = BTreeMap::new();
        let mut call_variable_categories = HashMap::new();
        if caller_variables.len() != called_ir.arguments.len() {
            Err(PatternDefinitionError::ArgumentCountMismatch {
                expected: called_ir.arguments.len(),
                actual: caller_variables.len(),
            })?
        }
        let block_context = &called_ir.block.context();
        for (idx, (call_var, sig_var)) in zip(&caller_variables, &called_ir.arguments).enumerate() {
            call_variable_mapping.insert(*call_var, idx);
            call_variable_categories.insert(*call_var, block_context.get_variable_category(*sig_var).unwrap());
        }

        let return_stream = matches!(&called_ir.return_operation, ReturnOperationIR::Stream(_));
        let returns = called_ir
            .return_operation
            .variables()
            .iter()
            .map(|variable| {
                let optionality = if block_context.is_variable_optional(*variable) {
                    VariableOptionality::Optional
                } else {
                    VariableOptionality::Required
                };
                (block_context.get_variable_category(*variable).unwrap(), optionality)
            })
            .collect();
        Ok(FunctionCall::new(called_id, call_variable_mapping, call_variable_categories, returns, return_stream))
    }

    pub fn arguments(&self) -> &Vec<Variable> {
        &self.arguments
    }
    pub fn block(&self) -> &FunctionalBlock {
        &self.block
    }
    pub fn return_operation(&self) -> &ReturnOperationIR {
        &self.return_operation
    }
}

pub enum ReturnOperationIR {
    Stream(Vec<Variable>),
    Single(Reducer, Vec<Variable>),
}

impl ReturnOperationIR {
    pub(crate) fn output_annotations(
        &self,
        function_variable_annotations: &HashMap<Variable, Arc<HashSet<Type>>>,
    ) -> Vec<BTreeSet<Type>> {
        let inputs = self.variables().iter().map(|v| function_variable_annotations.get(v).unwrap());
        match self {
            ReturnOperationIR::Stream(vars) => inputs
                .map(|types_as_arced_hashset| BTreeSet::from_iter(types_as_arced_hashset.iter().map(|t| t.clone())))
                .collect(),
            ReturnOperationIR::Single(_, _) => {
                todo!()
            }
        }
    }
}

impl ReturnOperationIR {
    pub(crate) fn variables(&self) -> &Vec<Variable> {
        match self {
            Self::Stream(v) | Self::Single(_, v) => v,
        }
    }

    pub(crate) fn is_stream(&self) -> bool {
        match self {
            Self::Stream(_) => true,
            Self::Single(_, _) => false,
        }
    }
}

pub enum Reducer {
    Count,
    Sum,
    // First, Any etc.
}

pub trait FunctionIDTrait: Clone + Into<FunctionID> {
    fn as_usize(&self) -> usize;
}

impl FunctionIDTrait for DefinitionKey<'static> {
    fn as_usize(&self) -> usize {
        self.definition_id().as_uint() as usize
    }
}

impl FunctionIDTrait for usize {
    fn as_usize(&self) -> usize {
        *self
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum FunctionID {
    Schema(DefinitionKey<'static>),
    QueryLocal(usize),
}

impl FunctionID {
    pub fn as_definition_key(&self) -> Option<DefinitionKey<'static>> {
        if let FunctionID::Schema(definition_key) = self {
            Some(definition_key.clone())
        } else {
            None
        }
    }

    pub fn as_query_local_index(&self) -> Option<usize> {
        if let FunctionID::QueryLocal(index) = self {
            Some(*index)
        } else {
            None
        }
    }

    pub fn as_usize(&self) -> usize {
        match self {
            FunctionID::Schema(id) => id.definition_id().as_uint() as usize,
            FunctionID::QueryLocal(id) => *id,
        }
    }
}

impl Display for FunctionID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionID::Schema(definition_key) => {
                write!(f, "SchemaFunction#{}", definition_key.definition_id().as_uint())
            }
            FunctionID::QueryLocal(index) => write!(f, "QueryFunction#{}", index),
        }
    }
}

impl Into<FunctionID> for usize {
    fn into(self) -> FunctionID {
        FunctionID::QueryLocal(self)
    }
}

impl Into<FunctionID> for DefinitionKey<'static> {
    fn into(self) -> FunctionID {
        FunctionID::Schema(self)
    }
}

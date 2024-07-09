/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, HashMap};

use answer::variable::Variable;
use ir::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        function_call::FunctionCall,
        variable_category::VariableCategory,
    },
    program::{
        block::{BlockContext, FunctionalBlock},
        function::{FunctionID, FunctionIDTrait, FunctionIR},
        program::Program,
    },
};
use storage::snapshot::ReadableSnapshot;

use crate::{
    function::{Function, FunctionReturn, PlaceholderForTypeQLFunctionalBlock},
    function_cache::ReadOnlyFunctionCache,
    FunctionDefinitionError, FunctionReadError,
};

// TODO: Move to wherever we build IR from TypeQL.

pub struct IRBuilder {}
impl IRBuilder {
    pub fn compile_functions<IDType: FunctionIDTrait>(
        existing_functions: &ReadOnlyFunctionCache,
        functions_to_compile: &Vec<Function<IDType>>,
    ) -> Result<Vec<FunctionIR>, FunctionDefinitionError> {
        let function_ir_index: HashMap<String, usize> =
            functions_to_compile.iter().enumerate().map(|(idx, function)| (function.name.clone(), idx)).collect();
        let ir: Result<Vec<FunctionIR>, FunctionDefinitionError> = functions_to_compile
            .iter()
            .map(|function| {
                Self::compile_function(existing_functions, functions_to_compile, &function_ir_index, function)
            })
            .collect();
        Ok(ir?)
    }

    fn compile_function<IDType: FunctionIDTrait>(
        schema_function_cache: &ReadOnlyFunctionCache,
        functions_to_compile: &Vec<Function<IDType>>,
        function_index: &HashMap<String, usize>,
        function: &Function<IDType>,
    ) -> Result<FunctionIR, FunctionDefinitionError> {
        // We'll use the FunctionValuePrototype as a stand-in for the VariableCategory
        let function_body_constraints: Vec<bool> = Vec::new(); // TODO

        let mut block = FunctionalBlock::builder();
        let context = block.context_mut();
        for unparsed_constraint in function_body_constraints {
            match unparsed_constraint {
                // Stand in for me to be able to test
                true => {
                    let function_call = Self::create_call::<IDType>(
                        context,
                        &mut block.conjunction_mut(),
                        schema_function_cache,
                        function_index,
                        functions_to_compile,
                        todo!(),
                    );
                }
                false => {}
            }
        }

        let return_operation = todo!(); // TODO:
        let ir =
            FunctionIR::new(block.finish(), function.arguments.iter().map(|arg| arg.name.as_str()), return_operation)
                .map_err(|source| FunctionDefinitionError::PatternDefinition { source })?;
        Ok(ir)
    }

    fn create_call<IDType: FunctionIDTrait>(
        context: &mut BlockContext,
        caller_conjunction: &mut ConjunctionBuilder,
        existing_functions: &ReadOnlyFunctionCache,
        function_index: &HashMap<String, usize>,
        functions_to_compile: &Vec<Function<IDType>>,
        call: PlaceholderForTypeQLFunctionCall,
    ) -> Result<FunctionCall<Variable>, FunctionReadError> {
        // TODO: Refine

        if let Some(local_function_index) = function_index.get(&call.name) {
            let function: &Function<IDType> = &functions_to_compile[local_function_index.as_usize()];

            let call_variable_mapping: BTreeMap<Variable, usize> = call
                .args
                .iter()
                .enumerate()
                .map(|(i, caller_var)| {
                    caller_conjunction
                        .get_or_declare_variable(caller_var)
                        .map(|var| (var, i))
                        .map_err(|source| FunctionReadError::PatternDefinition { source })
                })
                .collect::<Result<BTreeMap<Variable, usize>, FunctionReadError>>()?;

            let call_variable_categories: HashMap<Variable, VariableCategory> = call_variable_mapping
                .iter()
                .map(|(var, i)| (*var, function.arguments[*i].type_.clone().into()))
                .collect();

            let returns = match &function.return_type {
                FunctionReturn::Stream(prototypes) | FunctionReturn::Single(prototypes) => prototypes.iter(),
            }
            .map(|prototype| (prototype.clone().into(), prototype.clone().into()))
            .collect();

            let return_is_stream = matches!(&function.return_type, FunctionReturn::Stream(_));
            Ok(FunctionCall::new(
                function.function_id.clone().into(),
                call_variable_mapping,
                call_variable_categories,
                returns,
                return_is_stream,
            ))
        } else if let Some(definition_key) = existing_functions.lookup_function(&call.name) {
            let ir = existing_functions.get_function_ir(definition_key.clone()).unwrap();
            let call_variables = call
                .args
                .iter()
                .map(|caller_var| {
                    caller_conjunction
                        .get_or_declare_variable(&caller_var)
                        .map_err(|source| FunctionReadError::PatternDefinition { source })
                })
                .collect::<Result<Vec<Variable>, FunctionReadError>>()?;

            Ok(FunctionIR::create_call(FunctionID::Schema(definition_key.clone()), &ir, call_variables)
                .map_err(|source| FunctionReadError::PatternDefinition { source })?)
        } else {
            Err(FunctionReadError::FunctionNotFound { name: call.name.clone() })
        }
    }
}

type PlaceholderTypeQLVariable = String;
struct PlaceholderForTypeQLFunctionCall {
    name: String,
    args: Vec<PlaceholderTypeQLVariable>, // $arg = $var is supported?
    assign: Vec<PlaceholderTypeQLVariable>,
}

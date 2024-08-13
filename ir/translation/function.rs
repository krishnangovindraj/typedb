/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable::Variable;
use typeql::{
    schema::definable::function::{Output, ReturnSingle, ReturnStatement, ReturnStream, SingleOutput},
    TypeRefAny,
};

use crate::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    program::{
        block::{BlockContext, FunctionalBlock},
        function::{Function, Reducer, ReturnOperation},
        function_signature::{FunctionID, FunctionSignature, FunctionSignatureIndex},
        FunctionDefinitionError,
    },
    translation::match_::translate_match,
};

pub fn translate_function(
    function_index: &impl FunctionSignatureIndex,
    function: &typeql::Function,
) -> Result<Function, FunctionDefinitionError> {
    let mut context = BlockContext::new();
    let block = translate_match(&mut context, function_index, &function.body)
        .map_err(|source| FunctionDefinitionError::PatternDefinition { source })?
        .finish();

    let return_operation = match &function.return_stmt {
        ReturnStatement::Stream(stream) => build_return_stream(&context, &block, stream),
        ReturnStatement::Single(single) => build_return_single(&context, &block, single),
    }?;

    let arguments: Vec<Variable> = function
        .signature
        .args
        .iter()
        .map(|typeql_arg| {
            get_variable_in_block(&context, &block, &typeql_arg.var, |var| {
                FunctionDefinitionError::FunctionArgumentUnused { argument_variable: var.name().unwrap().to_string() }
            })
        })
        .collect::<Result<Vec<_>, FunctionDefinitionError>>()?;

    Ok(Function::new(block, context, arguments, return_operation))
}

pub fn build_signature(function_id: FunctionID, function: &typeql::Function) -> FunctionSignature {
    let args = function
        .signature
        .args
        .iter()
        .map(|arg| type_any_to_category_and_optionality(&arg.type_).0)
        .collect::<Vec<_>>();

    let return_is_stream = matches!(function.signature.output, Output::Stream(_));
    let returns = match &function.signature.output {
        Output::Stream(stream) => &stream.types,
        Output::Single(single) => &single.types,
    }
    .iter()
    .map(|type_any| type_any_to_category_and_optionality(type_any))
    .collect::<Vec<_>>();
    FunctionSignature::new(function_id.clone().into(), args, returns, return_is_stream)
}

fn type_any_to_category_and_optionality(type_any: &TypeRefAny) -> (VariableCategory, VariableOptionality) {
    match type_any {
        TypeRefAny::Type(_) => (VariableCategory::Thing, VariableOptionality::Required),
        TypeRefAny::Optional(_) => (VariableCategory::Thing, VariableOptionality::Optional),
        TypeRefAny::List(_) => (VariableCategory::ThingList, VariableOptionality::Required),
    }
}

fn build_return_stream(
    context: &BlockContext,
    block: &FunctionalBlock,
    stream: &ReturnStream,
) -> Result<ReturnOperation, FunctionDefinitionError> {
    let variables = stream
        .vars
        .iter()
        .map(|typeql_var| {
            get_variable_in_block(context, block, typeql_var, |var| {
                FunctionDefinitionError::ReturnVariableUnavailable { variable: var.name().unwrap().to_string() }
            })
        })
        .collect::<Result<Vec<Variable>, FunctionDefinitionError>>()?;
    Ok(ReturnOperation::Stream(variables))
}

fn build_return_single(
    context: &BlockContext,
    block: &FunctionalBlock,
    single: &ReturnSingle,
) -> Result<ReturnOperation, FunctionDefinitionError> {
    let reducers = single
        .outputs
        .iter()
        .map(|output| build_return_single_output(context, block, output))
        .collect::<Result<Vec<Reducer>, FunctionDefinitionError>>()?;
    Ok(ReturnOperation::Single(reducers))
}

fn build_return_single_output(
    context: &BlockContext,
    block: &FunctionalBlock,
    single_output: &SingleOutput,
) -> Result<Reducer, FunctionDefinitionError> {
    todo!()
}

fn get_variable_in_block<F>(
    context: &BlockContext,
    block: &FunctionalBlock,
    typeql_var: &typeql::Variable,
    err: F,
) -> Result<Variable, FunctionDefinitionError>
where
    F: FnOnce(&typeql::Variable) -> FunctionDefinitionError,
{
    context
        .get_variable_named(typeql_var.name().unwrap(), block.scope_id())
        .map_or_else(|| Err(err(typeql_var)), |var| Ok(var.clone()))
}

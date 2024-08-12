/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::program::{
    block::{FunctionalBlock, MultiBlockContext},
    function::Function,
    function_signature::FunctionSignatureIndex,
};

pub struct Program {
    context: MultiBlockContext,
    entry: FunctionalBlock,
    functions: Vec<Function>,
}

impl Program {
    pub fn new(context: MultiBlockContext, entry: FunctionalBlock, functions: Vec<Function>) -> Self {
        // TODO: verify exactly the required functions are provided
        // TODO: ^ Why? I've since interpreted it as the query-local functions
        debug_assert!(Self::all_variables_categorised(&context));
        Self { context, entry, functions }
    }

    pub fn entry(&self) -> &FunctionalBlock {
        &self.entry
    }

    pub fn entry_mut(&mut self) -> &mut FunctionalBlock {
        &mut self.entry
    }

    pub fn functions(&self) -> &Vec<Function> {
        &self.functions
    }

    pub fn into_parts(self) -> (MultiBlockContext, FunctionalBlock, Vec<Function>) {
        let Self { context, entry, functions } = self;
        (context, entry, functions)
    }

    // pub fn compile<'index>(
    //     function_index: &impl FunctionSignatureIndex,
    //     match_: &typeql::query::stage::Match,
    //     preamble_functions: Vec<&typeql::Function>,
    // ) -> Result<Self, ProgramDefinitionError> {
    //     let functions: Vec<Function> = preamble_functions
    //         .iter()
    //         .map(|function| {
    //             translate_function(function_index, &function)
    //                 .map_err(|source| ProgramDefinitionError::FunctionDefinition { source })
    //         })
    //         .collect::<Result<Vec<Function>, ProgramDefinitionError>>()?;
    //     let entry = translate_match(&mut MultiBlockContext::new()function_index, match_)
    //         .map_err(|source| ProgramDefinitionError::PatternDefinition { source })?
    //         .finish();
    //
    //     Ok(Self { entry, functions })
    // }
    //
    // pub fn compile_functions<'index, 'functions>(
    //     function_index: &impl FunctionSignatureIndex,
    //     functions_to_compile: impl Iterator<Item = &'functions typeql::Function>,
    // ) -> Result<Vec<Function>, FunctionDefinitionError> {
    //     let ir: Result<Vec<Function>, FunctionDefinitionError> =
    //         functions_to_compile.map(|function| translate_function(function_index, &function)).collect();
    //     Ok(ir?)
    // }

    fn all_variables_categorised(context: &MultiBlockContext) -> bool {
        let mut variables = context.variables();
        variables.all(|var| context.get_variable_category(var).is_some())
    }
}

#[cfg(test)]
pub mod tests {
    use typeql::query::Pipeline;

    use crate::{
        pattern::constraint::Constraint,
        program::{
            block::MultiBlockContext,
            function_signature::{FunctionID, HashMapFunctionSignatureIndex},
            program::Program,
            ProgramDefinitionError,
        },
        translation::{function::translate_function, match_::translate_match},
        PatternDefinitionError,
    };

    #[test]
    fn from_typeql() {
        let raw_query = "
            with
            fun cat_names($c: animal) -> { name } :
                match
                    $c has cat-name $n;
                return { $n };

            match
                $x isa animal;
                $n in cat_names($x);
            select $x;
        ";
        let query = typeql::parse_query(raw_query).unwrap().into_pipeline();
        let Pipeline { stages, preambles, .. } = query;
        let typeql_match = stages.into_iter().map(|stage| stage.into_match()).find(|_| true).unwrap();
        let typeql_function = preambles.into_iter().map(|preamble| preamble.function).find(|_| true).unwrap();
        let function_id = FunctionID::Preamble(0);
        let mut function_context = MultiBlockContext::new();
        let should_be_unresolved_error =
            translate_match(&mut function_context, &HashMapFunctionSignatureIndex::empty(), &typeql_match);
        assert!(matches!(should_be_unresolved_error, Err(PatternDefinitionError::UnresolvedFunction { .. })));

        let function_index =
            HashMapFunctionSignatureIndex::build([(FunctionID::Preamble(0), &typeql_function)].into_iter());
        let function = translate_function(&function_index, &typeql_function).unwrap();
        let mut context = MultiBlockContext::new();
        let entry = translate_match(&mut context, &function_index, &typeql_match).unwrap().finish();
        let program = Program::new(context, entry, vec![function]);
        match &program.entry.conjunction().constraints()[2] {
            Constraint::FunctionCallBinding(call) => {
                assert_eq!(function_id, call.function_call().function_id())
            }
            _ => assert!(false),
        }
    }
}

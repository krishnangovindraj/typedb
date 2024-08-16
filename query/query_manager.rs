/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use compiler::match_::{inference::annotated_functions::IndexedAnnotatedFunctions, planner::pattern_plan::PatternPlan};
use concept::type_::type_manager::TypeManager;
use function::{function::Function, function_manager::FunctionManager};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::{stage::Stage as TypeQLStage, SchemaQuery};

use crate::{
    define,
    error::QueryError,
    translation::{translate_pipeline, TranslatedPipeline},
    type_inference::{infer_types_for_pipeline, AnnotatedPipeline},
};

pub struct QueryManager {}

impl QueryManager {
    // TODO: clean up if QueryManager remains stateless
    pub fn new() -> QueryManager {
        QueryManager {}
    }

    pub fn execute_schema(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        query: SchemaQuery,
    ) -> Result<(), QueryError> {
        match query {
            SchemaQuery::Define(define) => {
                define::execute(snapshot, &type_manager, define).map_err(|err| QueryError::Define { source: err })
            }
            SchemaQuery::Redefine(redefine) => {
                todo!()
            }
            SchemaQuery::Undefine(undefine) => {
                todo!()
            }
        }
    }

    pub fn execute_pipeline(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        schema_function_annotations: &IndexedAnnotatedFunctions,
        query: &typeql::query::Pipeline,
    ) -> Result<(), QueryError> {
        // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {

        // 1: Translate
        let TranslatedPipeline { translated_preamble, translated_stages, variable_registry } =
            translate_pipeline(snapshot, function_manager, query)?;
        // TODO: Do we optimise here or after type-inference?

        // 2: Annotate
        let AnnotatedPipeline { annotated_preamble, annotated_stages } = infer_types_for_pipeline(
            snapshot,
            type_manager,
            schema_function_annotations,
            &variable_registry,
            translated_preamble,
            translated_stages,
        )?;

        // compile_pipeline(annotated_preamble, annotated_stages);
        // // 3: Compile
        // for stage in annotated_stages {
        //     let compiled_stage = compile_stage(stage);
        // }

        todo!()
    }

    // TODO: take in parsed TypeQL clause
    fn create_executor(&self, clause: &str) {
        // match clause
    }

    fn create_match_executor(&self, query_functions: Vec<Function<usize>>) {
        // let conjunction = Conjunction::new();
        // ... build conjunction...
    }
}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use typeql::query::stage::Stage as TypeQLStage;

use concept::type_::type_manager::TypeManager;
use function::function::Function;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::{query::SchemaQuery, Query};
use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use concept::error::ConceptReadError;
use executor::batch::ImmutableRow;
use function::function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex};
use ir::program::function_signature::{FunctionID, HashMapFunctionSignatureIndex};
use ir::translation::function::translate_function;
use ir::translation::match_::translate_match;
use ir::translation::TranslationContext;
use lending_iterator::LendingIterator;

use crate::{define, error::QueryError};

pub struct QueryManager {}

impl QueryManager {
    // TODO: clean up if QueryManager remains stateless
    pub fn new() -> QueryManager {
        QueryManager {}
    }

    pub fn execute_pipeline(
        &self,
        snapshot: &mut impl WritableSnapshot,
        type_manager: &TypeManager,
        function_manager: &FunctionManager,
        schema_function_annotations: &IndexedAnnotatedFunctions,
        query: typeql::query::Pipeline,
    ) -> Result<(), QueryError> {
    // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {
        let preamble_signatures = HashMapFunctionSignatureIndex::build(
            query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function))
        );
        let all_function_signatures = ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);

        let preamble_functions = query.preambles.iter().map(|preamble|  {
            translate_function(
                &all_function_signatures,
                &preamble.function)
        }).collect::<Result<Vec<_>, _>>().map_err(|source| QueryError::FunctionDefinition { source })?;

        let mut translation_context = TranslationContext::new();
        let stages : Vec<Stage> = Vec::with_capacity(query.stages.len());
        for stage in &query.stages {
            match stage {
                // TypeQLStage::Match(match_) => {
                //     MatchClause::new(translate_match(&mut translation_context, &all_function_signatures, match_))
                // },
                // Stage::Insert(_) => {}
                // Stage::Delete(_) => {}
                _ => todo!()
                // Stage::Put(_) => {}
                // Stage::Update(_) => {}
                // Stage::Fetch(_) => {}
                // Stage::Reduce(_) => {}
                // Stage::Modifier(_) => {}
            }
        }
        todo!()
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

    // TODO: take in parsed TypeQL clause
    fn create_executor(&self, clause: &str) {
        // match clause
    }

    fn create_match_executor(&self, query_functions: Vec<Function<usize>>) {
        // let conjunction = Conjunction::new();
        // ... build conjunction...
    }
}

enum Stage {
    Match,
    Insert,
    Delete,
    Put,
    Fetch,
    Assert,
    Select,
    Sort,
    Offset,
    Limit,
}

trait PipelineStage {}

enum QueryReturn {
    MapStream,
    JSONStream,
    Aggregate,
}

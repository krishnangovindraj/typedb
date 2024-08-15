/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use compiler::match_::inference::{
    annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
    type_annotations::TypeAnnotations,
    type_inference::{
        infer_types, infer_types_for_functions, infer_types_for_match_block,
    },
};
use concept::{type_::type_manager::TypeManager};
use function::{
    function::Function,
    function_manager::{FunctionManager, ReadThroughFunctionSignatureIndex},
};
use ir::{
    pattern::constraint::{Constraint, Constraints},
    program::{
        block::{FunctionalBlock, VariableRegistry},
        function_signature::{FunctionID, FunctionSignatureIndex, HashMapFunctionSignatureIndex},
        modifier::{Filter, Limit, Modifier, Offset, Sort},
    },
    translation::{
        function::translate_function,
        match_::translate_match,
        writes::{translate_delete, translate_insert},
        TranslationContext,
    },
};
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::{stage::Stage as TypeQLStage, SchemaQuery};
use compiler::delete::delete::DeletePlan;
use compiler::insert::insert::{build_insert_plan, InsertPlan};
use compiler::match_::planner::pattern_plan::PatternPlan;
use executor::VariablePosition;

use crate::{define, error::QueryError};

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
        query: typeql::query::Pipeline,
    ) -> Result<(), QueryError> {
        // ) -> Result<impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>>, QueryError> {

        // 1: Translate
        let preamble_signatures = HashMapFunctionSignatureIndex::build(
            query.preambles.iter().enumerate().map(|(i, preamble)| (FunctionID::Preamble(i), &preamble.function)),
        );
        let all_function_signatures =
            ReadThroughFunctionSignatureIndex::new(snapshot, function_manager, preamble_signatures);
        let preamble_functions = query
            .preambles
            .iter()
            .map(|preamble| translate_function(&all_function_signatures, &preamble.function))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|source| QueryError::FunctionDefinition { source })?;

        let mut translation_context = TranslationContext::new();
        let mut translated_stages: Vec<TranslatedStage> = Vec::with_capacity(query.stages.len());
        for typeql_stage in &query.stages {
            let translated = translate_stage(&mut translation_context, &all_function_signatures, typeql_stage)?;
            translated_stages.push(translated);
        }
        // TODO: Do we optimise here or after type-inference?

        // 2: Annotate
        let preamble_function_annotations =
            infer_types_for_functions(preamble_functions, snapshot, type_manager, schema_function_annotations)
                .map_err(|source| QueryError::TypeInference { source })?;

        let mut running_variable_annotations: HashMap<Variable, Arc<HashSet<answer::Type>>> = HashMap::new();
        let mut annotated_stages = Vec::with_capacity(translated_stages.len());
        for stage in translated_stages {
            let annotated_stage = annotate_stage(
                &mut running_variable_annotations,
                &translation_context.variable_registry,
                snapshot,
                type_manager,
                schema_function_annotations,
                &preamble_function_annotations,
                stage,
            )?;
            annotated_stages.push(annotated_stage);
        }

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

enum TranslatedStage {
    Match { block: FunctionalBlock },
    Insert { block: FunctionalBlock },
    Delete { block: FunctionalBlock, deleted_variables: Vec<Variable> },

    // ...
    Filter(Filter),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
}

fn translate_stage(
    translation_context: &mut TranslationContext,
    all_function_signatures: &impl FunctionSignatureIndex,
    typeql_stage: &TypeQLStage,
) -> Result<TranslatedStage, QueryError> {
    match typeql_stage {
        TypeQLStage::Match(match_) => translate_match(translation_context, all_function_signatures, match_)
            .map(|builder| TranslatedStage::Match { block: builder.finish() }),
        TypeQLStage::Insert(insert) => {
            translate_insert(translation_context, insert).map(|block| TranslatedStage::Insert { block })
        }
        TypeQLStage::Delete(delete) => translate_delete(translation_context, delete)
            .map(|(block, deleted_variables)| TranslatedStage::Delete { block, deleted_variables }),
        _ => todo!(),
        // TypeQLStage::Put(_) => {}
        // TypeQLStage::Update(_) => {}
        // TypeQLStage::Fetch(_) => {}
        // TypeQLStage::Reduce(_) => {}
        // TypeQLStage::Modifier(modifier) => {
        //     translate_modifier(modifier).map(|modifier| TranslatedStage::Modifier(modifier))
        // }
    }
    .map_err(|source| QueryError::PatternDefinition { source })
}

enum AnnotatedStage {
    Match { block: FunctionalBlock, block_annotations: TypeAnnotations },
    Insert { block: FunctionalBlock, annotations: TypeAnnotations },
    Delete { block: FunctionalBlock, deleted_variables: Vec<Variable> },
    // ...
    Filter(Filter),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
}

fn annotate_stage(
    running_variable_annotations: &mut HashMap<Variable, Arc<HashSet<Type>>>,
    variable_registry: &VariableRegistry,
    snapshot: &mut impl WritableSnapshot,
    type_manager: &TypeManager,
    schema_function_annotations: &IndexedAnnotatedFunctions,
    preamble_function_annotations: &AnnotatedUnindexedFunctions,
    stage: TranslatedStage,
) -> Result<AnnotatedStage, QueryError> {
    match stage {
        TranslatedStage::Match { block } => {
            let block_annotations = infer_types_for_match_block(
                &block,
                &variable_registry,
                snapshot,
                &type_manager,
                &running_variable_annotations,
                schema_function_annotations,
                &preamble_function_annotations,
            )
            .map_err(|source| QueryError::TypeInference { source })?;
            block_annotations.variable_annotations().iter().for_each(|(k, v)| {
                running_variable_annotations.insert(k.clone(), v.clone());
            });
            Ok(AnnotatedStage::Match { block, block_annotations })
        }
        TranslatedStage::Insert { block } => {
            let (annotations, _) = infer_types(&block, vec![], snapshot, type_manager, &IndexedAnnotatedFunctions::empty(), &variable_registry)
                .map_err(|source| QueryError::TypeInference { source })?;
            validate_insertable();

            Ok(AnnotatedStage::Insert { block, annotations })
        }
        TranslatedStage::Delete { block, deleted_variables } => {
            deleted_variables.iter().for_each(|v| {
                running_variable_annotations.remove(v);
            });
            Ok(AnnotatedStage::Delete { block, deleted_variables })
        }
        _ => todo!(),
    }
}

enum CompiledStage {
    Match(PatternPlan),
    Insert(InsertPlan),
    Delete(DeletePlan),
}

fn compile_stage(input_variables: HashMap<Variable, VariablePosition>, annotated_stage: AnnotatedStage) -> Result<CompiledStage, QueryError> {
    match annotated_stage {
        AnnotatedStage::Match { .. } => todo!(),
        AnnotatedStage::Insert { block, explicit_labels } => {
            build_insert_plan(block, input_variables, )
        }
        AnnotatedStage::Delete { .. } => {}
        _ => todo!(),
        // AnnotatedStage::Filter(_) => {}
        // AnnotatedStage::Sort(_) => {}
        // AnnotatedStage::Offset(_) => {}
        // AnnotatedStage::Limit(_) => {}
    };
}

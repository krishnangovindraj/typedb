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
use compiler::{
    insert::validate::validate_insertable,
    match_::inference::{
        annotated_functions::{AnnotatedUnindexedFunctions, IndexedAnnotatedFunctions},
        type_annotations::{ConstraintTypeAnnotations, TypeAnnotations},
        type_inference::{infer_types_for_functions, infer_types_for_match_block},
    },
};
use concept::type_::type_manager::TypeManager;
use ir::{
    pattern::constraint::Constraint,
    program::{
        block::{FunctionalBlock, VariableRegistry},
        function::Function,
        modifier::{Filter, Limit, Offset, Sort},
    },
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{error::QueryError, translation::TranslatedStage};

pub(super) struct AnnotatedPipeline {
    pub(super) annotated_preamble: AnnotatedUnindexedFunctions,
    pub(super) annotated_stages: Vec<AnnotatedStage>,
}

pub(super) enum AnnotatedStage {
    Match { block: FunctionalBlock, block_annotations: TypeAnnotations },
    Insert { block: FunctionalBlock, annotations: TypeAnnotations },
    Delete { block: FunctionalBlock, deleted_variables: Vec<Variable>, annotations: TypeAnnotations },
    // ...
    Filter(Filter),
    Sort(Sort),
    Offset(Offset),
    Limit(Limit),
}

pub(super) fn infer_types_for_pipeline(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_annotations: &IndexedAnnotatedFunctions,
    variable_registry: &VariableRegistry,
    translated_preamble: Vec<Function>,
    translated_stages: Vec<TranslatedStage>,
) -> Result<AnnotatedPipeline, QueryError> {
    let annotated_preamble =
        infer_types_for_functions(translated_preamble, snapshot, type_manager, schema_function_annotations)
            .map_err(|source| QueryError::TypeInference { source })?;

    let mut running_variable_annotations: HashMap<Variable, Arc<HashSet<answer::Type>>> = HashMap::new();
    let mut annotated_stages = Vec::with_capacity(translated_stages.len());

    let empty_constraint_annotations = HashMap::new();
    let mut latest_match_index = None;
    for stage in translated_stages {
        let running_constraint_annotations = latest_match_index
            .map(|idx| {
                let AnnotatedStage::Match { block_annotations, .. } = annotated_stages.get(idx).unwrap() else {
                    unreachable!();
                };
                block_annotations.constraint_annotations()
            })
            .unwrap_or(&empty_constraint_annotations);
        let annotated_stage = annotate_stage(
            &mut running_variable_annotations,
            &variable_registry,
            snapshot,
            type_manager,
            schema_function_annotations,
            &annotated_preamble,
            &running_constraint_annotations,
            stage,
        )?;
        if let AnnotatedStage::Match { .. } = annotated_stage {
            latest_match_index = Some(annotated_stages.len());
        }
        annotated_stages.push(annotated_stage);
    }
    Ok(AnnotatedPipeline { annotated_stages, annotated_preamble })
}

fn annotate_stage(
    running_variable_annotations: &mut HashMap<Variable, Arc<HashSet<Type>>>,
    variable_registry: &VariableRegistry,
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    schema_function_annotations: &IndexedAnnotatedFunctions,
    preamble_function_annotations: &AnnotatedUnindexedFunctions,
    running_constraint_annotations: &HashMap<Constraint<Variable>, ConstraintTypeAnnotations>,
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
            let insert_annotations = infer_types_for_match_block(
                &block,
                &variable_registry,
                snapshot,
                type_manager,
                running_variable_annotations,
                &IndexedAnnotatedFunctions::empty(),
                &AnnotatedUnindexedFunctions::empty(),
            )
            .map_err(|source| QueryError::TypeInference { source })?;

            validate_insertable(
                &block,
                running_variable_annotations,
                running_constraint_annotations,
                &insert_annotations,
            )
            .map_err(|source| QueryError::TypeInference { source })?;
            Ok(AnnotatedStage::Insert { block, annotations: insert_annotations })
        }
        TranslatedStage::Delete { block, deleted_variables } => {
            let delete_annotations = infer_types_for_match_block(
                &block,
                &variable_registry,
                snapshot,
                type_manager,
                running_variable_annotations,
                &IndexedAnnotatedFunctions::empty(),
                &AnnotatedUnindexedFunctions::empty(),
            )
            .map_err(|source| QueryError::TypeInference { source })?;
            // TODO: Do we want to validate delete sanity?
            deleted_variables.iter().for_each(|v| {
                running_variable_annotations.remove(v);
            });
            Ok(AnnotatedStage::Delete { block, deleted_variables, annotations: delete_annotations })
        }
        _ => todo!(),
    }
}

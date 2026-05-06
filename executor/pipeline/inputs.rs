/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{marker::PhantomData, sync::Arc};

use answer::variable_value::VariableValue;
use compiler::{annotation::function::FunctionParameterAnnotation, executable::pipeline::InputsExecutable};
use concept::{ConceptStatus, error::ConceptReadError, thing::ThingAPI};
use encoding::value::ValueEncodable;
use error::needs_update_when_feature_is_implemented;
use ir::pattern::variable_category::VariableOptionality;
use lending_iterator::LendingIterator;
use resource::profile::{StageProfile, StepProfile, StorageCounters};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    ExecutionInterrupt,
    batch::{Batch, BatchRowIterator},
    pipeline::{
        PipelineExecutionError, StageIterator,
        initial::InitialIterator,
        stage::{ExecutionContext, StageAPI},
    },
    row::MaybeOwnedRow,
};

pub struct InputStageExecutor<InputIterator> {
    executable: Arc<InputsExecutable>,
    _input_iterator: PhantomData<InputIterator>,
}

impl<InputIterator> InputStageExecutor<InputIterator> {
    pub fn new(executable: Arc<InputsExecutable>) -> Self {
        Self { executable, _input_iterator: PhantomData }
    }
}

impl<InputIterator, Snapshot> StageAPI<Snapshot> for InputStageExecutor<InputIterator>
where
    InputIterator: StageIterator,
    Snapshot: ReadableSnapshot + 'static,
{
    type InputIterator = InputIterator;
    type OutputIterator = InputStageIterator<Snapshot, InputIterator>;

    fn into_iterator(
        self,
        input_iterator: Self::InputIterator,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Result<
        (Self::OutputIterator, ExecutionContext<Snapshot>),
        (Box<PipelineExecutionError>, ExecutionContext<Snapshot>),
    > {
        Ok((InputStageIterator::new(context.clone(), self.executable, input_iterator), context))
    }
}

pub struct InputStageIterator<Snapshot: ReadableSnapshot + 'static, InputIterator: StageIterator> {
    context: ExecutionContext<Snapshot>,
    executable: Arc<InputsExecutable>,
    source_iterator: InputIterator,
    input_row_counter: usize,
    profile: Arc<StepProfile>,
}

impl<Snapshot: ReadableSnapshot + 'static, InputIterator: StageIterator> InputStageIterator<Snapshot, InputIterator> {
    pub(crate) fn new(
        context: ExecutionContext<Snapshot>,
        executable: Arc<InputsExecutable>,
        source_iterator: InputIterator,
    ) -> Self {
        let stage_profile = context.profile.profile_stage(|| "Inputs stage".to_owned(), executable.executable_id);
        let profile = stage_profile.extend_or_get(0, || "Inputs validation".to_owned());
        Self { context, executable, source_iterator, input_row_counter: 0, profile }
    }
}

impl<Snapshot, InputIterator> LendingIterator for InputStageIterator<Snapshot, InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{
    type Item<'a> = Result<MaybeOwnedRow<'a>, Box<PipelineExecutionError>>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let expected_types = self.executable.expected_types();
        let optionality = self.executable.optionality();
        let row_index = self.input_row_counter;
        self.input_row_counter += 1;
        Some(self.source_iterator.next()?.and_then(|row| {
            debug_assert!(row.row().len() == expected_types.len());
            row.iter().enumerate().try_for_each(|(column_index, entry)| {
                if !row_entry_satisfies_types(&expected_types[column_index], optionality[column_index], entry) {
                    Err(Box::new(PipelineExecutionError::InputDidNotSatisfyDeclaredType { row_index, column_index }))
                } else if !row_entry_exists(&self.context, self.profile.storage_counters(), entry)? {
                    Err(Box::new(PipelineExecutionError::InputConceptDoesNotExist { row_index, column_index }))
                } else {
                    Ok(())
                }
            })?;
            Ok(row)
        }))
    }
}

impl<Snapshot, InputIterator> StageIterator for InputStageIterator<Snapshot, InputIterator>
where
    Snapshot: ReadableSnapshot + 'static,
    InputIterator: StageIterator,
{}

fn row_entry_satisfies_types(
    expected_type: &FunctionParameterAnnotation,
    optionality: VariableOptionality,
    entry: &VariableValue<'_>,
) -> bool {
    match (entry, expected_type) {
        (VariableValue::Value(value), FunctionParameterAnnotation::Value(value_type)) => {
            *value_type == value.value_type()
        }
        (VariableValue::Thing(thing), FunctionParameterAnnotation::Concept(types)) => types.contains(&thing.type_()),
        (VariableValue::None, _) => optionality == VariableOptionality::Optional,
        (_, FunctionParameterAnnotation::AnyConcept) => unreachable!("Unexpected"),
        (VariableValue::Value(_), _)
        | (VariableValue::Thing(_), _)
        | (VariableValue::ValueList(_), _)
        | (VariableValue::ThingList(_), _)
        | (VariableValue::Type(_), _) => false,
    }
}

fn row_entry_exists(
    context: &ExecutionContext<impl ReadableSnapshot>,
    storage_counters: StorageCounters,
    entry: &VariableValue<'_>,
) -> Result<bool, Box<PipelineExecutionError>> {
    match entry {
        VariableValue::Thing(thing) => match thing {
            answer::Thing::Entity(entity) => {
                context.thing_manager.instance_exists(&*context.snapshot, entity, storage_counters)
            }
            answer::Thing::Relation(relation) => {
                context.thing_manager.instance_exists(&*context.snapshot, relation, storage_counters)
            }
            answer::Thing::Attribute(attribute) => {
                context.thing_manager.instance_exists(&*context.snapshot, attribute, storage_counters)
            }
        }
        .map_err(|typedb_source| Box::new(PipelineExecutionError::ConceptRead { typedb_source })),
        VariableValue::ThingList(_) => {
            needs_update_when_feature_is_implemented!(Lists);
            unreachable!("Unimplemented feature: Lists");
        }
        VariableValue::Value(_) | VariableValue::ValueList(_) | VariableValue::None => Ok(true),
        VariableValue::Type(_) => unreachable!("Types in input"),
    }
}

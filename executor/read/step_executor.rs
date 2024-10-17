/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;
use std::sync::Arc;

use compiler::{
    executable::{
        function::{ExecutableFunction, ExecutableReturn},
        match_::planner::{
            function_plan::ExecutableFunctionRegistry,
            match_executable::{ExecutionStep, MatchExecutable},
        },
        pipeline::ExecutableStage,
    },
    VariablePosition,
};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use ir::pipeline::function_signature::FunctionID;
use storage::snapshot::ReadableSnapshot;

use crate::read::{
    collecting_stage_executor::CollectingStageExecutor, immediate_executor::ImmediateExecutor,
    nested_pattern_executor::NestedPatternExecutor, pattern_executor::PatternExecutor,
};

pub(super) enum StepExecutors {
    Immediate(ImmediateExecutor),
    NestedPattern(NestedPatternExecutor),
    CollectingStage(CollectingStageExecutor),
    ReshapeForReturn(Vec<VariablePosition>),
}

impl StepExecutors {
    pub(crate) fn unwrap_immediate(&mut self) -> &mut ImmediateExecutor {
        match self {
            StepExecutors::Immediate(step) => step,
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap_nested_pattern_branch(&mut self) -> &mut NestedPatternExecutor {
        match self {
            StepExecutors::NestedPattern(step) => step,
            _ => unreachable!(),
        }
    }

    pub(crate) fn unwrap_collecting_stage(&mut self) -> &mut CollectingStageExecutor {
        match self {
            StepExecutors::CollectingStage(step) => step,
            _ => unreachable!(),
        }
    }
}

pub(super) fn create_executors_for_match(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    match_executable: &MatchExecutable,
    tmp__recursion_validation: &mut HashSet<FunctionID>,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    let mut steps = Vec::with_capacity(match_executable.steps().len());
    for step in match_executable.steps() {
        let step = match step {
            ExecutionStep::Intersection(_) => {
                StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::UnsortedJoin(_) => {
                StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Assignment(_) => {
                StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Check(_) => StepExecutors::Immediate(ImmediateExecutor::new(step, snapshot, thing_manager)?),
            ExecutionStep::Negation(negation_step) => StepExecutors::NestedPattern(
                // TODO: I'd like to refactor the immediate branches to this pattern too.
                NestedPatternExecutor::new_negation(negation_step, snapshot, thing_manager, function_registry)?,
            ),
            ExecutionStep::FunctionCall(function_call) => {
                let function = function_registry.get(function_call.function_id.clone());
                if function.is_tabled {
                    todo!()
                } else {
                    if tmp__recursion_validation.contains(&function_call.function_id) {
                        todo!("Recursive functions are unsupported in this release. Continuing would overflow the stack")
                    } else {
                        tmp__recursion_validation.insert(function_call.function_id.clone());
                    }
                    let to_return = StepExecutors::NestedPattern(NestedPatternExecutor::new_inline_function(
                        function_call,
                        snapshot,
                        thing_manager,
                        function_registry,
                        tmp__recursion_validation
                    )?);
                    tmp__recursion_validation.remove(&function_call.function_id);
                    to_return
                }
            }
            _ => todo!(),
        };
        steps.push(step);
    }
    Ok(steps)
}

pub(super) fn create_executors_for_function(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_function: &ExecutableFunction,
    tmp__recursion_validation: &mut HashSet<FunctionID>
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    // TODO: Support full pipelines
    debug_assert!(executable_function.executable_stages.len() == 1);
    let executable_stages = &executable_function.executable_stages;
    let mut steps = create_executors_for_pipeline_stages(
        snapshot,
        thing_manager,
        function_registry,
        executable_stages,
        executable_stages.len() - 1,
        tmp__recursion_validation,
    )?;

    // TODO: Add table writing step.
    match &executable_function.returns {
        ExecutableReturn::Stream(positions) => {
            steps.push(StepExecutors::ReshapeForReturn(positions.clone()));
        }
        _ => todo!(),
    }
    Ok(steps)
}

pub(super) fn create_executors_for_pipeline_stages(
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
    function_registry: &ExecutableFunctionRegistry,
    executable_stages: &Vec<ExecutableStage>,
    at_index: usize,
    tmp__recursion_validation: &mut HashSet<FunctionID>,
) -> Result<Vec<StepExecutors>, ConceptReadError> {
    let mut previous_stage_steps = if at_index > 0 {
        create_executors_for_pipeline_stages(
            snapshot,
            thing_manager,
            function_registry,
            executable_stages,
            at_index - 1,
            tmp__recursion_validation,
        )?
    } else {
        vec![]
    };

    match &executable_stages[at_index] {
        ExecutableStage::Match(match_executable) => {
            let mut match_stages =
                create_executors_for_match(snapshot, thing_manager, function_registry, match_executable, tmp__recursion_validation)?;
            previous_stage_steps.append(&mut match_stages);
            Ok(previous_stage_steps)
        }
        ExecutableStage::Select(_) => todo!(),
        ExecutableStage::Offset(offset_executable) => {
            let step =
                NestedPatternExecutor::new_offset(PatternExecutor::new(previous_stage_steps), offset_executable)?;
            Ok(vec![StepExecutors::NestedPattern(step)])
        }
        ExecutableStage::Limit(limit_executable) => {
            let step = NestedPatternExecutor::new_limit(PatternExecutor::new(previous_stage_steps), limit_executable)?;
            Ok(vec![StepExecutors::NestedPattern(step)])
        }
        ExecutableStage::Require(_) => todo!(),
        ExecutableStage::Sort(sort_executable) => {
            let step = CollectingStageExecutor::new_sort(PatternExecutor::new(previous_stage_steps), sort_executable);
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
        ExecutableStage::Reduce(reduce_executable) => {
            let step =
                CollectingStageExecutor::new_reduce(PatternExecutor::new(previous_stage_steps), reduce_executable);
            Ok(vec![StepExecutors::CollectingStage(step)])
        }
        ExecutableStage::Insert(_) | ExecutableStage::Delete(_) => todo!("Or unreachable?"),
    }
}

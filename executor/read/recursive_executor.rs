/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::match_::planner::match_executable::{ExecutionStep, MatchExecutable};
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    read::{step_executors::StepExecutor, subpattern_executor::SubPatternExecutor},
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub(super) enum StackInstruction {
    Executable(StepExecutor),
    SubPattern(SubPatternExecutor),

    // Internal markers which make the executor code simpler
    PatternStart,
    Yield,
}

fn jit(
    match_executable: &MatchExecutable,
    snapshot: &Arc<impl ReadableSnapshot + 'static>,
    thing_manager: &Arc<ThingManager>,
) -> Result<Vec<StackInstruction>, ConceptReadError> {
    let mut steps = Vec::with_capacity(2 + match_executable.steps().len());
    steps.push(StackInstruction::PatternStart);
    for step in match_executable.steps() {
        let step = match step {
            ExecutionStep::Intersection(_) => {
                StackInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::UnsortedJoin(_) => {
                StackInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Assignment(_) => {
                StackInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Check(_) => StackInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?),

            // SubPatterns
            ExecutionStep::Negation(_) => {
                StackInstruction::Executable(StepExecutor::new(step, snapshot, thing_manager)?)
            }
            ExecutionStep::Optional(_) => todo!(),
            ExecutionStep::Disjunction(_) => todo!(),
        };
        steps.push(step)
    }
    steps.push(StackInstruction::Yield);

    Ok(steps)
}

// PatternStack
pub struct StackFrame {
    steps: Vec<StackInstruction>,
    return_index: usize,
}

pub(crate) struct RecursiveQueryExecutor {
    // tabled_subquery_states: (), // Future: Map<SubqueryID, TabledSubqueryState>
    stack: Vec<StackFrame>,
    input: Option<MaybeOwnedRow<'static>>, // TakeOnce
}

impl RecursiveQueryExecutor {
    pub(crate) fn new(
        match_executable: &MatchExecutable,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        input: MaybeOwnedRow<'_>,
    ) -> Result<Self, ConceptReadError> {
        let entry_steps = jit(match_executable, snapshot, thing_manager)?;
        let base_frame = StackFrame { steps: entry_steps, return_index: 0 };
        Ok(Self { stack: vec![base_frame], input: Some(input.into_owned()) })
    }

    pub(crate) fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let (mut current_step, mut last_step_batch) = match self.input.take() {
            Some(row) => (0, Some(FixedBatch::from(row))),
            None => (self.stack.last().unwrap().steps.len() - 1, None),
        };

        loop {
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            let next_batch = match &mut self.instruction_at(current_step) {
                StackInstruction::Executable(executor) => {
                    if last_step_batch.is_some() {
                        executor.batch_from(last_step_batch.take().unwrap(), context, interrupt)?
                    } else {
                        executor.batch_continue(context)?
                    }
                }
                StackInstruction::PatternStart => {
                    // TODO: This is executed for the entry only. Not subpatterns
                    if last_step_batch.is_some() {
                        last_step_batch
                    } else {
                        return Ok(None);
                    }
                }
                StackInstruction::Yield => {
                    // TODO: This is executed for the entry only. Not subpatterns
                    if last_step_batch.is_some() {
                        return Ok(last_step_batch);
                    } else {
                        None
                    }
                }
                _ => todo!(),
            };
            current_step = if next_batch.is_some() { current_step + 1 } else { current_step - 1 };
            last_step_batch = next_batch;
        }
    }

    fn instruction_at(&mut self, step_index: usize) -> &mut StackInstruction {
        self.stack.last_mut().unwrap().steps.get_mut(step_index).unwrap()
    }
}

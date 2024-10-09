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
    read::{
        step_executors::StepExecutor,
        subpattern_executor::{EntryStep, SubPatternStep, SubPatternTrait},
    },
    row::MaybeOwnedRow,
    ExecutionInterrupt,
};

pub(super) enum StackInstruction {
    Executable(StepExecutor),
    SubPattern(SubPatternStep),

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
        let entry_instruction = StackInstruction::SubPattern(SubPatternStep::Entry(EntryStep::new(entry_steps)));
        let base_frame = StackFrame { steps: vec![entry_instruction], return_index: 0 };
        Ok(Self { stack: vec![base_frame], input: Some(input.into_owned()) })
    }

    pub(crate) fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let StackInstruction::SubPattern(SubPatternStep::Entry(entry)) = self.instruction_at(0) else { unreachable!() };
        let entry_pattern = entry.take_pattern().unwrap();
        self.push_into_subpattern(entry_pattern, 0);

        let (mut current_step, mut last_step_batch) = match self.input.take() {
            Some(row) => (0, Some(FixedBatch::from(row))),
            None => (self.stack.last().unwrap().steps.len() - 1, None),
        };

        while self.stack.len() > 1 {
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }
            let (next_step, next_batch) = match last_step_batch {
                Some(batch) => self.execute_forward(context, interrupt, current_step, batch)?,
                None => self.execute_backward(context, interrupt, current_step)?,
            };
            (current_step, last_step_batch) = (next_step, next_batch);
        }
        Ok(last_step_batch)
    }

    fn execute_forward(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        current_step: usize,
        input_batch: FixedBatch,
    ) -> Result<(usize, Option<FixedBatch>), ReadExecutionError> {
        match &mut self.instruction_at(current_step) {
            StackInstruction::Executable(executor) => {
                executor.prepare(input_batch, context)?;
                match executor.batch_continue(context, interrupt)? {
                    Some(batch) => Ok((current_step + 1, Some(batch))),
                    None => Ok((current_step - 1, None)),
                }
            }
            StackInstruction::PatternStart => Ok((current_step + 1, Some(input_batch))),
            StackInstruction::Yield => self.pop_to_subpattern_step(Some(input_batch)),
            _ => todo!(),
        }
    }

    fn execute_backward(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
        current_step: usize,
    ) -> Result<(usize, Option<FixedBatch>), ReadExecutionError> {
        match &mut self.instruction_at(current_step) {
            StackInstruction::Executable(executor) => match executor.batch_continue(context, interrupt)? {
                Some(batch) => Ok((current_step + 1, Some(batch))),
                None => Ok((current_step - 1, None)),
            },
            StackInstruction::PatternStart => self.pop_to_subpattern_step(None),
            StackInstruction::Yield => Ok((current_step - 1, None)),
            _ => todo!(),
        }
    }

    fn instruction_at(&mut self, step_index: usize) -> &mut StackInstruction {
        self.stack.last_mut().unwrap().steps.get_mut(step_index).unwrap()
    }

    fn pop_to_subpattern_step(
        &mut self,
        subpattern_return: Option<FixedBatch>,
    ) -> Result<(usize, Option<FixedBatch>), ReadExecutionError> {
        let StackFrame { steps, return_index } = self.stack.pop().unwrap();
        let StackInstruction::SubPattern(subpattern) = self.instruction_at(return_index) else {
            todo!("Could still be tabledCall");
        };
        let (ptr_update, next_batch) = match subpattern_return {
            None => subpattern.record_pattern_exhausted(steps),
            Some(batch) => subpattern.record_pattern_result(steps, batch),
        };
        Ok((ptr_update.apply_to(return_index), next_batch))
    }

    fn push_into_subpattern(&mut self, subpattern: Vec<StackInstruction>, subpattern_index: usize) {
        let frame = StackFrame { steps: subpattern, return_index: subpattern_index };
        self.stack.push(frame);
    }
}

pub(super) enum InstructionPointerUpdate {
    Forward,
    Remain,
    Backward,
}

impl InstructionPointerUpdate {
    fn apply_to(self, index: usize) -> usize {
        match self {
            InstructionPointerUpdate::Forward => index + 1,
            InstructionPointerUpdate::Remain => index,
            InstructionPointerUpdate::Backward => index - 1,
        }
    }
}

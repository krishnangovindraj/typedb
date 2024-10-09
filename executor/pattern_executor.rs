/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use compiler::executable::match_::planner::match_executable::MatchExecutable;
use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use itertools::Itertools;
use lending_iterator::adaptors::FlatMap;
use lending_iterator::{AsLendingIterator, LendingIterator};
use storage::snapshot::ReadableSnapshot;

use crate::{
    batch::FixedBatch,
    error::ReadExecutionError,
    pipeline::stage::ExecutionContext,
    row::MaybeOwnedRow,
    read::step_executors::StepExecutor,
    ExecutionInterrupt,
};
use crate::batch::FixedBatchRowIterator;
use crate::read::{EntryStep, prepare_executors, Step};

pub struct MatchExecutor {
    input: Option<MaybeOwnedRow<'static>>,
    step_executors: Vec<Step>,
}

impl MatchExecutor {
    pub fn new(
        match_executable: &MatchExecutable,
        snapshot: &Arc<impl ReadableSnapshot + 'static>,
        thing_manager: &Arc<ThingManager>,
        input: MaybeOwnedRow<'_>,
    ) -> Result<Self, ConceptReadError> {
        let step_executors = prepare_executors(match_executable, snapshot, thing_manager)?;
        Ok(Self {
            input: Some(input.into_owned()),
            step_executors,
            // modifiers:
        })
    }

    pub fn into_iterator<Snapshot: ReadableSnapshot + 'static>(
        self,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> PatternIterator<Snapshot> {
        PatternIterator::new(
            AsLendingIterator::new(BatchIterator::new(self, context, interrupt)).flat_map(FixedBatchRowIterator::new),
        )
    }

    fn compute_next_batch(
        &mut self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        interrupt: &mut ExecutionInterrupt,
    ) -> Result<Option<FixedBatch>, ReadExecutionError> {
        let steps_len = self.step_executors.len();

        let (mut current_step, mut last_step_batch) = if let Some(input) = self.input.take() {
            (0, Some(FixedBatch::from(input)))
        } else {
            (steps_len - 1, None)
        };

        loop {
            debug_assert!(current_step >= 0 && current_step <= steps_len - 1);
            // TODO: inject interrupt into Checkers that could filter out many rows without ending as well.
            if let Some(interrupt) = interrupt.check() {
                return Err(ReadExecutionError::Interrupted { interrupt });
            }

            let next_batch = match &mut self.step_executors[current_step] {
                Step::Fundamental(executor) => {
                    if last_step_batch.is_some() {
                        executor.batch_from(last_step_batch.take().unwrap(), context, interrupt)?
                    } else {
                        executor.batch_continue(context)?
                    }
                }
                _ => todo!()
            };

            if let Some(batch) = next_batch {
                if current_step == steps_len - 1 {
                    return Ok(Some(batch));
                } else {
                    current_step += 1;
                    last_step_batch = Some(batch);
                }
            } else {
                if current_step == 0 {
                    return Ok(None);
                } else {
                    current_step -= 1;
                    last_step_batch = None;
                }
            }
        }
    }
}

pub(crate) struct BatchIterator<Snapshot> {
    executor: MatchExecutor,
    context: ExecutionContext<Snapshot>,
    interrupt: ExecutionInterrupt,
}

impl<Snapshot> BatchIterator<Snapshot> {
    pub(crate) fn new(
        executor: MatchExecutor,
        context: ExecutionContext<Snapshot>,
        interrupt: ExecutionInterrupt,
    ) -> Self {
        Self { executor, context, interrupt }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> Iterator for BatchIterator<Snapshot> {
    type Item = Result<FixedBatch, ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.executor.compute_next_batch(&self.context, &mut self.interrupt);
        batch.transpose()
    }
}

// Wrappers around
type PatternRowIterator<Snapshot> = FlatMap<
    AsLendingIterator<BatchIterator<Snapshot>>,
    FixedBatchRowIterator,
    fn(Result<FixedBatch, ReadExecutionError>) -> FixedBatchRowIterator,
>;

pub struct PatternIterator<Snapshot: ReadableSnapshot + 'static> {
    iterator: PatternRowIterator<Snapshot>,
}

impl<Snapshot: ReadableSnapshot> PatternIterator<Snapshot> {
    fn new(iterator: PatternRowIterator<Snapshot>) -> Self {
        Self { iterator }
    }
}

impl<Snapshot: ReadableSnapshot + 'static> LendingIterator for PatternIterator<Snapshot> {
    type Item<'a> = Result<MaybeOwnedRow<'a>, &'a ReadExecutionError>;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        self.iterator.next()
    }
}

//
// // PatternStack
//
// pub struct StackFrame {
//     steps: Vec<Step>,
//     return_index: usize,
// }
//
// pub struct PatternStack {
//     inner: Vec<StackFrame>,
// }
//
// impl PatternStack {
//     fn new(entry: EntryStep) -> Self {
//         let base_frame = StackFrame { steps: vec![Step::Entry(entry)] , return_index: 0 };
//         Self { inner: vec![base_frame] }
//     }
//
//     fn is_empty(&self) -> bool {
//         self.inner.len() > 1 // Does not consider the base-frame
//     }
//
//     fn push_frame(&mut self, steps: Vec<Step>, return_index: usize) {
//         self.inner.push(StackFrame { steps, return_index })
//     }
//
//     fn pop_frame(&mut self) -> StackFrame {
//         self.inner.pop().unwrap()
//     }
//
//     fn step_at(&mut self, step_index: i64) -> &mut Step {
//         let steps = &mut self.inner.last_mut().unwrap().steps;
//         if step_index < 0 {
//             self.step_pop_failure.as_mut().unwrap()
//         } else if step_index as usize >= steps.len() {
//             self.step_yield.as_mut().unwrap()
//         } else {
//             steps.get_mut(step_index as usize).unwrap()
//         }
//     }
// }

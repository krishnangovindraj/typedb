/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use answer::variable_value::VariableValue;
use concept::thing::thing_manager::ThingManager;
use lending_iterator::LendingIterator;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::ImmutableRow,
    pipeline::{PipelineContext, PipelineError, PipelineStage},
};

// TODO: Optimise for allocations
pub(crate) struct AccumulatingStage<Snapshot: ReadableSnapshot + 'static, Executor: AccumulatingStageAPI> {
    upstream: Box<PipelineStage<Snapshot>>, // Can't do lending iterator because it has a generic associated type
    rows: Vec<(Box<[VariableValue<'static>]>, u64)>,
    executor: Executor,
}

impl<Snapshot: ReadableSnapshot, Executor: AccumulatingStageAPI> AccumulatingStage<Snapshot, Executor> {
    fn accumulate(&mut self) -> Result<(), PipelineError> {
        let Self { executor, rows, upstream } = self;
        while let Some(result) = upstream.next() {
            match result {
                Err(err) => return Err(err),
                Ok(row) => {
                    Self::accept_incoming_row(rows, executor, row);
                }
            }
        }
        Ok(())
    }

    fn accept_incoming_row(
        rows: &mut Vec<(Box<[VariableValue<'static>]>, u64)>,
        executor: &mut Executor,
        incoming: ImmutableRow<'_>,
    ) {
        let (output_multiplicity, output_row_count) = if executor.must_deduplicate_incoming_rows() {
            (1, incoming.get_multiplicity())
        } else {
            (incoming.get_multiplicity(), 1)
        };
        for _ in 0..output_row_count {
            let mut stored_row =
                (0..executor.row_width()).map(|_| VariableValue::Empty).collect::<Vec<_>>().into_boxed_slice();
            executor.store_incoming_row_into(&incoming, &mut stored_row);
            rows.push((stored_row, output_multiplicity));
        }
    }

    pub fn accumulate_process_and_iterate(mut self) -> Result<AccumulatedRowIterator<Snapshot>, PipelineError> {
        self.accumulate()?;
        let Self { executor, rows, upstream } = self;
        let context = upstream.finalise();
        let (snapshot, thing_manager) = context.borrow_parts();
        let mut rows = rows.into_boxed_slice();
        executor.process_accumulated(snapshot, thing_manager, &mut rows)?;
        Ok(AccumulatedRowIterator { context, rows, next_index: 0 })
    }
}

pub(crate) trait AccumulatingStageAPI: 'static {
    fn process_accumulated(
        &self,
        snapshot: &impl ReadableSnapshot,
        thing_manager: &ThingManager,
        row: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>,
    ) -> Result<(), PipelineError>;
    fn store_incoming_row_into(&self, incoming: &ImmutableRow<'_>, stored_row: &mut Box<[VariableValue<'static>]>);
    fn must_deduplicate_incoming_rows(&self) -> bool;
    fn row_width(&self) -> usize;
}

pub struct AccumulatedRowIterator<Snapshot: ReadableSnapshot + 'static> {
    context: PipelineContext<Snapshot>,
    rows: Box<[(Box<[VariableValue<'static>]>, u64)]>,
    next_index: usize,
}

//
// // TODO: Implement LendingIterator instead ?
// impl<Snapshot, Executor: AccumulatingStageAPI> Iterator for AccumulatedRowIterator<Snapshot, Executor> {
//     // type Item<'a> = Result<ImmutableRow<'a>, Executor::Error>;
//     type Item = Result<ImmutableRow<'static>, Executor::Error>;
//     fn next(&mut self) -> Option<Self::Item> {
//         if AccumulatingStageState::Initial == self.state {
//             self.accumulator.accumulate();
//             let mut (snapshot, thing_manager) = upstream.into_parts();
//             match self.accumulator.executor.accumulate_and_process(&mut snapshot, self.accumulator.rows.as_mut_slice()) {
//                 Err(err) => {
//                     return Some(Err(err));
//                 },
//                 Ok(()) => {
//                     self.state = AccumulatingStageState::Streaming(0);
//                 }
//             }
//         }
//         match self.state {
//             AccumulatingStageState::Initial => {}
//             AccumulatingStageState::Streaming(next_index) => {
//                 self.state = AccumulatingStageState::Streaming(next_index + 1);
//                 Some(Ok(&self.rows.get(next_index)))
//             }
//         }
//         let AccumulatingStageState::Streaming(next_index) = self.state;
//     }
// }

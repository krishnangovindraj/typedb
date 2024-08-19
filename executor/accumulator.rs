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

use crate::batch::{ImmutableRow};

#[derive(Debug)]
enum AccumulatingStageState {
    Initial,
    Streaming(usize),
}

// TODO: Optimise for allocations
pub(crate) struct AccumulatingStage<Executor: AccumulatingStageAPI> {
    upstream: Box<dyn Iterator<Item=ImmutableRow<'static>>>, // Can't do lending iterator because it has a generic associated type
    rows: Vec<(Box<[VariableValue<'static>]>, u64)>,
    executor: Executor,
}

impl<Executor: AccumulatingStageAPI> AccumulatingStage<Executor> {
    pub fn accumulate(self) -> Box<[(Box<[VariableValue<'static>]>, u64)]> {
        for row in self.upstream {

        }
        self.rows.into_boxed_slice()
    }

    pub(crate) fn accept_incoming_row(&mut self, incoming: &ImmutableRow<'_>) {
        let (output_multiplicity, output_row_count) = if self.executor.must_deduplicate_incoming_rows() {
            (1, incoming.get_multiplicity())
        } else {
            (incoming.get_multiplicity(), 1)
        };
        for _ in 0..output_row_count {
            let mut stored_row = (0..self.executor.row_width())
                .map(|_| VariableValue::Empty).collect::<Vec<_>>().into_boxed_slice();
            self.executor.store_incoming_row_into(incoming, &mut stored_row);
            self.rows.push((stored_row, output_multiplicity));
        }
    }
}



pub(crate) trait AccumulatingStageAPI : 'static {
    type Error;
    fn process_accumulated(&self, snapshot: &impl ReadableSnapshot, thing_manager: &ThingManager, row: &mut Box<[(Box<[VariableValue<'static>]>, u64)]>) -> Result<(), Self::Error>;
    fn store_incoming_row_into(&self, incoming: &ImmutableRow<'_>, stored_row: &mut Box<[VariableValue<'static>]>);
    fn must_deduplicate_incoming_rows(&self) -> bool;
    fn row_width(&self) -> usize;
}

// pub struct AccumulatedRowIterator<Snapshot, Executor: AccumulatingStageAPI> {
//     snapshot: Snapshot,
//     thing_manager: ThingManager,
//     next_index: usize,
//     upstream: Box<dyn Stage>,
//     accumulator: AccumulatingStage<Executor>,
//     state: AccumulatingStageState,
// }
//
// // TODO: Implement LendingIterator instead
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

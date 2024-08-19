/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::variable_value::VariableValue;
use crate::batch::Row;

// TODO: Optimise for allocations
pub(crate) struct RowAccumulator {
    width: usize,
    rows: Vec<Row<'static>>,
}

impl RowAccumulator {
    pub(crate) fn new(width: usize) -> Self {
        Self { rows: Vec::new() , width }
    }
    // pub(crate) fn allocate_next(&mut self) -> &mut [VariableValue<'static>] {
    //     let row = (0..self.width).map().collect::<Vec<_>>().into_boxed_slice();
    //     self.rows.push(row);
    //     self.rows.last_mut().unwrap()
    // }
    //
    // pub(crate) fn into_iterator(self) -> AccumulatedRowIterator {
    //     AccumulatedRowIterator { next_index: 0, rows: self.rows.into_boxed_slice() }
    // }
}

// pub struct AccumulatedRowIterator {
//     next_index: usize,
//     rows: Box<[Box<[VariableValue<'static>]>]>,
// }
//
// impl RowAccumulator {
//     fn accumulate(&self) {
//         rows
//     }
// }
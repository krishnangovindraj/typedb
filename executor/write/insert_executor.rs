/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
};
use std::sync::Arc;
use answer::variable_value::VariableValue;

use compiler::{
    delete::{delete::DeletePlan, instructions::DeleteInstruction},
    insert::{insert::InsertPlan, instructions::InsertInstruction},
};
use concept::{error::ConceptWriteError, thing::thing_manager::ThingManager};
use concept::error::ConceptReadError;
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{
    batch::Row,
    write::{
        common::populate_output_row,
        write_instruction::{AsDeleteInstruction, AsInsertInstruction},
    },
};
use crate::accumulator::RowAccumulator;
use crate::batch::{Batch, BatchRowIterator, ImmutableRow};
use crate::pattern_executor::PatternExecutor;

//
pub struct InsertExecutor {
    plan: InsertPlan,
    accumulator: RowAccumulator
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        let accumulator =  RowAccumulator::new(plan.n_created_concepts);
        Self { plan, accumulator }
    }
}
//
// pub struct AccumulatingInserter<Snapshot: WritableSnapshot> {
//     executor: InsertExecutor,
//     snapshot: Arc<Snapshot>,
//     thing_manager: Arc<ThingManager>,
//     row: Row<'static>
// }
//
// impl<Snapshot: WritableSnapshot> AccumulatingInserter<Snapshot> {
//     fn new(executor: InsertExecutor, snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
//         Self { executor, snapshot, thing_manager }
//     }
//
//     fn for_row(&mut self, input_row: ImmutableRow<'_>) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
//         (0..input_row.get_multiplicity()).flat_map(|_| {
//             self.executor.execute_insert(self.snapshot, self.thing_manager, &mut input_row)
//         })
//     }
// }
//

impl InsertExecutor {
    
    pub fn execute_insert<'input, 'output>(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        row: &mut Row<'static>,
    ) -> Result<(), WriteError> {
        debug_assert!(row.multiplicity() == 1); // The accumulator should de-duplicate for insert
        let Self {plan, accumulator } = self;
        for instruction in &plan.instructions {
            match instruction {
                InsertInstruction::PutAttribute(isa_attr) => {
                    isa_attr.insert(snapshot, thing_manager, row)?
                }
                InsertInstruction::PutObject(isa_object) => {
                    isa_object.insert(snapshot, thing_manager, row)?
                }
                InsertInstruction::Has(has) => has.insert(snapshot, thing_manager, row)?,
                InsertInstruction::RolePlayer(role_player) => {
                    role_player.insert(snapshot, thing_manager, row)?
                }
            };
        }
        Ok(())
    }
}
//
// pub fn execute_delete<'input, 'output>(
//     // TODO: pub(crate)
//     snapshot: &mut impl WritableSnapshot,
//     thing_manager: &ThingManager,
//     plan: &DeletePlan,
//     input: &Row<'input>
// ) -> Result<Row<'output>, WriteError> {
//     debug_assert!(input.multiplicity() == 1); // Else, we have to return a set of rows.
//
//     for instruction in &plan.instructions {
//         match instruction {
//             DeleteInstruction::Thing(thing) => thing.delete(snapshot, thing_manager, input)?,
//             DeleteInstruction::Has(has) => has.delete(snapshot, thing_manager, input)?,
//             DeleteInstruction::RolePlayer(role_player) => role_player.delete(snapshot, thing_manager, input)?,
//         }
//     }
//
//     let mut tmp_output = (0..plan.output_row_plan.len()).map(|_| VariableValue::EMPTY).collect::<Vec<_>>().into_boxed_slice();
//     populate_output_row(&plan.output_row_plan, input, [].as_slice(), &mut tmp_output);
//     Ok(output) // TODO: Create output row
// }

#[derive(Debug, Clone)]
pub enum WriteError {
    ConceptWrite { source: ConceptWriteError },
}

impl Display for WriteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl Error for WriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ConceptWrite { source, .. } => Some(source),
        }
    }
}

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
use lending_iterator::{AsLendingIterator, LendingIterator};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    batch::Row,
    write::{
        common::populate_output_row,
        write_instruction::{AsDeleteInstruction, AsInsertInstruction},
    },
};
use crate::batch::{Batch, BatchRowIterator, ImmutableRow};
use crate::pattern_executor::PatternExecutor;

//
pub struct InsertExecutor {
    plan: InsertPlan,
    reused_created_things: Vec<answer::Thing<'static>>,
    reused_output_row: Box<[VariableValue<'static>]>,
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        let reused_created_things = Vec::with_capacity(plan.n_created_concepts);
        let reused_output_row = (0..plan.output_row_plan.len()).map(|_| VariableValue::EMPTY).collect::<Vec<_>>();
        Self { plan, reused_created_things, reused_output_row: reused_output_row.into_boxed_slice() }
    }


    // pub fn into_iterator(
    //     self,
    //     snapshot: Arc<impl ReadableSnapshot + 'static>,
    //     thing_manager: Arc<ThingManager>,
    // ) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
    //     AsLendingIterator::new(InsertingIterator::new(self, snapshot, thing_manager))
    // }
}

pub struct InsertingFlatMapper<Snapshot: WritableSnapshot> {
    executor: InsertExecutor,
    snapshot: Arc<Snapshot>,
    thing_manager: Arc<ThingManager>,
    row: Row<'static>
}

impl<Snapshot: WritableSnapshot> InsertingFlatMapper<Snapshot> {
    fn new(executor: InsertExecutor, snapshot: Arc<Snapshot>, thing_manager: Arc<ThingManager>) -> Self {
        Self { executor, snapshot, thing_manager }
    }

    fn for_row(&mut self, input_row: ImmutableRow<'_>) -> impl for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, &'a ConceptReadError>> {
        (0..input_row.get_multiplicity()).flat_map(|_| {
            self.executor.execute_insert(self.snapshot, self.thing_manager, input_row)
        })
    }
}


impl InsertExecutor {
    pub fn execute_insert<'input, 'output>(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        input: &Row<'input>,
    ) -> Result<ImmutableRow<'_>, WriteError> {
        debug_assert!(input.multiplicity() == 1); // Else, we have to return a set of rows.
        let Self {plan, reused_created_things, reused_output_row } = self;
        for instruction in &plan.instructions {
            let inserted = match instruction {
                InsertInstruction::PutAttribute(isa_attr) => {
                    isa_attr.insert(snapshot, thing_manager, &input, reused_created_things)?
                }
                InsertInstruction::PutObject(isa_object) => {
                    isa_object.insert(snapshot, thing_manager, &input, reused_created_things)?
                }
                InsertInstruction::Has(has) => has.insert(snapshot, thing_manager, &input, reused_created_things)?,
                InsertInstruction::RolePlayer(role_player) => {
                    role_player.insert(snapshot, thing_manager, &input, reused_created_things)?
                }
            };
            if let Some(thing) = inserted {
                reused_created_things.push(thing);
            }
        }
        populate_output_row(&plan.output_row_plan, input, reused_created_things.as_slice(), reused_output_row);
        Ok(ImmutableRow::new(reused_output_row, 1))
    }
}

pub fn execute_delete<'input, 'output>(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    plan: &DeletePlan,
    input: &Row<'input>
) -> Result<Row<'output>, WriteError> {
    debug_assert!(input.multiplicity() == 1); // Else, we have to return a set of rows.

    for instruction in &plan.instructions {
        match instruction {
            DeleteInstruction::Thing(thing) => thing.delete(snapshot, thing_manager, input)?,
            DeleteInstruction::Has(has) => has.delete(snapshot, thing_manager, input)?,
            DeleteInstruction::RolePlayer(role_player) => role_player.delete(snapshot, thing_manager, input)?,
        }
    }
    let mut output = output;
    let mut tmp_output = (0..plan.output_row_plan.len()).map(|_| VariableValue::EMPTY).collect::<Vec<_>>().into_boxed_slice();
    populate_output_row(&plan.output_row_plan, input, [].as_slice(), &mut tmp_output);
    Ok(output) // TODO: Create output row
}

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

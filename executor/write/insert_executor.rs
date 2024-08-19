/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    error::Error,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use answer::variable_value::VariableValue;
use compiler::{
    delete::{delete::DeletePlan, instructions::DeleteEdge},
    insert::{
        insert::InsertPlan,
        instructions::{InsertEdgeInstruction, InsertVertexInstruction},
        ThingSource,
    },
};
use concept::{
    error::{ConceptReadError, ConceptWriteError},
    thing::thing_manager::ThingManager,
};
use lending_iterator::LendingIterator;
use storage::snapshot::WritableSnapshot;

use crate::{accumulator::RowAccumulator, batch::Row, write::write_instruction::AsWriteInstruction};

//
pub struct InsertExecutor {
    plan: InsertPlan,
    accumulator: RowAccumulator,
}

impl InsertExecutor {
    pub fn new(plan: InsertPlan) -> Self {
        let accumulator = RowAccumulator::new(plan.output_row_plan.len());
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
    pub fn execute_insert(
        &mut self,
        snapshot: &mut impl WritableSnapshot,
        thing_manager: &ThingManager,
        row: &mut Row<'_>,
    ) -> Result<(), WriteError> {
        debug_assert!(row.multiplicity() == 1); // The accumulator should de-duplicate for insert
        let Self { plan, accumulator } = self;
        for instruction in &plan.vertex_instructions {
            match instruction {
                InsertVertexInstruction::PutAttribute(isa_attr) => {
                    isa_attr.execute(snapshot, thing_manager, row)?;
                }
                InsertVertexInstruction::PutObject(isa_object) => {
                    let inserted = isa_object.execute(snapshot, thing_manager, row)?;
                }
            }
        }
        for instruction in &plan.edge_instructions {
            match instruction {
                InsertEdgeInstruction::Has(has) => {
                    has.execute(snapshot, thing_manager, row)?;
                }
                InsertEdgeInstruction::RolePlayer(role_player) => {
                    role_player.execute(snapshot, thing_manager, row)?;
                }
            };
        }
        Ok(())
    }
}

pub fn execute_delete(
    // TODO: pub(crate)
    snapshot: &mut impl WritableSnapshot,
    thing_manager: &ThingManager,
    plan: &DeletePlan,
    row: &mut Row<'_>,
) -> Result<(), WriteError> {
    // Row multiplicity doesn't matter. You can't delete the same thing twice
    for instruction in &plan.edge_instructions {
        match instruction {
            DeleteEdge::Has(has) => has.execute(snapshot, thing_manager, row)?,
            DeleteEdge::RolePlayer(role_player) => role_player.execute(snapshot, thing_manager, row)?,
        }
    }

    for instruction in &plan.vertex_instructions {
        instruction.execute(snapshot, thing_manager, row)?;
    }
    Ok(())
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

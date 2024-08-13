/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use compiler::{
    instruction::constraint::instructions::ConstraintInstruction,
    planner::{function_plan::FunctionPlan, pattern_plan::Step, program_plan::ProgramPlan},
    write::{
        insert::{InsertInstruction, InsertPlan},
        VariableSource,
    },
};
use encoding::graph::definition::definition_key::DefinitionKey;
use function::function_manager::ReadThroughFunctionSignatureIndex;
use ir::{
    pattern::ScopeId,
    program::block::BlockContext,
    translation::{
        match_::translate_match,
        writes::{translate_delete, translate_insert},
    },
    PatternDefinitionError,
};
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};
use typeql::query::stage::Stage;

struct StageIndex(usize);

// TODO: Where does this go?
pub struct Pipeline {
    stages: Vec<NonTerminalStage>,
    functions: HashMap<DefinitionKey<'static>, FunctionPlan>, // This should be in a program, but not in a pipeline

    variable_annotation_sources: HashMap<String, Vec<StageIndex>>, // The declaring block & any match blocks that mention it.
}

impl Pipeline {
    fn new(
        stages: Vec<NonTerminalStage>,
        functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
        variable_annotation_sources: HashMap<String, Vec<StageIndex>>,
    ) -> Self {
        Self { stages, functions, variable_annotation_sources }
    }
}

pub trait Clause {
    type Instruction;
    fn root_scope(&self) -> ScopeId;
}

pub enum NonTerminalStage {
    Match(ProgramPlan),
    Insert(InsertPlan),
    // Delete(DeleteClause),
    // Put(PutClause),
    // Update(UpdateClause),
    // OperatorSelect(SelectOperator),
    // OperatorDistinct(DistinctOperator),
}

// TODO: This needing access to pipeline internals, yet being a translate method indicates that either:
//  1. ir::translate should be in query     OR
//  2. The pipeline data-structures should be in IR.
fn translate_pipeline<Snapshot: WritableSnapshot>(
    // TODO: Separate function for ReadSnapshot?
    function_index: ReadThroughFunctionSignatureIndex<Snapshot>,
    typeql_pipeline: &typeql::query::Pipeline,
) -> Result<Pipeline, PatternDefinitionError> {
    let mut context = BlockContext::new();
    for stage in &typeql_pipeline.stages {
        match stage {
            Stage::Match(match_) => {
                translate_match(&mut context, &function_index, match_)?;
                // todo
            }
            Stage::Insert(insert) => {
                translate_insert(&mut context, insert)?;
                // todo
            }
            Stage::Delete(delete) => {
                translate_delete(&mut context, delete)?;
                // todo
            }
            Stage::Put(_) => todo!(),
            Stage::Update(_) => todo!(),
            Stage::Fetch(_) => todo!(),
            Stage::Reduce(_) => todo!(),
            Stage::Modifier(_) => todo!(),
        };
    }
    todo!()
    // Ok(Pipeline { stages, functions,  })
}

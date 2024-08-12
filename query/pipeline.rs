/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use compiler::instruction::constraint::instructions::ConstraintInstruction;
use compiler::planner::function_plan::FunctionPlan;
use compiler::planner::pattern_plan::Step;
use compiler::planner::program_plan::ProgramPlan;
use compiler::write::insert::{InsertInstruction, InsertPlan};
use compiler::write::VariableSource;
use encoding::graph::definition::definition_key::DefinitionKey;
use ir::pattern::ScopeId;
use ir::program::block::BlockContext;


struct StageIndex(usize);

// TODO: Where does this go?
pub struct Pipeline {
    stages: Vec<NonTerminalStage>,
    functions: HashMap<DefinitionKey<'static>, FunctionPlan>,

    variable_annotation_sources: HashMap<String, Vec<StageIndex>>, // The declaring block & any match blocks that mention it.
}

impl Pipeline {
    fn new(
        stages: Vec<NonTerminalStage>,
        functions: HashMap<DefinitionKey<'static>, FunctionPlan>,
        variable_annotation_sources: HashMap<String, Vec<StageIndex>>,
    ) -> Self {
        Self { stages, functions, variable_annotation_sources}
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

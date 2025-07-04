/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use structural_equality::StructuralEquality;

use crate::{
    pattern::{
        conjunction::{Conjunction, ConjunctionBuilder},
        BranchID, Scope, ScopeId, VariableDependency,
    },
    pipeline::block::{BlockBuilderContext, BlockContext, VariableLocality},
};

#[derive(Debug, Clone)]
pub struct Optional {
    conjunction: Conjunction,
    branch_id: BranchID,
}

impl Optional {
    pub fn new(scope_id: ScopeId, branch_id: BranchID) -> Self {
        Self { conjunction: Conjunction::new(scope_id), branch_id }
    }

    pub(super) fn new_builder<'cx, 'reg>(
        context: &'cx mut BlockBuilderContext<'reg>,
        optional: &'cx mut Optional,
    ) -> ConjunctionBuilder<'cx, 'reg> {
        ConjunctionBuilder::new(context, &mut optional.conjunction)
    }

    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn branch_id(&self) -> BranchID {
        self.branch_id
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    fn referenced_variables(&self) -> impl Iterator<Item = Variable> + '_ {
        self.conjunction().referenced_variables()
    }

    pub fn optional_variables(&self, block_context: &BlockContext) -> impl Iterator<Item = Variable> + '_ {
        self.variable_dependency(block_context).into_iter().filter_map(|(v, mode)| {
            mode.is_producing().then_some(v)
        })
    }

    pub fn required_inputs<'a>(&'a self, block_context: &'a BlockContext) -> impl Iterator<Item = Variable> + 'a {
        self.variable_dependency(block_context).into_iter().filter_map(|(v, mode)| {
            mode.is_required().then_some(v)
        })
    }

    pub(crate) fn variable_dependency(
        &self,
        block_context: &BlockContext,
    ) -> HashMap<Variable, VariableDependency<'_>> {
        self.conjunction
            .variable_dependency(block_context)
            .into_iter()
            .map(|(var, mut mode)| {
                let status = block_context.variable_status_in_scope(var, self.scope_id());
                if status == VariableLocality::Parent || mode.is_required() {
                    mode.set_required();
                }
                (var, mode)
            })
            .collect()
    }
}

impl Scope for Optional {
    fn scope_id(&self) -> ScopeId {
        self.conjunction.scope_id()
    }
}

impl StructuralEquality for Optional {
    fn hash(&self) -> u64 {
        self.conjunction.hash()
    }

    fn equals(&self, other: &Self) -> bool {
        self.conjunction.equals(&other.conjunction)
    }
}

impl fmt::Display for Optional {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        error::todo_display_for_error!(f, self)
    }
}

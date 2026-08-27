/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, fmt};

use answer::variable::Variable;
use structural_equality::StructuralEquality;
use typeql::common::Span;

use crate::pattern::{
    conjunction::{Conjunction, ConjunctionBuilder}, impl_pattern_from_pattern_variables, nested_pattern::NestedPattern, BranchID, ContextualisedBindingMode, Pattern,
    PatternVariables,
    Scope,
    ScopeId,
};
use crate::pattern::mode_inference::{OptionalityStatus, VariableBindingMode, VariableUsageMode};

#[derive(Debug, Clone)]
pub struct Optional {
    conjunction: Conjunction,
    branch_id: BranchID,
    pattern_variables: PatternVariables,
    source_span: Option<Span>,
}

impl Optional {
    pub fn conjunction(&self) -> &Conjunction {
        &self.conjunction
    }

    pub fn branch_id(&self) -> BranchID {
        self.branch_id
    }

    pub fn conjunction_mut(&mut self) -> &mut Conjunction {
        &mut self.conjunction
    }

    pub(crate) fn source_span(&self) -> Option<Span> {
        self.source_span
    }
}

impl_pattern_from_pattern_variables!(Optional);

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

#[derive(Debug)]
pub(crate) struct OptionalBuilder {
    conjunction: ConjunctionBuilder,
    branch_id: BranchID,
    source_span: Option<Span>,
}

impl OptionalBuilder {
    pub(crate) fn new(scope_id: ScopeId, branch_id: BranchID, source_span: Option<Span>) -> Self {
        let conjunction = ConjunctionBuilder::new(scope_id);
        Self { conjunction, branch_id, source_span }
    }

    pub(crate) fn finish(self, parent_modes: &ContextualisedBindingMode) -> NestedPattern {
        let source_span = self.source_span;
        let binding_modes = ContextualisedBindingMode::from(self.variable_usage_modes(), parent_modes);
        let branch_id = self.branch_id;
        let conjunction = self.conjunction.finish(&binding_modes);
        let pattern_variables = PatternVariables::from(&binding_modes);
        NestedPattern::Optional(Optional { branch_id, conjunction, pattern_variables, source_span })
    }

    pub(crate) fn conjunction(&self) -> &ConjunctionBuilder {
        &self.conjunction
    }

    pub fn conjunction_mut(&mut self) -> &mut ConjunctionBuilder {
        &mut self.conjunction
    }

    pub(crate) fn variable_usage_modes(&self) -> HashMap<Variable, VariableUsageMode> {
        let mut inner_modes = self.conjunction.variable_usage_modes();
        inner_modes.iter_mut().for_each(|(var, mode)| {
            if mode.binding_mode.is_always_binding() {
                mode.binding_mode = VariableBindingMode::BoundInTry
            }
            mode.optionality_status = OptionalityStatus::IntactInSomePaths;
            // mode.optional_safety = mode.optional_safety; // Unless `try` implicitly `isset`s
        });
        inner_modes
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use core::{cmp::Eq, default::Default};
use std::{
    collections::HashMap,
    iter,
    ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign},
};

use answer::variable::Variable;
use typeql::common::Span;

use crate::pattern::{
    LocationNote,
    conjunction::{ConjunctionBuilder, NestedPatternBuilder},
    constraint::Constraint,
    disjunction::DisjunctionBuilder,
    negation::NegationBuilder,
    optional::OptionalBuilder,
    variable_category::VariableOptionality,
};

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct VariableUsageMode {
    pub(crate) binding_mode: BindingMode,
    pub(crate) assignment: AssignmentStatus,
    pub(crate) optional_safety: OptionalReferenceSafety,
    pub(crate) optionality_status: OptionalityStatus,
}

impl VariableUsageMode {
    pub(crate) fn of_constraint(
        constraint: &Constraint<Variable>,
    ) -> Box<dyn Iterator<Item = (Variable, VariableUsageMode)> + '_> {
        fn _all_binding<'a>(
            it: impl Iterator<Item = Variable> + 'a,
            span: LocationNote,
        ) -> Box<dyn Iterator<Item = (Variable, VariableUsageMode)> + 'a> {
            Box::new(it.map(move |id| (id, VariableUsageMode::regular_binding(span))))
        }
        fn _all_required<'a, ID1>(
            it: impl Iterator<Item = ID1> + 'a,
            span: LocationNote,
        ) -> Box<dyn Iterator<Item = (ID1, VariableUsageMode)> + 'a> {
            Box::new(it.map(move |id| (id, VariableUsageMode::regular_input(span))))
        }
        let span = constraint.source_span();
        match constraint {
            Constraint::Kind(kind) => _all_binding(kind.ids(), span),
            Constraint::Label(label) => _all_binding(label.ids(), span),
            Constraint::RoleName(role_name) => _all_binding(role_name.ids(), span),
            Constraint::Sub(sub) => _all_binding(sub.ids(), span),
            Constraint::Isa(isa) => _all_binding(isa.ids(), span),
            Constraint::Iid(iid) => _all_binding(iid.ids(), span),
            Constraint::Links(rp) => _all_binding(rp.ids(), span),
            Constraint::IndexedRelation(indexed) => _all_binding(indexed.ids(), span),
            Constraint::Has(has) => _all_binding(has.ids(), span),
            Constraint::Owns(owns) => _all_binding(owns.ids(), span),
            Constraint::Relates(relates) => _all_binding(relates.ids(), span),
            Constraint::Plays(plays) => _all_binding(plays.ids(), span),
            Constraint::Value(value) => _all_binding(value.ids(), span),

            Constraint::Comparison(comparison) => _all_required(comparison.ids(), span),
            Constraint::Is(is) => _all_binding(is.ids(), span),
            Constraint::Unsatisfiable(inner) => _all_binding(inner.ids(), span),
            Constraint::IsSet(inner) => _all_required(inner.ids(), span),
            Constraint::LinksDeduplication(_) => Box::new(iter::empty()),

            Constraint::ExpressionBinding(binding) => {
                let arg_modes = binding.expression_ids().map(move |id| (id, VariableUsageMode::regular_input(span)));
                let assigned_modes =
                    binding.ids_assigned().map(move |id| (id, VariableUsageMode::regular_binding(span)));
                Box::new(arg_modes.chain(assigned_modes))
            }
            Constraint::FunctionCallBinding(binding) => {
                let arg_modes =
                    binding.function_call_arg_ids().map(move |id| (id, VariableUsageMode::regular_input(span)));
                let assigned_modes = binding
                    .assigned_optionalities()
                    .map(move |(id, optionality)| (id, VariableUsageMode::assigned(optionality, span)));
                Box::new(arg_modes.chain(assigned_modes))
            }
        }
    }

    pub(crate) fn absent() -> Self {
        Self {
            binding_mode: BindingMode::Absent,
            assignment: AssignmentStatus::NotAssigned,
            optional_safety: OptionalReferenceSafety::AbsentOrSafe,
            optionality_status: OptionalityStatus::IntactInSomePaths,
        }
    }

    pub(crate) fn regular_binding(location: LocationNote) -> Self {
        Self {
            binding_mode: BindingMode::AlwaysBinding,
            assignment: AssignmentStatus::NotAssigned,
            optional_safety: OptionalReferenceSafety::UnwrappingReference(location),
            optionality_status: OptionalityStatus::UnwrappedInAllPaths,
        }
    }

    pub(crate) fn regular_input(location: LocationNote) -> Self {
        Self {
            binding_mode: BindingMode::RequirePrebound,
            assignment: AssignmentStatus::NotAssigned,
            optional_safety: OptionalReferenceSafety::UnwrappingReference(location),
            optionality_status: OptionalityStatus::UnwrappedInAllPaths,
        }
    }

    pub(crate) fn assigned(return_optionality: VariableOptionality, location: LocationNote) -> Self {
        let (optional_safety, optionality_status) = match return_optionality {
            VariableOptionality::Optional => (
                OptionalReferenceSafety::AssignedOrStageInput(location), // TODO: I had AbsentOrSafe down.
                OptionalityStatus::IntactInSomePaths,
            ),
            VariableOptionality::Required => {
                (OptionalReferenceSafety::UnwrappingReference(location), OptionalityStatus::UnwrappedInAllPaths)
            }
        };
        Self {
            binding_mode: BindingMode::AlwaysBinding,
            assignment: AssignmentStatus::AtMostOncePerBranch(location),
            optional_safety,
            optionality_status,
        }
    }
}

impl BitAnd for VariableUsageMode {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self {
            assignment: self.assignment & rhs.assignment,
            binding_mode: self.binding_mode & rhs.binding_mode,
            optional_safety: self.optional_safety & rhs.optional_safety,
            optionality_status: self.optionality_status & rhs.optionality_status,
        }
    }
}

impl BitOr for VariableUsageMode {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self {
            assignment: self.assignment | rhs.assignment,
            binding_mode: self.binding_mode | rhs.binding_mode,
            optional_safety: self.optional_safety | rhs.optional_safety,
            optionality_status: self.optionality_status | rhs.optionality_status,
        }
    }
}

impl BitAndAssign for VariableUsageMode {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}

impl BitOrAssign for VariableUsageMode {
    fn bitor_assign(&mut self, rhs: Self) {
        *self = *self | rhs;
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum BindingMode {
    RequirePrebound,
    AlwaysBinding,
    LocallyBindingInChild, // Bound in some, but not all branches
    BoundInTry,
    #[default]
    Absent,
}

impl BindingMode {
    pub fn is_require_prebound(&self) -> bool {
        *self == BindingMode::RequirePrebound
    }

    pub fn is_always_binding(&self) -> bool {
        *self == BindingMode::AlwaysBinding
    }

    pub fn is_locally_binding_in_child(&self) -> bool {
        *self == BindingMode::LocallyBindingInChild
    }

    pub fn is_optionally_binding(&self) -> bool {
        *self == BindingMode::BoundInTry
    }
}

impl BitAnd for BindingMode {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        // We upgrade (Optionally|LocallyBinding) & (Optionally|LocallyBinding) to RequirePrebound
        match (self, rhs) {
            (Self::Absent, x) | (x, Self::Absent) => x,
            (Self::AlwaysBinding, _) | (_, Self::AlwaysBinding) => Self::AlwaysBinding,
            (Self::RequirePrebound, _) | (_, Self::RequirePrebound) => Self::RequirePrebound,
            (Self::LocallyBindingInChild, _) | (_, Self::LocallyBindingInChild) => Self::RequirePrebound,
            (Self::BoundInTry, Self::BoundInTry) => Self::RequirePrebound,
        }
    }
}

impl BitOr for BindingMode {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::BoundInTry, Self::BoundInTry) => Self::BoundInTry,
            (Self::AlwaysBinding, Self::AlwaysBinding) => Self::AlwaysBinding,
            (Self::Absent, Self::Absent) => Self::Absent,
            (Self::Absent, Self::AlwaysBinding) | (Self::AlwaysBinding, Self::Absent) => Self::LocallyBindingInChild,
            (Self::Absent, Self::LocallyBindingInChild) | (Self::LocallyBindingInChild, Self::Absent) => {
                Self::LocallyBindingInChild
            }
            (Self::RequirePrebound, _) | (_, Self::RequirePrebound) => Self::RequirePrebound,
            (Self::BoundInTry, _) | (_, Self::BoundInTry) => Self::RequirePrebound,
            (Self::LocallyBindingInChild, _) | (_, Self::LocallyBindingInChild) => {
                // This preserves associativity, but doesn't correctly escalate to RequirePrebound.
                // ((AlwaysBinding | AlwaysBinding) | Absent) should be required
                // That's corrected in disjunction
                Self::LocallyBindingInChild
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum AssignmentStatus {
    #[default]
    NotAssigned,
    AtMostOncePerBranch(Option<Span>),
    ErrorMultipleAssignments(Option<Span>, Option<Span>),
}

impl BitAnd for AssignmentStatus {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::NotAssigned, x) | (x, Self::NotAssigned) => x,
            (Self::ErrorMultipleAssignments(s1, s2), _) | (_, Self::ErrorMultipleAssignments(s1, s2)) => {
                Self::ErrorMultipleAssignments(s1, s2)
            }
            (Self::AtMostOncePerBranch(s1), Self::AtMostOncePerBranch(s2)) => Self::ErrorMultipleAssignments(s1, s2),
        }
    }
}

impl BitOr for AssignmentStatus {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::NotAssigned, x) | (x, Self::NotAssigned) => x,
            (Self::ErrorMultipleAssignments(s1, s2), _) | (_, Self::ErrorMultipleAssignments(s1, s2)) => {
                Self::ErrorMultipleAssignments(s1, s2)
            }
            (Self::AtMostOncePerBranch(s), Self::AtMostOncePerBranch(_)) => Self::AtMostOncePerBranch(s),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum OptionalReferenceSafety {
    // We use this ONLY to check all references are "safe".
    // The semantic issues of two try blocks assigning the same variable,
    //      and actual variable optionality are computed by other `*Mode`s
    UnsafeUnwrap(LocationNote),         // Can be reset by a safe unwrap in a parent
    UnwrappingReference(LocationNote),  // match $x isa person;
    AssignedOrStageInput(LocationNote), // Can we treat Assign & Input them as the same thing?

    // TryBlockReference,      // Identical to regular reference?
    #[default]
    AbsentOrSafe, // When uninteresting
}

impl BitAnd for OptionalReferenceSafety {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        match (self, rhs) {
            (OptionalReferenceSafety::AbsentOrSafe, other) | (other, OptionalReferenceSafety::AbsentOrSafe) => other,
            (OptionalReferenceSafety::UnsafeUnwrap(location), _)
            | (_, OptionalReferenceSafety::UnsafeUnwrap(location)) => OptionalReferenceSafety::UnsafeUnwrap(location),
            (
                OptionalReferenceSafety::AssignedOrStageInput(location),
                OptionalReferenceSafety::AssignedOrStageInput(_),
            ) => {
                OptionalReferenceSafety::UnsafeUnwrap(location) // Should trigger the MultipleAssignments
            }
            (
                OptionalReferenceSafety::UnwrappingReference(location),
                OptionalReferenceSafety::AssignedOrStageInput(_),
            )
            | (
                OptionalReferenceSafety::AssignedOrStageInput(_),
                OptionalReferenceSafety::UnwrappingReference(location),
            ) => OptionalReferenceSafety::UnsafeUnwrap(location),
            (
                OptionalReferenceSafety::UnwrappingReference(location),
                OptionalReferenceSafety::UnwrappingReference(_),
            ) => OptionalReferenceSafety::UnwrappingReference(location),
        }
    }
}

impl BitOr for OptionalReferenceSafety {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (OptionalReferenceSafety::AbsentOrSafe, other) | (other, OptionalReferenceSafety::AbsentOrSafe) => other,
            (OptionalReferenceSafety::UnsafeUnwrap(location), _)
            | (_, OptionalReferenceSafety::UnsafeUnwrap(location)) => OptionalReferenceSafety::UnsafeUnwrap(location),
            (
                OptionalReferenceSafety::AssignedOrStageInput(location),
                OptionalReferenceSafety::AssignedOrStageInput(_),
            ) => {
                OptionalReferenceSafety::AssignedOrStageInput(location) // Should trigger the MultipleAssignments
            }
            (
                OptionalReferenceSafety::UnwrappingReference(location),
                OptionalReferenceSafety::AssignedOrStageInput(_),
            )
            | (
                OptionalReferenceSafety::AssignedOrStageInput(_),
                OptionalReferenceSafety::UnwrappingReference(location),
            ) => {
                OptionalReferenceSafety::AssignedOrStageInput(location) // Should be illegal unsatisfiable input / double assignment
            }
            (
                OptionalReferenceSafety::UnwrappingReference(location),
                OptionalReferenceSafety::UnwrappingReference(_),
            ) => OptionalReferenceSafety::UnwrappingReference(location),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum OptionalityStatus {
    UnwrappedInAllPaths,
    // TryBlockReference, // Identical to regular reference?
    #[default]
    IntactInSomePaths, // Can the optionality survive?
}

// Is it true that the scope of Optionality is now the block?
impl BitAnd for OptionalityStatus {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::UnwrappedInAllPaths, _) | (_, Self::UnwrappedInAllPaths) => Self::UnwrappedInAllPaths,
            (Self::IntactInSomePaths, Self::IntactInSomePaths) => Self::IntactInSomePaths,
        }
    }
}

impl BitOr for OptionalityStatus {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (Self::IntactInSomePaths, _) | (_, Self::IntactInSomePaths) => Self::IntactInSomePaths,
            (Self::UnwrappedInAllPaths, Self::UnwrappedInAllPaths) => Self::UnwrappedInAllPaths,
        }
    }
}

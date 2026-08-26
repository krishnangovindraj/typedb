/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::ops::{BitAnd, BitOr, BitAndAssign, BitOrAssign};
use core::default::Default;
use core::cmp::Eq;
use typeql::common::Span;
use answer::variable::Variable;
use std::collections::HashMap;
use crate::pattern::conjunction::{ConjunctionBuilder, NestedPatternBuilder};
use crate::pattern::constraint::Constraint;
use crate::pattern::disjunction::DisjunctionBuilder;
use crate::pattern::LocationNote;
use crate::pattern::negation::NegationBuilder;
use crate::pattern::optional::OptionalBuilder;
use crate::pattern::variable_category::VariableOptionality;

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
        if let Constraint::FunctionCallBinding(f) = constraint {
            Box::new(f.binding_modes().filter_map(|(id, binding_mode)| match binding_mode {
                BindingMode::AlwaysBinding => {
                    Some((id, VariableUsageMode::assigned(VariableOptionality::Required, f.source_span())))
                }
                BindingMode::BoundInTry => {
                    Some((id, VariableUsageMode::assigned(VariableOptionality::Optional, f.source_span())))
                }
                BindingMode::RequirePrebound => Some((id, VariableUsageMode::regular_reference(f.source_span()))),
                BindingMode::Absent | BindingMode::LocallyBindingInChild => {
                    debug_assert!(false, "Unreachable");
                    None
                }
            }))
        } else {
            Box::new(constraint.ids().map(|id| (id, VariableUsageMode::regular_reference(constraint.source_span()))))
        }
    }

    pub(crate) fn for_conjunction(conjunction: &ConjunctionBuilder) -> HashMap<Variable, Self> {
        let mut modes = HashMap::new();
        conjunction.constraints().iter().flat_map(Self::of_constraint).for_each(|(id, mode)| {
            *modes.entry(id).or_default() &= mode;
        });

        conjunction
            .nested_patterns()
            .iter()
            .flat_map(|nested| match nested {
                NestedPatternBuilder::Disjunction(disjunction) => Self::for_disjunction(disjunction).into_iter(),
                NestedPatternBuilder::Negation(negation) => Self::for_negation(negation).into_iter(),
                NestedPatternBuilder::Optional(optional) => Self::for_optional(optional).into_iter(),
            })
            .for_each(|(id, mode)| {
                *modes.entry(id).or_default() &= mode;
            });

        // Reset any safely unwrapped
        for is_set in conjunction.constraints().iter().filter_map(|c| c.as_is_set()) {
            for id in is_set.ids() {
                let entry: &mut VariableUsageMode = modes.entry(id).or_default();
                entry.optional_safety = OptionalReferenceSafety::AbsentOrSafe;
            }
        }
        modes
    }

    pub(crate) fn for_disjunction(disjunction: &DisjunctionBuilder) -> HashMap<Variable, Self> {
        let mut modes = HashMap::new();
        disjunction.conjunctions().flat_map(|branch| Self::for_conjunction(branch).into_iter()).for_each(
            |(id, mode)| {
                *modes.entry(id).or_default() |= mode;
            },
        );
        modes
    }

    fn for_negation(negation: NegationBuilder) -> HashMap<Variable, Self> {
        Self::for_conjunction(negation.conjunction()).into_iter().map(|(var, mode)| {
            // Could be `impl UnaryNot`, I guess
            let negated_mode = Self {
                binding_mode: ,
                assignment: mode.assignment,
                optional_safety: mode.optional_safety,
                optionality_status: OptionalityStatus::IntactInSomePaths,
            };
            (var, negated_mode)
        }).collect()
    }

    fn for_optional(negation: OptionalBuilder) -> Self {
        todo!()
    }

    fn new(assignment_mode: AssignmentStatus, optionality_mode: OptionalReferenceSafety) -> Self {
        Self {
            assignment: AssignmentStatus::NotAssigned,
            optional_safety: optionality_mode,

        }
    }

    pub(crate) fn regular_reference(location: LocationNote) -> Self {
        Self {
            assignment: AssignmentStatus::NotAssigned,
            optional_safety: OptionalReferenceSafety::RegularReference(location),

        }
    }

    pub(crate) fn assigned(optionality: VariableOptionality, location: LocationNote) -> Self {
        let assignment_mode = AssignmentStatus::AtMostOncePerBranch(location);
        let optionality_mode = match optionality {
            VariableOptionality::Optional => OptionalReferenceSafety::AssignedOrStageInput(location),
            VariableOptionality::Required => OptionalReferenceSafety::RegularReference(location),
        };
        Self::new(assignment_mode, optionality_mode)
    }
}

impl BitAnd for VariableUsageMode {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        Self {
            assignment: self.assignment & rhs.assignment,
            binding_mode: self.binding_mode & rhs.binding_mode,
            optional_safety: self.optional_safety & rhs.optional_safety,
            optionality_status: self.optionality_status & rhs.optional_safety,
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
            optionality_status: self.optionality_status | rhs.optional_safety,
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

impl BitAndAssign for BindingMode {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
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

impl BitAndAssign for AssignmentStatus {
    fn bitand_assign(&mut self, rhs: Self) {
        *self = *self & rhs;
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum OptionalReferenceSafety {
    // We use this ONLY to check all references are "safe".
    // The semantic issues of two try blocks assigning the same variable,
    //      and actual variable optionality are computed by other `*Mode`s
    UnsafeUnwrap(LocationNote),         // Can be reset by a safe unwrap in a parent
    RegularReference(LocationNote),     // match $x isa person;
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
            (OptionalReferenceSafety::UnsafeUnwrap(location), _) | (_, OptionalReferenceSafety::UnsafeUnwrap(location)) => {
                OptionalReferenceSafety::UnsafeUnwrap(location)
            }
            (OptionalReferenceSafety::AssignedOrStageInput(location), OptionalReferenceSafety::AssignedOrStageInput(_)) => {
                OptionalReferenceSafety::UnsafeUnwrap(location) // Should trigger the MultipleAssignments
            }
            (OptionalReferenceSafety::RegularReference(location), OptionalReferenceSafety::AssignedOrStageInput(_))
            | (OptionalReferenceSafety::AssignedOrStageInput(_), OptionalReferenceSafety::RegularReference(location)) => {
                OptionalReferenceSafety::UnsafeUnwrap(location)
            }
            (OptionalReferenceSafety::RegularReference(location), OptionalReferenceSafety::RegularReference(_)) => {
                OptionalReferenceSafety::RegularReference(location)
            }
        }
    }
}

impl BitOr for OptionalReferenceSafety {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        match (self, rhs) {
            (OptionalReferenceSafety::AbsentOrSafe, other) | (other, OptionalReferenceSafety::AbsentOrSafe) => other,
            (OptionalReferenceSafety::UnsafeUnwrap(location), _) | (_, OptionalReferenceSafety::UnsafeUnwrap(location)) => {
                OptionalReferenceSafety::UnsafeUnwrap(location)
            }
            (OptionalReferenceSafety::AssignedOrStageInput(location), OptionalReferenceSafety::AssignedOrStageInput(_)) => {
                OptionalReferenceSafety::AssignedOrStageInput(location) // Should trigger the MultipleAssignments
            }
            (OptionalReferenceSafety::RegularReference(location), OptionalReferenceSafety::AssignedOrStageInput(_))
            | (OptionalReferenceSafety::AssignedOrStageInput(_), OptionalReferenceSafety::RegularReference(location)) => {
                OptionalReferenceSafety::AssignedOrStageInput(location) // Should be illegal unsatisfiable input / double assignment
            }
            (OptionalReferenceSafety::RegularReference(location), OptionalReferenceSafety::RegularReference(_)) => {
                OptionalReferenceSafety::RegularReference(location)
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) enum OptionalityStatus {
    UnwrappedInAllPaths,
    // TryBlockReference, // Identical to regular reference?
    #[default]
    IntactInSomePaths,    // Can the optionality survive?
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

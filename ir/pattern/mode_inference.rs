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
use std::io::Read;
use std::iter;
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
        fn _all_binding<'a>(
            it: impl Iterator<Item = Variable> + 'a,
            span: LocationNote,
        ) -> Box<dyn Iterator<Item = (Variable, VariableUsageMode)> + 'a> {
            Box::new(it.map(|id| (id, VariableUsageMode  {
                binding_mode: BindingMode::AlwaysBinding,
                assignment: AssignmentStatus::NotAssigned,
                optional_safety: OptionalReferenceSafety::UnwrappingReference(span),
                optionality_status: OptionalityStatus::UnwrappedInAllPaths,
            })))
        }
        fn _all_required<'a, ID1>(
            it: impl Iterator<Item = ID1> + 'a,
            span: LocationNote,
        ) -> Box<dyn Iterator<Item = (ID1, BindingMode)> + 'a> {
            Box::new(it.map(|id| (id, VariableUsageMode  {
                binding_mode: BindingMode::RequirePrebound,
                assignment: AssignmentStatus::NotAssigned,
                optional_safety: OptionalReferenceSafety::UnwrappingReference(span),
                optionality_status: OptionalityStatus::UnwrappedInAllPaths,
            })))
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
                Box::new(binding.binding_modes())
            },
            Constraint::FunctionCallBinding(binding) => {
                let arg_modes = binding.function_call_arg_ids().map(|id| (id, VariableUsageMode  {
                    binding_mode: BindingMode::RequirePrebound,
                    assignment: AssignmentStatus::NotAssigned,
                    optional_safety: OptionalReferenceSafety::UnwrappingReference(constraint.span()),
                    optionality_status: OptionalityStatus::UnwrappedInAllPaths,
                }));
                let assigned_modes = binding.assigned_optionalities().map(|(id, optionality)| {
                    let (optional_safety, optionality_status) = match optionality {
                        VariableOptionality::Optional => (
                            OptionalReferenceSafety::AbsentOrSafe,
                            OptionalityStatus::IntactInSomePaths
                        ),
                        VariableOptionality::Required => (
                            OptionalReferenceSafety::UnwrappingReference(constraint.source_span()),
                            OptionalityStatus::UnwrappedInAllPaths
                        ),
                    };
                    (id, VariableUsageMode  {
                        binding_mode: BindingMode::AlwaysBinding,
                        assignment: AssignmentStatus::AtMostOncePerBranch(constraint.source_span()),
                        optional_safety,
                        optionality_status,
                    })
                });
                Box::new(arg_modes.chain(assigned_modes))
            },
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

    fn for_negation(negation: &NegationBuilder) -> HashMap<Variable, Self> {
        Self::for_conjunction(negation.conjunction()).into_iter().map(|(var, mode)| {
            // Could be `impl UnaryNot`, I guess
            let binding_mode = if mode.binding_mode.is_always_binding() {
                // if it is binding, we demote it to only locally binding (only relevant in the negation)
                BindingMode::LocallyBindingInChild
            } else {
                mode.binding_mode
            };
            let negated_mode = Self {
                binding_mode,
                assignment: mode.assignment,
                optional_safety: mode.optional_safety,
                optionality_status: OptionalityStatus::IntactInSomePaths,
            };
            (var, negated_mode)
        }).collect()
    }

    fn for_optional(optional: &OptionalBuilder) -> HashMap<Variable, Self> {
        Self::for_conjunction(optional.conjunction()).into_iter().map(|(var, mode)| {
            let binding_mode = if mode.binding_mode.is_always_binding() {
                BindingMode::BoundInTry
            } else {
                mode.binding_mode
            };
            let negated_mode = Self {
                binding_mode: binding_mode,
                assignment: mode.assignment,
                optional_safety: mode.optional_safety, // Unless `try` implicitly `isset`s
                optionality_status: OptionalityStatus::IntactInSomePaths,
            };
            (var, negated_mode)
        }).collect()
    }

    pub(crate) fn regular_reference(location: LocationNote) -> Self {
        Self {
            binding_mode: BindingMode::AlwaysBinding,
            assignment: AssignmentStatus::NotAssigned,
            optional_safety: OptionalReferenceSafety::UnwrappingReference(location),
            optionality_status: OptionalityStatus::UnwrappedInAllPaths,
        }
    }

    pub(crate) fn assigned(return_optionality: VariableOptionality, location: LocationNote) -> Self {
        let optionality_safety = match return_optionality {
            VariableOptionality::Optional => OptionalReferenceSafety::AssignedOrStageInput(location),
            VariableOptionality::Required => OptionalReferenceSafety::UnwrappingReference(location),
        };
        Self {
            binding_mode: BindingMode::AlwaysBinding,
            assignment: AssignmentStatus::AtMostOncePerBranch(location),
            optional_safety: optionality_safety,
            optionality_status: OptionalityStatus::UnwrappedInAllPaths,
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
    UnwrappingReference(LocationNote),     // match $x isa person;
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
            (OptionalReferenceSafety::UnwrappingReference(location), OptionalReferenceSafety::AssignedOrStageInput(_))
            | (OptionalReferenceSafety::AssignedOrStageInput(_), OptionalReferenceSafety::UnwrappingReference(location)) => {
                OptionalReferenceSafety::UnsafeUnwrap(location)
            }
            (OptionalReferenceSafety::UnwrappingReference(location), OptionalReferenceSafety::UnwrappingReference(_)) => {
                OptionalReferenceSafety::UnwrappingReference(location)
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
            (OptionalReferenceSafety::UnwrappingReference(location), OptionalReferenceSafety::AssignedOrStageInput(_))
            | (OptionalReferenceSafety::AssignedOrStageInput(_), OptionalReferenceSafety::UnwrappingReference(location)) => {
                OptionalReferenceSafety::AssignedOrStageInput(location) // Should be illegal unsatisfiable input / double assignment
            }
            (OptionalReferenceSafety::UnwrappingReference(location), OptionalReferenceSafety::UnwrappingReference(_)) => {
                OptionalReferenceSafety::UnwrappingReference(location)
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

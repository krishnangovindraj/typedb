// // /*
// //  * This Source Code Form is subject to the terms of the Mozilla Public
// //  * License, v. 2.0. If a copy of the MPL was not distributed with this
// //  * file, You can obtain one at https://mozilla.org/MPL/2.0/.
// //  */
// use core::{cmp::Eq, default::Default};
// use std::{
//     iter,
//     ops::{BitAnd, BitAndAssign, BitOr, BitOrAssign},
// };
//
// use answer::variable::Variable;
// use typeql::common::Span;
//
// use crate::pattern::{constraint::Constraint, variable_category::VariableOptionality};
//
// #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
// pub(crate) struct VariableUsageMode {
//     pub(crate) assignment: AssignmentStatus,
//     pub(crate) optional_safety: OptionalReferenceSafety,
//     // pub(crate) optionality_status: OptionalityStatus,
// }
//
// impl VariableUsageMode {
//     pub(crate) fn of_constraint(
//         constraint: &Constraint<Variable>,
//     ) -> Box<dyn Iterator<Item = (Variable, VariableUsageMode)> + '_> {
//         fn _all_binding<'a>(
//             it: impl Iterator<Item = Variable> + 'a,
//             span: LocationNote,
//         ) -> Box<dyn Iterator<Item = (Variable, VariableUsageMode)> + 'a> {
//             Box::new(it.map(move |id| (id, VariableUsageMode::regular_binding(span))))
//         }
//         fn _all_required<'a, ID1>(
//             it: impl Iterator<Item = ID1> + 'a,
//             span: LocationNote,
//         ) -> Box<dyn Iterator<Item = (ID1, VariableUsageMode)> + 'a> {
//             Box::new(it.map(move |id| (id, VariableUsageMode::regular_input(span))))
//         }
//         let span = constraint.source_span();
//         match constraint {
//             Constraint::Kind(kind) => _all_binding(kind.ids(), span),
//             Constraint::Label(label) => _all_binding(label.ids(), span),
//             Constraint::RoleName(role_name) => _all_binding(role_name.ids(), span),
//             Constraint::Sub(sub) => _all_binding(sub.ids(), span),
//             Constraint::Isa(isa) => _all_binding(isa.ids(), span),
//             Constraint::Iid(iid) => _all_binding(iid.ids(), span),
//             Constraint::Links(rp) => _all_binding(rp.ids(), span),
//             Constraint::IndexedRelation(indexed) => _all_binding(indexed.ids(), span),
//             Constraint::Has(has) => _all_binding(has.ids(), span),
//             Constraint::Owns(owns) => _all_binding(owns.ids(), span),
//             Constraint::Relates(relates) => _all_binding(relates.ids(), span),
//             Constraint::Plays(plays) => _all_binding(plays.ids(), span),
//             Constraint::Value(value) => _all_binding(value.ids(), span),
//
//             Constraint::Comparison(comparison) => _all_required(comparison.ids(), span),
//             Constraint::Is(is) => _all_binding(is.ids(), span),
//             Constraint::Unsatisfiable(inner) => _all_binding(inner.ids(), span),
//             Constraint::IsSet(inner) => _all_required(inner.ids(), span),
//             Constraint::LinksDeduplication(_) => Box::new(iter::empty()),
//
//             Constraint::ExpressionBinding(binding) => {
//                 let arg_modes = binding.expression_ids().map(move |id| (id, VariableUsageMode::regular_input(span)));
//                 let assigned_modes =
//                     binding.ids_assigned().map(move |id| (id, VariableUsageMode::regular_binding(span)));
//                 Box::new(arg_modes.chain(assigned_modes))
//             }
//             Constraint::FunctionCallBinding(binding) => {
//                 let arg_modes =
//                     binding.function_call_arg_ids().map(move |id| (id, VariableUsageMode::regular_input(span)));
//                 let assigned_modes = binding
//                     .assigned_optionalities()
//                     .map(move |(id, optionality)| (id, VariableUsageMode::assigned(optionality, span)));
//                 Box::new(arg_modes.chain(assigned_modes))
//             }
//         }
//     }
//
//     pub(crate) fn absent() -> Self {
//         Self {
//             assignment: AssignmentStatus::NotAssigned,
//             optional_safety: OptionalReferenceSafety::AbsentOrSafe,
//             // optionality_status: OptionalityStatus::IntactInSomePaths,
//         }
//     }
//
//     pub(crate) fn regular_binding(location: LocationNote) -> Self {
//         Self {
//             assignment: AssignmentStatus::NotAssigned,
//             optional_safety: OptionalReferenceSafety::UnwrappingReference(location),
//             // optionality_status: OptionalityStatus::UnwrappedInAllPaths,
//         }
//     }
//
//     pub(crate) fn regular_input(location: LocationNote) -> Self {
//         Self {
//             assignment: AssignmentStatus::NotAssigned,
//             optional_safety: OptionalReferenceSafety::UnwrappingReference(location),
//             // optionality_status: OptionalityStatus::UnwrappedInAllPaths,
//         }
//     }
//
//     pub(crate) fn assigned(return_optionality: VariableOptionality, location: LocationNote) -> Self {
//         let optional_safety = match return_optionality {
//             // TODO: I had AbsentOrSafe down, but I think this is more right
//             VariableOptionality::Optional => OptionalReferenceSafety::OptionallyBindingOrStageInput(location),
//                 // OptionalityStatus::IntactInSomePaths),
//             VariableOptionality::Required => OptionalReferenceSafety::UnwrappingReference(location),
//                 // OptionalityStatus::UnwrappedInAllPaths)
//         };
//         Self {
//             assignment: AssignmentStatus::AtMostOncePerBranch(location),
//             optional_safety,
//             // optionality_status,
//         }
//     }
// }
//
// impl BitAnd for VariableUsageMode {
//     type Output = Self;
//
//     fn bitand(self, rhs: Self) -> Self {
//         Self {
//             assignment: self.assignment & rhs.assignment,
//             optional_safety: self.optional_safety & rhs.optional_safety,
//             // optionality_status: self.optionality_status & rhs.optionality_status,
//         }
//     }
// }
//
// impl BitOr for VariableUsageMode {
//     type Output = Self;
//
//     fn bitor(self, rhs: Self) -> Self {
//         Self {
//             assignment: self.assignment | rhs.assignment,
//             optional_safety: self.optional_safety | rhs.optional_safety,
//             // optionality_status: self.optionality_status | rhs.optionality_status,
//         }
//     }
// }
//
// impl BitAndAssign for VariableUsageMode {
//     fn bitand_assign(&mut self, rhs: Self) {
//         *self = *self & rhs;
//     }
// }
//
// impl BitOrAssign for VariableUsageMode {
//     fn bitor_assign(&mut self, rhs: Self) {
//         *self = *self | rhs;
//     }
// }
// pub(crate) type LocationNote = Option<Span>;
//
// #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
// pub(crate) enum AssignmentStatus {
//     #[default]
//     NotAssigned,
//     AtMostOncePerBranch(LocationNote),
//     ErrorMultipleAssignments(LocationNote, LocationNote),
// }
//
// impl BitAnd for AssignmentStatus {
//     type Output = Self;
//
//     fn bitand(self, rhs: Self) -> Self {
//         match (self, rhs) {
//             (Self::NotAssigned, x) | (x, Self::NotAssigned) => x,
//             (Self::ErrorMultipleAssignments(s1, s2), _) | (_, Self::ErrorMultipleAssignments(s1, s2)) => {
//                 Self::ErrorMultipleAssignments(s1, s2)
//             }
//             (Self::AtMostOncePerBranch(s1), Self::AtMostOncePerBranch(s2)) => Self::ErrorMultipleAssignments(s1, s2),
//         }
//     }
// }
//
// impl BitOr for AssignmentStatus {
//     type Output = Self;
//
//     fn bitor(self, rhs: Self) -> Self {
//         match (self, rhs) {
//             (Self::NotAssigned, x) | (x, Self::NotAssigned) => x,
//             (Self::ErrorMultipleAssignments(s1, s2), _) | (_, Self::ErrorMultipleAssignments(s1, s2)) => {
//                 Self::ErrorMultipleAssignments(s1, s2)
//             }
//             (Self::AtMostOncePerBranch(s), Self::AtMostOncePerBranch(_)) => Self::AtMostOncePerBranch(s),
//         }
//     }
// }
//
// #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
// pub(crate) enum OptionalReferenceSafety {
//     LocalIsSet, // There is a local isset. This must not be propagated upwards.
//
//     // We use this ONLY to check all references are "safe".
//     // The semantic issues of two try blocks assigning the same variable,
//     //      and actual variable optionality are computed by other `*Mode`s
//     UnsafeUnwrap(LocationNote),         // Can be reset by a safe unwrap in a parent
//     UnwrappingReference(LocationNote),  // match $x isa person;
//     OptionallyBindingOrStageInput(LocationNote), // Try, Assignment, Stage Input.
//
//     // TryBlockReference,      // Identical to regular reference?
//     #[default]
//     AbsentOrSafe, // When uninteresting
// }
//
// impl BitAnd for OptionalReferenceSafety {
//     type Output = Self;
//
//     fn bitand(self, rhs: Self) -> Self {
//         match (self, rhs) {
//             (Self::UnsafeUnwrap(location), _) | (_, Self::UnsafeUnwrap(location)) => Self::UnsafeUnwrap(location),
//             (Self::LocalIsSet, other) | (other, Self::LocalIsSet) => Self::LocalIsSet,
//             (Self::AbsentOrSafe, other) | (other, Self::AbsentOrSafe) => other,
//
//             (Self::OptionallyBindingOrStageInput(location), Self::OptionallyBindingOrStageInput(_)) => {
//                 Self::UnsafeUnwrap(location) // Should trigger the MultipleAssignments
//             }
//             (Self::UnwrappingReference(location), Self::OptionallyBindingOrStageInput(_))
//             | (Self::OptionallyBindingOrStageInput(_), Self::UnwrappingReference(location)) => Self::UnsafeUnwrap(location),
//             (Self::UnwrappingReference(location), Self::UnwrappingReference(_)) => Self::UnwrappingReference(location),
//         }
//     }
// }
//
// impl BitOr for OptionalReferenceSafety {
//     type Output = Self;
//
//     fn bitor(self, rhs: Self) -> Self {
//         match (self, rhs) {
//             (Self::UnsafeUnwrap(location), _) | (_, Self::UnsafeUnwrap(location)) => Self::UnsafeUnwrap(location),
//             (Self::LocalIsSet, other) | (other, Self::LocalIsSet) =>  {
//                 debug_assert!(false, "Should have been reset");
//                 other
//             }
//             (Self::AbsentOrSafe, other) | (other, Self::AbsentOrSafe) => other,
//
//             (Self::OptionallyBindingOrStageInput(location), Self::OptionallyBindingOrStageInput(_)) => {
//                 Self::OptionallyBindingOrStageInput(location) // Should trigger the MultipleAssignments
//             }
//             (Self::UnwrappingReference(location), Self::OptionallyBindingOrStageInput(_))
//             | (Self::OptionallyBindingOrStageInput(_), Self::UnwrappingReference(location)) => {
//                 Self::OptionallyBindingOrStageInput(location) // Should be illegal unsatisfiable input / double assignment
//             }
//             (Self::UnwrappingReference(location), Self::UnwrappingReference(_)) => Self::UnwrappingReference(location),
//         }
//     }
// }
// //
// // #[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
// // pub(crate) enum OptionalityStatus {
// //     UnwrappedInAllPaths,
// //     // TryBlockReference, // Identical to regular reference?
// //     #[default]
// //     IntactInSomePaths, // Can the optionality survive?
// // }
// //
// // // Is it true that the scope of Optionality is now the block?
// // impl BitAnd for OptionalityStatus {
// //     type Output = Self;
// //
// //     fn bitand(self, rhs: Self) -> Self {
// //         match (self, rhs) {
// //             (Self::UnwrappedInAllPaths, _) | (_, Self::UnwrappedInAllPaths) => Self::UnwrappedInAllPaths,
// //             (Self::IntactInSomePaths, Self::IntactInSomePaths) => Self::IntactInSomePaths,
// //         }
// //     }
// // }
// //
// // impl BitOr for OptionalityStatus {
// //     type Output = Self;
// //
// //     fn bitor(self, rhs: Self) -> Self {
// //         match (self, rhs) {
// //             (Self::IntactInSomePaths, _) | (_, Self::IntactInSomePaths) => Self::IntactInSomePaths,
// //             (Self::UnwrappedInAllPaths, Self::UnwrappedInAllPaths) => Self::UnwrappedInAllPaths,
// //         }
// //     }
// // }

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use error::{needs_update_when_feature_is_implemented, optional_usage_error};

use crate::pattern::{
    LocationNote, Pattern, conjunction::Conjunction, constraint::Constraint, disjunction::Disjunction,
    negation::Negation, nested_pattern::NestedPattern, optional::Optional, variable_category::VariableOptionality,
};

pub(crate) struct OptionalSafetyError {
    pub(crate) variable: Variable,
    pub(crate) optionality: LocationNote,
    pub(crate) unwrapping: LocationNote,
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct OptionalSafety {
    pub(crate) optionality: Option<LocationNote>,
    pub(crate) unwrapping: Option<LocationNote>,
}

impl OptionalSafety {
    // TODO: Make conjunction non_optional
    pub(crate) fn for_conjunction(
        conjunction: &mut Conjunction,
        tmp__fix_unwraps: bool,
    ) -> Result<HashMap<Variable, Self>, OptionalSafetyError> {
        let mut modes: HashMap<Variable, Self> = HashMap::new();
        for nested in conjunction.nested_patterns_mut().iter_mut() {
            let nested_modes = match nested {
                NestedPattern::Disjunction(disjunction) => Self::for_disjunction(disjunction, tmp__fix_unwraps),
                NestedPattern::Optional(optional) => Self::for_optional(optional, tmp__fix_unwraps),
                NestedPattern::Negation(negation) => Self::for_negation(negation, tmp__fix_unwraps),
            }?;
            for (id, mode) in nested_modes {
                modes.entry(id).or_default().may_update(mode)
            }
        }

        for constraint in conjunction.constraints() {
            if let Constraint::FunctionCallBinding(binding) = constraint {
                needs_update_when_feature_is_implemented!(error::UnimplementedFeature::OptionalArguments);
                for id in binding.function_call_arg_ids() {
                    modes.entry(id).or_default().unwrapping = Some(constraint.source_span());
                }
                for (id, _) in binding.assigned_optionalities().filter(|(_, o)| *o == VariableOptionality::Required) {
                    modes.entry(id).or_default().unwrapping = Some(constraint.source_span());
                }
            } else {
                for id in constraint.ids() {
                    modes.entry(id).or_default().unwrapping = Some(constraint.source_span());
                }
            }
        }

        let funcs = conjunction.constraints().iter().filter_map(|constraint| constraint.as_function_call_binding());
        let optional_assignments = funcs.flat_map(|f| {
            let optional_ids = f.assigned_optionalities().filter(|(id, o)| (*o == VariableOptionality::Optional));
            optional_ids.map(|(id, _)| (id, f.source_span()))
        });
        Self::update_with_new_optionals(&mut modes, optional_assignments);

        let is_sets = conjunction.constraints().iter().filter_map(|constraint| constraint.as_is_set());
        let reset_variables = is_sets.flat_map(|is_set| is_set.ids());
        Self::reset_unwrapped_variables(&mut modes, reset_variables);
        if tmp__fix_unwraps {
            Self::tmp__check_bad_unwraps_and_fix(conjunction, &mut modes)?;
            Self::tmp__inject_checks_for_inputs_and_reset_unwrap(conjunction, &mut modes);
        } else {
            Self::check_bad_unwraps(&modes)?;
        }
        Ok(modes)
    }

    fn for_disjunction(disjunction: &mut Disjunction, tmp__fix_unwraps: bool) -> Result<HashMap<Variable, Self>, OptionalSafetyError> {
        let mut modes: HashMap<Variable, Self> = HashMap::new();
        for branch in disjunction.conjunctions_mut() {
            for (id, branch_mode) in Self::for_conjunction(branch, tmp__fix_unwraps)? {
                modes.entry(id).or_default().may_update(branch_mode);
            }
        }
        Ok(modes)
    }

    fn for_optional(optional: &mut Optional, tmp__fix_unwraps: bool) -> Result<HashMap<Variable, Self>, OptionalSafetyError> {
        let mut modes = Self::for_conjunction(optional.conjunction_mut(), tmp__fix_unwraps)?;
        for id in optional.optionally_bound_by_pattern() {
            let entry = modes.entry(id).or_default();
            entry.optionality = entry.optionality.or(Some(optional.source_span())); // Prefer deeper optional
            entry.unwrapping = None;
        }
        Ok(modes)
    }

    fn for_negation(negation: &mut Negation, tmp__fix_unwraps: bool) -> Result<HashMap<Variable, Self>, OptionalSafetyError> {
        Self::for_conjunction(negation.conjunction_mut(), tmp__fix_unwraps)
    }

    fn may_update(&mut self, other: Self) {
        self.optionality = self.optionality.or(other.optionality);
        self.unwrapping = self.unwrapping.or(other.unwrapping);
    }

    pub(crate) fn update_with_new_optionals(
        modes: &mut HashMap<Variable, Self>,
        new_optionals: impl Iterator<Item = (Variable, LocationNote)>,
    ) {
        for (id, location) in new_optionals {
            modes.entry(id).or_default().optionality = Some(location);
        }
    }

    pub(crate) fn reset_unwrapped_variables(
        modes: &mut HashMap<Variable, Self>,
        new_optionals: impl Iterator<Item = Variable>,
    ) {
        for id in new_optionals {
            *modes.entry(id).or_default() = Self { optionality: None, unwrapping: None };
        }
    }

    pub(crate) fn tmp__check_bad_unwraps_and_fix(
        conjunction: &mut Conjunction,
        modes: &mut HashMap<Variable, OptionalSafety>,
    ) -> Result<(), OptionalSafetyError> {
        if let Err(_err) = Self::check_bad_unwraps(modes) {
            tmp__inject_issets(conjunction, modes);
        }
        Ok(())
    }

    pub(crate) fn check_bad_unwraps(modes: &HashMap<Variable, OptionalSafety>) -> Result<(), OptionalSafetyError> {
        for (&variable, mode) in modes {
            if let Self { optionality: Some(optionality), unwrapping: Some(unwrapping) } = mode {
                return Err(OptionalSafetyError {
                    variable,
                    optionality: optionality.clone(),
                    unwrapping: unwrapping.clone(),
                });
            }
        }
        Ok(())
    }

    fn tmp__inject_checks_for_inputs_and_reset_unwrap(
        optional_or_negation_inner: &mut Conjunction,
        modes: &mut HashMap<Variable, OptionalSafety>,
    ) {
        // Unwrapping inputs earlier than we should causes a change in behaviour, so we have to unwrap at the deepest level.
        // Temporarily, We have to reset the unwrap so we don't inject checks above.
        let mut unwrapped_input_variables = Vec::new();
        for id in optional_or_negation_inner.required_inputs() {
            let entry = modes.entry(id).or_default();
            if entry.unwrapping.is_some() {
                entry.unwrapping = None;
                unwrapped_input_variables.push(id);
            }
        }
        optional_or_negation_inner.tmp__inject_isset(unwrapped_input_variables);
    }
}

// May still be useful if we auto-unwrap in writes.
fn tmp__inject_issets(conjunction: &mut Conjunction, modes: &mut HashMap<Variable, OptionalSafety>) {
    let bad_unwraps = modes.iter_mut().filter(|(_, mode)| mode.optionality.is_some() && mode.unwrapping.is_some());
    let mut vars = Vec::new();
    for (&id, mode) in bad_unwraps {
        vars.push(id);
        mode.unwrapping = None;
        // Don't reset the optionality so we inject checks excessively. Only the first check will be needed.
        // This could also have been inside a try. Maybe I'm doing it wrong in the main one as well.
        // mode.optionality = None;
    }
    conjunction.tmp__inject_isset(vars);
}

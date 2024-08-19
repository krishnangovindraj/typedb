/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;

use answer::variable::Variable;
use encoding::graph::type_::Kind;
use ir::pattern::constraint::Constraint;

use crate::{delete::instructions::{DeleteInstruction, DeleteThing, Has, RolePlayer}, insert::{
    get_thing_source, insert::collect_role_type_bindings, ThingSource, TypeSource, VariableSource,
    WriteCompilationError,
}, match_::inference::type_annotations::TypeAnnotations, VariablePosition};

pub struct DeletePlan {
    pub instructions: Vec<DeleteInstruction>,
    pub output_row_plan: Vec<(Variable, VariableSource)>,
    // pub debug_info: HashMap<VariableSource, Variable>,
}

pub fn build_delete_plan(
    input_variables: &HashMap<Variable, VariablePosition>,
    type_annotations: &TypeAnnotations,
    constraints: &[Constraint<Variable>],
    deleted_concepts: &[Variable],
) -> Result<DeletePlan, WriteCompilationError> {
    // TODO: Maybe unify all WriteCompilation errors?
    let named_role_types = collect_role_type_bindings(constraints, type_annotations)?;
    let mut instructions = Vec::new();
    for constraint in constraints {
        match constraint {
            Constraint::Has(has) => {
                instructions.push(DeleteInstruction::Has(Has {
                    owner: get_thing_source(input_variables, has.owner())?,
                    attribute: get_thing_source(input_variables, has.attribute())?,
                }));
            }
            Constraint::Links(role_player) => {
                let relation = get_thing_source(input_variables, role_player.relation())?;
                let player = get_thing_source(input_variables, role_player.player())?;
                let role_variable = role_player.role_type();
                let role = match (input_variables.get(&role_variable), named_role_types.get(&role_variable)) {
                    (Some(input), None) => TypeSource::InputVariable(input.clone()),
                    (None, Some(type_)) => TypeSource::TypeConstant(type_.clone()),
                    (None, None) => {
                        let annotations = type_annotations.variable_annotations_of(role_variable).unwrap();
                        if annotations.len() == 1 {
                            TypeSource::TypeConstant(annotations.iter().find(|_| true).unwrap().clone())
                        } else {
                            return Err(WriteCompilationError::CouldNotUniquelyDetermineRoleType {
                                variable: role_variable.clone(),
                            })?;
                        }
                    }
                    (Some(_), Some(_)) => unreachable!(),
                };
                instructions.push(DeleteInstruction::RolePlayer(RolePlayer { relation, player, role }));
            }
            Constraint::Isa(_)
            | Constraint::Label(_)
            | Constraint::RoleName(_)
            | Constraint::Owns(_)
            | Constraint::Relates(_)
            | Constraint::Plays(_)
            | Constraint::ExpressionBinding(_)
            | Constraint::Comparison(_)
            | Constraint::Sub(_)
            | Constraint::FunctionCallBinding(_) => {
                unreachable!()
            }
        }
    }
    for variable in deleted_concepts {
        let Some(thing) = input_variables.get(variable) else {
            return Err(WriteCompilationError::DeletedThingWasNotInInput { variable: variable.clone() });
        };
        if type_annotations
            .variable_annotations_of(variable.clone())
            .unwrap()
            .iter()
            .any(|type_| type_.kind() == Kind::Role)
        {
            Err(WriteCompilationError::IllegalRoleDelete { variable: variable.clone() })?;
        } else {
            instructions.push(DeleteInstruction::Thing(DeleteThing { thing: ThingSource(thing.clone()) }));
        };
    }
    // To produce the output stream, we remove the deleted concepts from each map in the stream.
    let output_row = input_variables
        .iter()
        .filter_map(|(variable, position)| {
            if deleted_concepts.contains(variable) {
                None
            } else {
                Some((variable.clone(), VariableSource::InputVariable(position.clone())))
            }
        })
        .collect::<Vec<_>>();

    Ok(DeletePlan { instructions, output_row_plan: output_row })
}

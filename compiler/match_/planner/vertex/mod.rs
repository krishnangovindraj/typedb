/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, ops::Deref};

use answer::variable::Variable;
use concept::thing::statistics::Statistics;
use ir::pattern::constraint::{Has, Links};
use itertools::Itertools;

use crate::match_::inference::type_annotations::TypeAnnotations;

const OPEN_ITERATOR_RELATIVE_COST: f64 = 5.0;
const ADVANCE_ITERATOR_RELATIVE_COST: f64 = 1.0;

// FIXME name
#[derive(Debug)]
pub(super) enum PlannerVertex {
    Thing(ThingPlanner),
    Has(HasPlanner),
    Links(LinksPlanner),
}

impl PlannerVertex {
    fn as_thing(&self) -> Option<&ThingPlanner> {
        match self {
            Self::Thing(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn as_has(&self) -> Option<&HasPlanner> {
        match self {
            Self::Has(v) => Some(v),
            _ => None,
        }
    }

    pub(super) fn as_links(&self) -> Option<&LinksPlanner> {
        match self {
            Self::Links(v) => Some(v),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub(super) struct VertexCost {
    pub per_input: f64,
    pub per_output: f64,
    pub branching_factor: f64,
}

pub(super) trait Costed {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> VertexCost;
}

// TODO delegate
impl Costed for PlannerVertex {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> VertexCost {
        match self {
            Self::Thing(inner) => inner.cost(inputs, elements),
            Self::Has(inner) => inner.cost(inputs, elements),
            Self::Links(inner) => inner.cost(inputs, elements),
        }
    }
}

#[derive(Debug)]
pub(super) struct ThingPlanner {
    expected_size: f64,
}

impl ThingPlanner {
    pub(super) fn from_variable(
        variable: Variable,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let expected_size = type_annotations
            .variable_annotations_of(variable)
            .expect("expected thing variable to have been annotated with types")
            .iter()
            .filter_map(|type_| match type_ {
                answer::Type::Entity(type_) => statistics.entity_counts.get(type_),
                answer::Type::Relation(type_) => statistics.relation_counts.get(type_),
                answer::Type::Attribute(type_) => statistics.attribute_counts.get(type_),
                answer::Type::RoleType(type_) => {
                    panic!("Found a Thing variable `{variable}` with a Role Type annotation: {type_}")
                }
            })
            .sum::<u64>() as f64;
        Self { expected_size }
    }
}

impl Costed for ThingPlanner {
    fn cost(&self, inputs: &[usize], _elements: &[PlannerVertex]) -> VertexCost {
        if inputs.is_empty() {
            VertexCost {
                per_input: OPEN_ITERATOR_RELATIVE_COST,
                per_output: ADVANCE_ITERATOR_RELATIVE_COST,
                branching_factor: self.expected_size,
            }
        } else {
            VertexCost {
                per_input: 0.0,
                per_output: 0.0,
                branching_factor: 1.0, // assumes deconstruction; TODO consider intersection
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct HasPlanner {
    pub owner: usize,
    pub attribute: usize,
    expected_size: f64,
    expected_unbound_size: f64,
    pub unbound_is_forward: bool, //FIXME
}

impl HasPlanner {
    pub(super) fn from_constraint(
        constraint: &Has<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let owner = constraint.owner();
        let attribute = constraint.attribute();

        let owner_types = type_annotations.variable_annotations_of(owner).unwrap().deref();
        let attribute_types = type_annotations.variable_annotations_of(attribute).unwrap().deref();

        let expected_size = itertools::iproduct!(owner_types, attribute_types)
            .filter_map(|(owner, attribute)| {
                statistics.has_attribute_counts.get(&owner.as_object_type())?.get(&attribute.as_attribute_type())
            })
            .sum::<u64>() as f64;

        let unbound_forward_size = owner_types
            .iter()
            .filter_map(|owner| statistics.has_attribute_counts.get(&owner.as_object_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        let unbound_backward_size = attribute_types
            .iter()
            .filter_map(|attribute| statistics.attribute_owner_counts.get(&attribute.as_attribute_type()))
            .flat_map(|counts| counts.values())
            .sum::<u64>() as f64;

        let expected_unbound_size = f64::min(unbound_forward_size, unbound_backward_size);
        let unbound_is_forward = unbound_forward_size <= unbound_backward_size;

        Self {
            owner: variable_index[&owner],
            attribute: variable_index[&attribute],
            expected_size,
            expected_unbound_size,
            unbound_is_forward,
        }
    }
}

impl Costed for HasPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> VertexCost {
        let is_owner_bound = inputs.contains(&self.owner);
        let is_attribute_bound = inputs.contains(&self.attribute);

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_owner_bound, is_attribute_bound) {
            (true, true) => 0.0,
            (false, false) => ADVANCE_ITERATOR_RELATIVE_COST * self.expected_unbound_size / self.expected_size,
            (true, false) | (false, true) => ADVANCE_ITERATOR_RELATIVE_COST,
        };

        let owner = elements[self.owner].as_thing().unwrap();
        let attribute = elements[self.attribute].as_thing().unwrap();

        let branching_factor = match (is_owner_bound, is_attribute_bound) {
            (true, true) => self.expected_size / owner.expected_size / attribute.expected_size,
            (true, false) => self.expected_size / owner.expected_size,
            (false, true) => self.expected_size / attribute.expected_size,
            (false, false) => self.expected_size,
        };

        VertexCost { per_input, per_output, branching_factor }
    }
}

#[derive(Debug)]
pub(super) struct LinksPlanner {
    pub relation: usize,
    pub player: usize,
    pub role: usize,
    expected_size: f64,
    expected_unbound_size: f64,
    pub unbound_is_forward: bool, //FIXME
}

impl LinksPlanner {
    pub(super) fn from_constraint(
        links: &Links<Variable>,
        variable_index: &HashMap<Variable, usize>,
        type_annotations: &TypeAnnotations,
        statistics: &Statistics,
    ) -> Self {
        let relation = links.relation();
        let player = links.player();
        let role = links.role_type();

        let relation_types = type_annotations.variable_annotations_of(relation).unwrap().deref();
        let player_types = type_annotations.variable_annotations_of(player).unwrap().deref();

        let constraint_types =
            type_annotations.constraint_annotations_of(links.clone().into()).unwrap().as_left_right_filtered();

        let expected_size = constraint_types
            .filters_on_left()
            .iter()
            .flat_map(|(relation, roles)| {
                roles.iter().cartesian_product(player_types).flat_map(|(role, player)| {
                    statistics
                        .relation_role_player_counts
                        .get(&relation.as_relation_type())?
                        .get(&role.as_role_type())?
                        .get(&player.as_object_type())
                })
            })
            .sum::<u64>() as f64;

        let unbound_forward_size = relation_types
            .iter()
            .filter_map(|relation| {
                Some(statistics.relation_role_player_counts.get(&relation.as_relation_type())?.values().flat_map(
                    |player_to_count| {
                        player_types.iter().filter_map(|player| player_to_count.get(&player.as_object_type()))
                    },
                ))
            })
            .flatten()
            .sum::<u64>() as f64;

        let unbound_backward_size = player_types
            .iter()
            .filter_map(|player| {
                Some(statistics.player_role_relation_counts.get(&player.as_object_type())?.values().flat_map(
                    |relation_to_count| {
                        relation_types.iter().filter_map(|relation| relation_to_count.get(&relation.as_relation_type()))
                    },
                ))
            })
            .flatten()
            .sum::<u64>() as f64;

        let expected_unbound_size = f64::min(unbound_forward_size, unbound_backward_size);
        let unbound_is_forward = unbound_forward_size <= unbound_backward_size;

        Self {
            relation: variable_index[&relation],
            player: variable_index[&player],
            role: variable_index[&role],
            expected_size,
            expected_unbound_size,
            unbound_is_forward,
        }
    }
}

impl Costed for LinksPlanner {
    fn cost(&self, inputs: &[usize], elements: &[PlannerVertex]) -> VertexCost {
        let is_relation_bound = inputs.contains(&self.relation);
        let is_player_bound = inputs.contains(&self.player);

        let per_input = OPEN_ITERATOR_RELATIVE_COST;

        let per_output = match (is_relation_bound, is_player_bound) {
            (true, true) => 0.0,
            (false, false) => ADVANCE_ITERATOR_RELATIVE_COST * self.expected_unbound_size / self.expected_size,
            (true, false) | (false, true) => ADVANCE_ITERATOR_RELATIVE_COST,
        };

        let relation = elements[self.relation].as_thing().unwrap();
        let player = elements[self.player].as_thing().unwrap();

        let branching_factor = match (is_relation_bound, is_player_bound) {
            (true, true) => self.expected_size / relation.expected_size / player.expected_size,
            (true, false) => self.expected_size / relation.expected_size,
            (false, true) => self.expected_size / player.expected_size,
            (false, false) => self.expected_size,
        };

        VertexCost { per_input, per_output, branching_factor }
    }
}
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{Bound, HashMap},
    marker::PhantomData,
    sync::Arc,
};

use answer::{Thing, Type, variable_value::VariableValue};
use bytes::byte_array::ByteArray;
use compiler::{
    ExecutorVariable,
    executable::match_::instructions::{CheckInstruction, CheckVertex},
};
use concept::{
    error::ConceptReadError,
    thing::{ThingAPI, object::ObjectAPI, thing_manager::ThingManager},
    type_::{OwnerAPI, PlayerAPI},
};
use encoding::{
    AsBytes,
    graph::thing::THING_VERTEX_MAX_LENGTH,
    value::{ValueEncodable, value::Value},
};
use error::unimplemented_feature;
use ir::{
    pattern::constraint::{Comparator, IsaKind, SubKind},
    pipeline::ParameterRegistry,
};
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;
use unicase::UniCase;

use crate::{instruction::FilterFn, pipeline::stage::ExecutionContext, row::MaybeOwnedRow};

#[derive(Debug)]
pub(crate) struct Checker<T: 'static> {
    extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    pub checks: Vec<CheckInstruction<ExecutorVariable>>,
    _phantom_data: PhantomData<T>,
}

type BoxExtractor<T> = Box<dyn for<'a> Fn(&'a T) -> VariableValue<'a>>;

macro_rules! unwrap_or_result_false {
    ($value:expr => $variant:ident) => {{
        let VariableValue::$variant(x) = $value else { return Ok(false) };
        x
    }};
}

macro_rules! unwrap_or_return_false {
    ($value:expr => $variant:ident) => {{
        let VariableValue::$variant(x) = $value else { return false };
        x
    }};
}

impl<T> Checker<T> {
    pub(crate) fn new(
        checks: Vec<CheckInstruction<ExecutorVariable>>,
        extractors: HashMap<ExecutorVariable, fn(&T) -> VariableValue<'_>>,
    ) -> Self {
        Self { extractors, checks, _phantom_data: PhantomData }
    }

    pub(crate) fn value_range_for(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: Option<MaybeOwnedRow<'_>>,
        target_variable: ExecutorVariable,
        storage_counters: StorageCounters,
    ) -> Result<(Bound<Value<'_>>, Bound<Value<'_>>), Box<ConceptReadError>> {
        fn intersect<'a>(
            (a_min, a_max): (Bound<Value<'a>>, Bound<Value<'a>>),
            (b_min, b_max): (Bound<Value<'a>>, Bound<Value<'a>>),
        ) -> (Bound<Value<'a>>, Bound<Value<'a>>) {
            let select_a_min = match (&a_min, &b_min) {
                (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Included(b)) => a >= b,
                (Bound::Excluded(a), Bound::Excluded(b)) => a >= b,
                (Bound::Included(a), Bound::Included(b)) => a >= b,
                (Bound::Included(a), Bound::Excluded(b)) => a > b,
                _ => false,
            };
            let select_a_max = match (&a_max, &b_max) {
                (_, Bound::Unbounded) => true,
                (Bound::Excluded(a), Bound::Included(b)) => a <= b,
                (Bound::Excluded(a), Bound::Excluded(b)) => a <= b,
                (Bound::Included(a), Bound::Included(b)) => a <= b,
                (Bound::Included(a), Bound::Excluded(b)) => a < b,
                _ => false,
            };
            (if select_a_min { a_min } else { b_min }, if select_a_max { a_max } else { b_max })
        }

        let mut range = (Bound::Unbounded, Bound::Unbounded);
        for i in 0..self.checks.len() {
            let check = &self.checks[i];
            match check {
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    if lhs.as_variable() == Some(target_variable) {
                        let rhs_variable_value = get_vertex_value(rhs, row.as_ref(), &context.parameters);
                        let rhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &rhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(rhs_value) = rhs_value {
                            let comp_range = match comparator {
                                Comparator::Equal => (Bound::Included(rhs_value.clone()), Bound::Included(rhs_value)),
                                Comparator::Less => (Bound::Unbounded, Bound::Excluded(rhs_value)),
                                Comparator::LessOrEqual => (Bound::Unbounded, Bound::Included(rhs_value)),
                                Comparator::Greater => (Bound::Excluded(rhs_value), Bound::Unbounded),
                                Comparator::GreaterOrEqual => (Bound::Included(rhs_value), Bound::Unbounded),
                                Comparator::Like => continue,
                                Comparator::Contains => continue,
                                Comparator::NotEqual => continue,
                            };
                            range = intersect(range, comp_range);
                        }
                    } else {
                        debug_assert!(
                            rhs.as_variable().expect("RHS of comparison must be a variable") == target_variable
                        );
                        let lhs_variable_value = get_vertex_value(lhs, row.as_ref(), &context.parameters);
                        let lhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &lhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(lhs_value) = lhs_value {
                            let comp_range = match comparator {
                                Comparator::Equal => (Bound::Included(lhs_value.clone()), Bound::Included(lhs_value)),
                                Comparator::Less => (Bound::Excluded(lhs_value), Bound::Unbounded),
                                Comparator::LessOrEqual => (Bound::Included(lhs_value), Bound::Unbounded),
                                Comparator::Greater => (Bound::Unbounded, Bound::Excluded(lhs_value)),
                                Comparator::GreaterOrEqual => (Bound::Unbounded, Bound::Included(lhs_value)),
                                Comparator::Like => continue,
                                Comparator::Contains => continue,
                                Comparator::NotEqual => continue,
                            };
                            range = intersect(range, comp_range);
                        }
                    }
                }
                CheckInstruction::Is { lhs, rhs } => {
                    if *lhs == target_variable {
                        let rhs_as_vertex = CheckVertex::Variable(*rhs);
                        let rhs_variable_value = get_vertex_value(&rhs_as_vertex, row.as_ref(), &context.parameters);
                        let rhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &rhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(rhs_value) = rhs_value {
                            let comp_range = (Bound::Included(rhs_value.clone()), Bound::Included(rhs_value));
                            range = intersect(range, comp_range);
                        }
                    } else {
                        let lhs_as_vertex = CheckVertex::Variable(*lhs);
                        let lhs_variable_value = get_vertex_value(&lhs_as_vertex, row.as_ref(), &context.parameters);
                        let lhs_value = Self::read_value(
                            context.snapshot.as_ref(),
                            &context.thing_manager,
                            &lhs_variable_value,
                            storage_counters.clone(),
                        )?;
                        if let Some(lhs_value) = lhs_value {
                            let comp_range = (Bound::Included(lhs_value.clone()), Bound::Included(lhs_value));
                            range = intersect(range, comp_range);
                        }
                    }
                }
                _ => (),
            }
        }
        let range = (range.0.map(|value| value.into_owned()), range.1.map(|value| value.into_owned()));
        Ok(range)
    }

    fn read_value<'a>(
        snapshot: &'a impl ReadableSnapshot,
        thing_manager: &'a ThingManager,
        variable_value: &'a VariableValue<'_>,
        storage_counters: StorageCounters,
    ) -> Result<Option<Value<'static>>, Box<ConceptReadError>> {
        // TODO: is there a way to do this without cloning the value?
        match variable_value {
            VariableValue::Thing(Thing::Attribute(attribute)) => {
                let value = attribute.get_value(snapshot, thing_manager, storage_counters)?;
                Ok(Some(value.into_owned()))
            }
            VariableValue::Value(value) => {
                let value = value.as_reference();
                Ok(Some(value.into_owned()))
            }
            _ => Ok(None),
        }
    }

    fn make_extractor_new(
        &self,
        variable: ExecutorVariable,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> ExtractorOrExtractedVariable<T> {
        match self.extractors.get(&variable) {
            None => {
                let value = get_variable_value(Some(row), &variable);
                let owned_value = value.into_owned();
                ExtractorOrExtractedVariable::Extracted(owned_value)
            }
            Some(&tuple_extractor) => ExtractorOrExtractedVariable::ExtractFromTuple(tuple_extractor),
        }
    }

    pub(crate) fn filter_fn_for_row(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Box<FilterFn<T>> {
        let mut filters: Vec<CheckInstruction<ExtractorOrExtractedVariable<T>>> = Vec::with_capacity(self.checks.len());

        for check in &self.checks {
            let filter = match check {
                CheckInstruction::Iid { var, iid } => self.filter_iid_fn(context, row, *var, iid),
                &CheckInstruction::TypeList { type_var, ref types } => {
                    self.filter_type_list_fn(context, row, type_var, types)
                }
                &CheckInstruction::ThingTypeList { thing_var, ref types } => {
                    self.filter_thing_type_list_fn(context, row, thing_var, types)
                }
                &CheckInstruction::Sub { sub_kind, ref subtype, ref supertype } => {
                    self.filter_sub_fn(context, row, sub_kind, subtype, supertype)
                }
                CheckInstruction::Owns { owner, attribute } => self.filter_owns_fn(context, row, owner, attribute),
                CheckInstruction::Relates { relation, role_type } => {
                    self.filter_relates_fn(context, row, relation, role_type)
                }
                CheckInstruction::Plays { player, role_type } => self.filter_plays_fn(context, row, player, role_type),
                &CheckInstruction::Isa { isa_kind, ref type_, ref thing } => {
                    self.filter_isa_fn(context, row, isa_kind, type_, thing)
                }
                CheckInstruction::Has { owner, attribute } => {
                    self.filter_has_fn(context, row, owner, attribute, storage_counters.clone())
                }
                CheckInstruction::Links { relation, player, role } => {
                    self.filter_links_fn(context, row, relation, player, role, storage_counters.clone())
                }
                CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => self
                    .filter_indexed_relation_fn(
                        context,
                        row,
                        start_player,
                        end_player,
                        relation,
                        start_role,
                        end_role,
                        storage_counters.clone(),
                    ),
                &CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                    self.filter_links_dedup_fn(context, row, role1, player1, role2, player2)
                }
                CheckInstruction::NotNone { variables } => self.filter_not_none_fn(context, row, variables),
                &CheckInstruction::Is { lhs, rhs } => self.filter_is_fn(context, row, lhs, rhs),
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    self.filter_comparison_fn(context, row, lhs, rhs, *comparator, storage_counters.clone())
                }
                CheckInstruction::Unsatisfiable => CheckInstruction::Unsatisfiable,
            };
            filters.push(filter);
        }
        let context = context.clone();
        Box::new(move |res| {
            let Ok(value) = res else { return Ok(true) };
            Self::filter(&filters, &context, value, storage_counters.clone())
        })
    }

    fn filter_iid_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        var: ExecutorVariable,
        iid: &ir::pattern::ParameterID,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let var = self.make_extractor_new(var, row, context);
        CheckInstruction::Iid { var, iid: iid.clone() }
    }

    fn resolve_vertex(
        &self,
        vertex: &CheckVertex<ExecutorVariable>,
        row: &MaybeOwnedRow<'_>,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    ) -> CheckVertex<ExtractorOrExtractedVariable<T>> {
        match vertex {
            CheckVertex::Variable(var) => CheckVertex::Variable(self.make_extractor_new(*var, row, context)),
            CheckVertex::Type(t) => CheckVertex::Type(*t),
            CheckVertex::Parameter(p) => CheckVertex::Parameter(p.clone()),
        }
    }

    fn filter_type_list_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        type_var: ExecutorVariable,
        types: &Arc<std::collections::BTreeSet<Type>>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let type_var = self.make_extractor_new(type_var, row, context);
        CheckInstruction::TypeList { type_var, types: types.clone() }
    }

    fn filter_thing_type_list_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        thing_var: ExecutorVariable,
        types: &Arc<std::collections::BTreeSet<Type>>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let thing_var = self.make_extractor_new(thing_var, row, context);
        CheckInstruction::ThingTypeList { thing_var, types: types.clone() }
    }

    fn filter_sub_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        sub_kind: SubKind,
        subtype: &CheckVertex<ExecutorVariable>,
        supertype: &CheckVertex<ExecutorVariable>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let subtype = self.resolve_vertex(subtype, row, context);
        let supertype = self.resolve_vertex(supertype, row, context);
        CheckInstruction::Sub { sub_kind, subtype, supertype }
    }

    fn filter_owns_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let owner = self.resolve_vertex(owner, row, context);
        let attribute = self.resolve_vertex(attribute, row, context);
        CheckInstruction::Owns { owner, attribute }
    }

    fn filter_relates_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        relation: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let relation = self.resolve_vertex(relation, row, context);
        let role_type = self.resolve_vertex(role_type, row, context);
        CheckInstruction::Relates { relation, role_type }
    }

    fn filter_plays_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        player: &CheckVertex<ExecutorVariable>,
        role_type: &CheckVertex<ExecutorVariable>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let player = self.resolve_vertex(player, row, context);
        let role_type = self.resolve_vertex(role_type, row, context);
        CheckInstruction::Plays { player, role_type }
    }

    fn filter_isa_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        isa_kind: IsaKind,
        type_: &CheckVertex<ExecutorVariable>,
        thing: &CheckVertex<ExecutorVariable>,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let type_ = self.resolve_vertex(type_, row, context);
        let thing = self.resolve_vertex(thing, row, context);
        CheckInstruction::Isa { isa_kind, type_, thing }
    }

    fn filter_has_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        owner: &CheckVertex<ExecutorVariable>,
        attribute: &CheckVertex<ExecutorVariable>,
        _storage_counters: StorageCounters,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let owner = self.resolve_vertex(owner, row, context);
        let attribute = self.resolve_vertex(attribute, row, context);
        CheckInstruction::Has { owner, attribute }
    }

    fn filter_links_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        relation: &CheckVertex<ExecutorVariable>,
        player: &CheckVertex<ExecutorVariable>,
        role: &CheckVertex<ExecutorVariable>,
        _storage_counters: StorageCounters,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let relation = self.resolve_vertex(relation, row, context);
        let player = self.resolve_vertex(player, row, context);
        let role = self.resolve_vertex(role, row, context);
        CheckInstruction::Links { relation, player, role }
    }

    fn filter_indexed_relation_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        start_player: &CheckVertex<ExecutorVariable>,
        end_player: &CheckVertex<ExecutorVariable>,
        relation: &CheckVertex<ExecutorVariable>,
        start_role: &CheckVertex<ExecutorVariable>,
        end_role: &CheckVertex<ExecutorVariable>,
        _storage_counters: StorageCounters,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let start_player = self.resolve_vertex(start_player, row, context);
        let end_player = self.resolve_vertex(end_player, row, context);
        let relation = self.resolve_vertex(relation, row, context);
        let start_role = self.resolve_vertex(start_role, row, context);
        let end_role = self.resolve_vertex(end_role, row, context);
        CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role }
    }

    fn filter_is_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        lhs: ExecutorVariable,
        rhs: ExecutorVariable,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let lhs = self.make_extractor_new(lhs, row, context);
        let rhs = self.make_extractor_new(rhs, row, context);
        CheckInstruction::Is { lhs, rhs }
    }

    fn filter_links_dedup_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        role1: ExecutorVariable,
        player1: ExecutorVariable,
        role2: ExecutorVariable,
        player2: ExecutorVariable,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let role1 = self.make_extractor_new(role1, row, context);
        let player1 = self.make_extractor_new(player1, row, context);
        let role2 = self.make_extractor_new(role2, row, context);
        let player2 = self.make_extractor_new(player2, row, context);
        CheckInstruction::LinksDeduplication { role1, player1, role2, player2 }
    }

    fn filter_not_none_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        variables: &[ExecutorVariable],
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let variables = variables.iter().map(|var| self.make_extractor_new(*var, row, context)).collect();
        CheckInstruction::NotNone { variables }
    }

    fn filter_comparison_fn(
        &self,
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        lhs: &CheckVertex<ExecutorVariable>,
        rhs: &CheckVertex<ExecutorVariable>,
        comparator: Comparator,
        _storage_counters: StorageCounters,
    ) -> CheckInstruction<ExtractorOrExtractedVariable<T>> {
        let lhs = self.resolve_vertex(lhs, row, context);
        let rhs = self.resolve_vertex(rhs, row, context);
        CheckInstruction::Comparison { lhs, rhs, comparator }
    }
}

impl Checker<()> {
    pub(crate) fn filter_for_row(
        checks: &[CheckInstruction<ExecutorVariable>],
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &MaybeOwnedRow<'_>,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        for check in checks {
            let passes = match check {
                CheckInstruction::Iid { var, iid } => filter_iid(context, row, var, iid),
                CheckInstruction::TypeList { type_var, types } => filter_type_list(context, row, type_var, types),
                CheckInstruction::ThingTypeList { thing_var, types } => {
                    filter_thing_type_list(context, row, thing_var, types)
                }
                CheckInstruction::Sub { sub_kind, subtype, supertype } => {
                    filter_sub(context, row, *sub_kind, subtype, supertype)?
                }
                CheckInstruction::Owns { owner, attribute } => filter_owns(context, row, owner, attribute)?,
                CheckInstruction::Relates { relation, role_type } => filter_relates(context, row, relation, role_type)?,
                CheckInstruction::Plays { player, role_type } => filter_plays(context, row, player, role_type)?,
                CheckInstruction::Isa { isa_kind, type_, thing } => filter_isa(context, row, *isa_kind, type_, thing)?,
                CheckInstruction::Has { owner, attribute } => {
                    filter_has(context, row, owner, attribute, storage_counters.clone())?
                }
                CheckInstruction::Links { relation, player, role } => {
                    filter_links(context, row, relation, player, role, storage_counters.clone())?
                }
                CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => {
                    filter_indexed_relation(
                        context,
                        row,
                        start_player,
                        end_player,
                        relation,
                        start_role,
                        end_role,
                        storage_counters.clone(),
                    )?
                }
                CheckInstruction::Is { lhs, rhs } => filter_is(row, lhs, rhs),
                CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                    filter_links_dedup(context, row, role1, player1, role2, player2)
                }
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    filter_comparison(context, row, lhs, rhs, *comparator, storage_counters.clone())?
                }
                CheckInstruction::NotNone { variables } => filter_not_none(row, variables),
                CheckInstruction::Unsatisfiable => false,
            };
            if !passes {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl<T> Checker<T> {
    pub(crate) fn filter<V: ExtractFrom<T>>(
        checks: &[CheckInstruction<V>],
        context: &ExecutionContext<impl ReadableSnapshot + 'static>,
        row: &T,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        for check in checks {
            let passes = match check {
                CheckInstruction::Iid { var, iid } => filter_iid(context, row, var, iid),
                CheckInstruction::TypeList { type_var, types } => filter_type_list(context, row, type_var, types),
                CheckInstruction::ThingTypeList { thing_var, types } => {
                    filter_thing_type_list(context, row, thing_var, types)
                }
                CheckInstruction::Sub { sub_kind, subtype, supertype } => {
                    filter_sub(context, row, *sub_kind, subtype, supertype)?
                }
                CheckInstruction::Owns { owner, attribute } => filter_owns(context, row, owner, attribute)?,
                CheckInstruction::Relates { relation, role_type } => filter_relates(context, row, relation, role_type)?,
                CheckInstruction::Plays { player, role_type } => filter_plays(context, row, player, role_type)?,
                CheckInstruction::Isa { isa_kind, type_, thing } => filter_isa(context, row, *isa_kind, type_, thing)?,
                CheckInstruction::Has { owner, attribute } => {
                    filter_has(context, row, owner, attribute, storage_counters.clone())?
                }
                CheckInstruction::Links { relation, player, role } => {
                    filter_links(context, row, relation, player, role, storage_counters.clone())?
                }
                CheckInstruction::IndexedRelation { start_player, end_player, relation, start_role, end_role } => {
                    filter_indexed_relation(
                        context,
                        row,
                        start_player,
                        end_player,
                        relation,
                        start_role,
                        end_role,
                        storage_counters.clone(),
                    )?
                }
                CheckInstruction::Is { lhs, rhs } => filter_is(row, lhs, rhs),
                CheckInstruction::LinksDeduplication { role1, player1, role2, player2 } => {
                    filter_links_dedup(context, row, role1, player1, role2, player2)
                }
                CheckInstruction::Comparison { lhs, rhs, comparator } => {
                    filter_comparison(context, row, lhs, rhs, *comparator, storage_counters.clone())?
                }
                CheckInstruction::NotNone { variables } => filter_not_none(row, variables),
                CheckInstruction::Unsatisfiable => false,
            };
            if !passes {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn filter_iid<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    var: &V,
    iid: &ir::pattern::ParameterID,
) -> bool {
    let extracted = var.extract(row);
    let iid = context.parameters().iid(iid).unwrap();
    check_iid(iid, extracted)
}

fn filter_type_list<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    type_var: &V,
    types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
) -> bool {
    let extracted = type_var.extract(row);
    let VariableValue::Type(t) = extracted else { return false };
    types.contains(&t)
}

fn filter_thing_type_list<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    thing_var: &V,
    types: &std::sync::Arc<std::collections::BTreeSet<Type>>,
) -> bool {
    let extracted = thing_var.extract(row);
    let VariableValue::Thing(thing) = extracted else { return false };
    types.contains(&thing.type_())
}

fn filter_sub<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    sub_kind: SubKind,
    subtype: &CheckVertex<V>,
    supertype: &CheckVertex<V>,
) -> Result<bool, Box<ConceptReadError>> {
    let subtype = V::extract_vertex(subtype, row, &context.parameters);
    let supertype = V::extract_vertex(supertype, row, &context.parameters);
    check_sub(
        context.snapshot.as_ref(),
        context.thing_manager.as_ref(),
        sub_kind,
        unwrap_or_result_false!(subtype => Type),
        unwrap_or_result_false!(supertype => Type),
    )
}

fn filter_owns<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    owner: &CheckVertex<V>,
    attribute: &CheckVertex<V>,
) -> Result<bool, Box<ConceptReadError>> {
    let owner = V::extract_vertex(owner, row, &context.parameters);
    let attribute = V::extract_vertex(attribute, row, &context.parameters);
    let owner = unwrap_or_result_false!(owner => Type).as_object_type();
    let attribute = unwrap_or_result_false!(attribute => Type).as_attribute_type();
    owner
        .get_owns_attribute(context.snapshot.as_ref(), context.thing_manager.clone().type_manager(), attribute)
        .map(|owns| owns.is_some())
}

fn filter_relates<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    relation: &CheckVertex<V>,
    role_type: &CheckVertex<V>,
) -> Result<bool, Box<ConceptReadError>> {
    let relation = V::extract_vertex(relation, row, &context.parameters);
    let role_type = V::extract_vertex(role_type, row, &context.parameters);
    let relation_type = unwrap_or_result_false!(relation => Type).as_relation_type();
    let role_type = unwrap_or_result_false!(role_type => Type).as_role_type();
    relation_type
        .get_relates_role(context.snapshot.as_ref(), context.thing_manager.type_manager(), role_type)
        .map(|relates| relates.is_some())
}

fn filter_plays<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    player: &CheckVertex<V>,
    role_type: &CheckVertex<V>,
) -> Result<bool, Box<ConceptReadError>> {
    let player = V::extract_vertex(player, row, &context.parameters);
    let role_type = V::extract_vertex(role_type, row, &context.parameters);
    let object_type = unwrap_or_result_false!(player => Type).as_object_type();
    let role_type = unwrap_or_result_false!(role_type => Type).as_role_type();
    object_type
        .get_plays_role(context.snapshot.as_ref(), context.thing_manager.type_manager(), role_type)
        .map(|plays| plays.is_some())
}

fn filter_isa<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    isa_kind: IsaKind,
    type_: &CheckVertex<V>,
    thing: &CheckVertex<V>,
) -> Result<bool, Box<ConceptReadError>> {
    let thing = V::extract_vertex(thing, row, &context.parameters);
    let type_ = V::extract_vertex(type_, row, &context.parameters);
    let actual = unwrap_or_result_false!(thing => Thing).type_();
    let expected = unwrap_or_result_false!(type_ => Type);
    if isa_kind == IsaKind::Exact {
        Ok(actual == expected)
    } else {
        actual.is_transitive_subtype_of(expected, context.snapshot.as_ref(), context.thing_manager.type_manager())
    }
}

fn filter_has<'a, T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    owner: &CheckVertex<V>,
    attribute: &CheckVertex<V>,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let owner = V::extract_vertex(owner, row, &context.parameters);
    let attribute = V::extract_vertex(attribute, row, &context.parameters);
    let owner = unwrap_or_result_false!(&owner => Thing).as_object();
    let attribute = unwrap_or_result_false!(&attribute => Thing).as_attribute();
    owner.has_attribute(context.snapshot.as_ref(), context.thing_manager.as_ref(), attribute, storage_counters.clone())
}

fn filter_links<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    relation: &CheckVertex<V>,
    player: &CheckVertex<V>,
    role: &CheckVertex<V>,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let relation = V::extract_vertex(relation, row, &context.parameters);
    let player = V::extract_vertex(player, row, &context.parameters);
    let role = V::extract_vertex(role, row, &context.parameters);
    let relation = unwrap_or_result_false!(relation => Thing).as_relation();
    let player = unwrap_or_result_false!(player => Thing).as_object();
    let role = unwrap_or_result_false!(role => Type).as_role_type();
    relation.has_role_player(
        context.snapshot.as_ref(),
        context.thing_manager.as_ref(),
        player,
        role,
        storage_counters.clone(),
    )
}

fn filter_indexed_relation<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    start_player: &CheckVertex<V>,
    end_player: &CheckVertex<V>,
    relation: &CheckVertex<V>,
    start_role: &CheckVertex<V>,
    end_role: &CheckVertex<V>,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let start_player = V::extract_vertex(start_player, row, &context.parameters);
    let end_player = V::extract_vertex(end_player, row, &context.parameters);
    let relation = V::extract_vertex(relation, row, &context.parameters);
    let start_role = V::extract_vertex(start_role, row, &context.parameters);
    let end_role = V::extract_vertex(end_role, row, &context.parameters);
    let start_player = unwrap_or_result_false!(start_player => Thing).as_object();
    let end_player = unwrap_or_result_false!(end_player => Thing).as_object();
    let relation = unwrap_or_result_false!(relation => Thing).as_relation();
    let start_role = unwrap_or_result_false!(start_role => Type).as_role_type();
    let end_role = unwrap_or_result_false!(end_role => Type).as_role_type();
    start_player.has_indexed_relation_player(
        context.snapshot.as_ref(),
        context.thing_manager.as_ref(),
        end_player,
        relation,
        start_role,
        end_role,
        storage_counters.clone(),
    )
}

fn filter_is<T, V: ExtractFrom<T>>(row: &T, lhs: &V, rhs: &V) -> bool {
    let lhs = V::extract(lhs, row);
    let rhs = V::extract(rhs, row);
    lhs == rhs
}

fn filter_links_dedup<T, V: ExtractFrom<T>>(
    _context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    role1: &V,
    player1: &V,
    role2: &V,
    player2: &V,
) -> bool {
    let role1 = V::extract(role1, row);
    let player1 = V::extract(player1, row);
    let role2 = V::extract(role2, row);
    let player2 = V::extract(player2, row);
    !(role1 == role2 && player1 == player2)
}

fn filter_not_none<T, V: ExtractFrom<T>>(row: &T, variables: &[V]) -> bool {
    variables.iter().all(|var| {
        let value = V::extract(var, row);
        !value.is_none()
    })
}

fn filter_comparison<T, V: ExtractFrom<T>>(
    context: &ExecutionContext<impl ReadableSnapshot + 'static>,
    row: &T,
    lhs: &CheckVertex<V>,
    rhs: &CheckVertex<V>,
    comparator: Comparator,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let lhs = V::extract_vertex(lhs, row, &context.parameters);
    let rhs = V::extract_vertex(rhs, row, &context.parameters);
    let rhs = match &rhs {
        VariableValue::Thing(Thing::Attribute(attr)) => {
            attr.get_value(context.snapshot.as_ref(), context.thing_manager.as_ref(), storage_counters.clone())?
        }
        VariableValue::Value(value) => value.as_reference(),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
        VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
    };
    let lhs = match &lhs {
        VariableValue::Thing(Thing::Attribute(attr)) => {
            attr.get_value(context.snapshot.as_ref(), context.thing_manager.as_ref(), storage_counters.clone())?
        }
        VariableValue::Value(value) => value.as_reference(),
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
        VariableValue::None | VariableValue::Type(_) | VariableValue::Thing(_) => unreachable!(),
    };
    if rhs.value_type().is_trivially_castable_to(lhs.value_type().category()) {
        Ok(cmp_values_fn(&comparator)(&lhs, &rhs.cast(lhs.value_type().category()).unwrap()))
    } else if lhs.value_type().is_trivially_castable_to(rhs.value_type().category()) {
        Ok(cmp_values_fn(&comparator)(&lhs.cast(rhs.value_type().category()).unwrap(), &rhs))
    } else {
        Ok(false)
    }
}

fn cmp_values_fn(comparator: &Comparator) -> fn(&Value<'_>, &Value<'_>) -> bool {
    match comparator {
        Comparator::Equal => |a, b| a == b,
        Comparator::NotEqual => |a, b| a != b,
        Comparator::Less => |a, b| a < b,
        Comparator::Greater => |a, b| a > b,
        Comparator::LessOrEqual => |a, b| a <= b,
        Comparator::GreaterOrEqual => |a, b| a >= b,
        Comparator::Like => |a, b| {
            // TODO: Avoid recompiling the regex every time.
            regex::Regex::new(b.unwrap_string_ref())
                .expect("Invalid regex should have been caught at compile time")
                .is_match(a.unwrap_string_ref())
        },
        Comparator::Contains => |a, b| {
            let a_unicase = UniCase::new(a.unwrap_string_ref()).to_folded_case();
            let b_unicase = UniCase::new(b.unwrap_string_ref()).to_folded_case();
            a_unicase.contains(b_unicase.as_str())
        },
    }
}

fn check_iid(iid: &ByteArray<{ THING_VERTEX_MAX_LENGTH }>, value: VariableValue<'_>) -> bool {
    match value {
        VariableValue::Thing(thing) => match thing {
            Thing::Entity(entity) => *iid == *entity.vertex().to_bytes(),
            Thing::Relation(relation) => *iid == *relation.vertex().to_bytes(),
            Thing::Attribute(attribute) => *iid == *attribute.vertex().to_bytes(),
        },
        VariableValue::None => false,
        VariableValue::Type(_) => false,
        VariableValue::Value(_) => false, // or unreachable?
        VariableValue::ThingList(_) | VariableValue::ValueList(_) => unimplemented_feature!(Lists),
    }
}

fn check_sub(
    snapshot: &impl ReadableSnapshot,
    thing_manager: &ThingManager,
    sub_kind: SubKind,
    subtype: Type,
    supertype: Type,
) -> Result<bool, Box<ConceptReadError>> {
    match sub_kind {
        SubKind::Subtype => subtype.is_transitive_subtype_of(supertype, &*snapshot, thing_manager.type_manager()),
        SubKind::Exact => subtype.is_direct_subtype_of(supertype, &*snapshot, thing_manager.type_manager()),
    }
}

fn get_vertex_value<'a, 'b>(
    vertex: &'a CheckVertex<ExecutorVariable>,
    row: Option<&'b MaybeOwnedRow<'b>>,
    parameters: &'b ParameterRegistry,
) -> VariableValue<'b> {
    match vertex {
        CheckVertex::Variable(var) => get_variable_value(row, &var),
        CheckVertex::Type(type_) => VariableValue::Type(*type_),
        CheckVertex::Parameter(parameter_id) => {
            VariableValue::Value(parameters.value_unchecked(parameter_id).as_reference())
        }
    }
}

fn get_variable_value<'a>(row: Option<&'a MaybeOwnedRow<'a>>, variable: &ExecutorVariable) -> VariableValue<'a> {
    match variable {
        ExecutorVariable::RowPosition(position) => {
            row.expect("CheckVertex::Variable requires a row to take from").get(*position).as_reference()
        }
        ExecutorVariable::Internal(_) => {
            unreachable!("Check variables without an extractor must have been recorded in the row.")
        }
    }
}

#[derive(Debug, Clone)]
pub enum ExtractorOrExtractedVariable<T> {
    Extracted(VariableValue<'static>),
    ExtractFromTuple(fn(&T) -> VariableValue<'_>),
}

impl<T> ExtractorOrExtractedVariable<T> {
    fn get<'a>(&'a self, may_extract_from: &'a T) -> VariableValue<'a> {
        self.extract(may_extract_from)
    }
}

trait ExtractFrom<T>: Sized {
    fn extract_vertex<'a>(
        vertex: &'a CheckVertex<Self>,
        from: &'a T,
        parameters: &'a ParameterRegistry,
    ) -> VariableValue<'a> {
        match vertex {
            CheckVertex::Variable(var) => var.extract(from),
            CheckVertex::Type(type_) => VariableValue::Type(*type_),
            CheckVertex::Parameter(parameter_id) => {
                VariableValue::Value(parameters.value_unchecked(parameter_id).as_reference())
            }
        }
    }

    fn extract<'a>(&'a self, from: &'a T) -> VariableValue<'a>;
}

impl<'r> ExtractFrom<MaybeOwnedRow<'r>> for ExecutorVariable {
    fn extract<'a>(&'a self, row: &'a MaybeOwnedRow<'r>) -> VariableValue<'a> {
        match self {
            ExecutorVariable::RowPosition(position) => row.get(*position).as_reference(),
            ExecutorVariable::Internal(_) => {
                unreachable!("Check variables without an extractor must have been recorded in the row.")
            }
        }
    }
}

impl<T> ExtractFrom<T> for ExtractorOrExtractedVariable<T> {
    fn extract<'a>(&'a self, tuple: &'a T) -> VariableValue<'a> {
        match self {
            ExtractorOrExtractedVariable::Extracted(v) => v.as_reference(),
            ExtractorOrExtractedVariable::ExtractFromTuple(f) => f(tuple),
        }
    }
}

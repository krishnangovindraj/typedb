/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
};

use answer::variable::Variable;
use concept::{
    error::ConceptReadError,
    type_::{
        attribute_type::AttributeType, entity_type::EntityType, object_type::ObjectType, relation_type::RelationType,
        role_type::RoleType,
    },
};
use encoding::value::value_type::ValueType;
use ir::pattern::Vertex;

pub mod match_inference;
pub mod type_seeder;

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) enum VertexTypeAnnotation {
    Concept(answer::Type),
    Value(ValueType),
}

impl From<answer::Type> for VertexTypeAnnotation {
    fn from(value: answer::Type) -> Self {
        Self::Concept(value)
    }
}

impl From<EntityType> for VertexTypeAnnotation {
    fn from(value: EntityType) -> Self {
        Self::Concept(answer::Type::Entity(value))
    }
}

impl From<RelationType> for VertexTypeAnnotation {
    fn from(value: RelationType) -> Self {
        Self::Concept(answer::Type::Relation(value))
    }
}

impl From<AttributeType> for VertexTypeAnnotation {
    fn from(value: AttributeType) -> Self {
        Self::Concept(answer::Type::Attribute(value))
    }
}

impl From<RoleType> for VertexTypeAnnotation {
    fn from(value: RoleType) -> Self {
        Self::Concept(answer::Type::RoleType(value))
    }
}

impl From<ObjectType> for VertexTypeAnnotation {
    fn from(value: ObjectType) -> Self {
        match value {
            ObjectType::Entity(entity) => Self::Concept(answer::Type::Entity(entity)),
            ObjectType::Relation(relation) => Self::Concept(answer::Type::Relation(relation)),
        }
    }
}

impl VertexTypeAnnotation {
    pub(crate) fn as_concept(&self) -> Option<answer::Type> {
        match self {
            VertexTypeAnnotation::Concept(type_) => Some(*type_),
            VertexTypeAnnotation::Value(_) => None,
        }
    }

    pub(crate) fn as_value(&self) -> Option<ValueType> {
        match self {
            VertexTypeAnnotation::Concept(_) => None,
            VertexTypeAnnotation::Value(type_) => Some(*type_),
        }
    }

    pub(crate) fn unwrap_concept(&self) -> answer::Type {
        self.as_concept().expect("Expected to be concept variant")
    }

    pub(crate) fn unwrap_value(&self) -> ValueType {
        self.as_value().expect("Expected to be value variant")
    }

    pub fn try_retain(
        annotations: &mut BTreeSet<Self>,
        predicate: impl Fn(&Self) -> Result<bool, Box<ConceptReadError>>,
    ) -> Result<(), Box<ConceptReadError>> {
        let mut to_be_removed = Vec::new();
        for annotation in annotations.iter() {
            if !predicate(annotation)? {
                to_be_removed.push(*annotation);
            }
        }
        for annotation in to_be_removed.iter() {
            annotations.remove(annotation);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct VertexAnnotations {
    annotations: BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>>,
}

impl VertexAnnotations {
    pub(crate) fn new() -> VertexAnnotations {
        Self { annotations: BTreeMap::new() }
    }

    pub(crate) fn add_or_intersect(
        &mut self,
        vertex: &Vertex<Variable>,
        new_annotations: Cow<'_, BTreeSet<VertexTypeAnnotation>>,
    ) -> bool {
        if let Some(existing_annotations) = self.get_mut(vertex) {
            existing_annotations.retain_intersection(&*new_annotations)
        } else {
            self.insert(vertex.clone(), new_annotations.into_owned());
            true
        }
    }
}

impl Deref for VertexAnnotations {
    type Target = BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>>;

    fn deref(&self) -> &Self::Target {
        &self.annotations
    }
}

impl DerefMut for VertexAnnotations {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.annotations
    }
}

impl IntoIterator for VertexAnnotations {
    type Item = <BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.into_iter()
    }
}

impl<'a> IntoIterator for &'a VertexAnnotations {
    type Item = <&'a BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <&'a BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter()
    }
}

impl<'a> IntoIterator for &'a mut VertexAnnotations {
    type Item = <&'a mut BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>> as IntoIterator>::Item;
    type IntoIter = <&'a mut BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>> as IntoIterator>::IntoIter;
    fn into_iter(self) -> Self::IntoIter {
        self.annotations.iter_mut()
    }
}

impl<T> From<T> for VertexAnnotations
where
    BTreeMap<Vertex<Variable>, BTreeSet<VertexTypeAnnotation>>: From<T>,
{
    fn from(t: T) -> Self {
        Self { annotations: t.into() }
    }
}

pub(crate) trait FromIteratorMappedOperations<T>: Sized + FromIterator<T> {
    fn from_into<FROM>(iter: impl IntoIterator<Item = FROM>) -> Self
    where
        T: From<FROM>,
    {
        Self::from_iter(iter.into_iter().map(From::from))
    }

    fn from_into_ref<'a, FROM>(iter: impl IntoIterator<Item = &'a FROM>) -> Self
    where
        FROM: Copy + 'a,
        T: From<FROM>,
    {
        Self::from_into(iter.into_iter().copied())
    }

    fn from_mapped<FROM>(iter: impl IntoIterator<Item = FROM>, f: impl Fn(FROM) -> T) -> Self {
        Self::from_iter(iter.into_iter().map(f))
    }

    fn from_mapped_ref<'a, FROM>(iter: impl IntoIterator<Item = &'a FROM>, f: impl Fn(FROM) -> T) -> Self
    where
        FROM: Copy + 'a,
    {
        Self::from_mapped(iter.into_iter().copied(), f)
    }
}

pub(crate) trait ExtendMappedOperations<T>: Extend<T> {
    fn extend_into<FROM>(&mut self, iter: impl IntoIterator<Item = FROM>)
    where
        T: From<FROM>,
    {
        self.extend(iter.into_iter().map(From::from))
    }

    fn extend_into_ref<'a, FROM>(&mut self, iter: impl IntoIterator<Item = &'a FROM>)
    where
        FROM: Copy + 'a,
        T: From<FROM>,
    {
        self.extend_into(iter.into_iter().copied())
    }

    fn extend_mapped<FROM>(&mut self, iter: impl IntoIterator<Item = FROM>, f: impl Fn(FROM) -> T) {
        self.extend(iter.into_iter().map(f))
    }

    fn extend_mapped_ref<'a, FROM>(&mut self, iter: impl IntoIterator<Item = &'a FROM>, f: impl Fn(FROM) -> T)
    where
        FROM: Copy + 'a,
    {
        self.extend_mapped(iter.into_iter().copied(), f)
    }
}

impl<T: Ord> FromIteratorMappedOperations<T> for BTreeSet<T> {}
impl<T: Ord> ExtendMappedOperations<T> for BTreeSet<T> {}
impl<T> FromIteratorMappedOperations<T> for Vec<T> {}
impl<T> ExtendMappedOperations<T> for Vec<T> {}

pub(crate) trait RetainAndContainExt<T> {
    fn contains_ext(&self, item: &T) -> bool;
    fn retain_intersection<S: RetainAndContainExt<T>>(&mut self, other: &S) -> bool;
}

impl<K: Ord, V> RetainAndContainExt<K> for BTreeMap<K, V> {
    fn contains_ext(&self, item: &K) -> bool {
        self.contains_key(item)
    }

    fn retain_intersection<S: RetainAndContainExt<K>>(&mut self, other: &S) -> bool {
        let size_before = self.len();
        self.retain(|x, _| other.contains_ext(x));
        self.len() != size_before
    }
}

impl<K: Ord> RetainAndContainExt<K> for BTreeSet<K> {
    fn contains_ext(&self, item: &K) -> bool {
        self.contains(item)
    }

    fn retain_intersection<S: RetainAndContainExt<K>>(&mut self, other: &S) -> bool {
        let size_before = self.len();
        self.retain(|x| other.contains_ext(x));
        self.len() != size_before
    }
}

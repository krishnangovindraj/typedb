/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use encoding::graph::type_::edge::EncodableParametrisedTypeEdge;
use encoding::layout::prefix::Prefix;
use std::collections::HashSet;


use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::ConceptReadError,
    type_::{
        annotation::{Annotation, AnnotationCardinality, AnnotationDistinct, AnnotationKey},
        attribute_type::AttributeType,
        object_type::ObjectType,
        type_manager::TypeManager,
        Ordering, TypeAPI,
    },
};
use crate::error::ConceptWriteError;
use crate::type_::InterfaceImplementation;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Owns<'a> {
    owner: ObjectType<'a>,
    attribute: AttributeType<'a>,
}

impl<'a> Owns<'a> {
    pub fn new(owner_type: ObjectType<'a>, attribute_type: AttributeType<'a>) -> Self {
        Owns { owner: owner_type, attribute: attribute_type }
    }

    pub fn owner(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    pub fn attribute(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }

    pub fn is_distinct<'this, Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<bool, ConceptReadError> {
        let is_ordered = false; // TODO
        if is_ordered {
            let annotations = self.get_annotations(snapshot, type_manager)?;
            Ok(annotations.contains(&OwnsAnnotation::Distinct(AnnotationDistinct)))
        } else {
            Ok(true)
        }
    }

    // TODO: Should it be 'this or just 'tm on type_manager?
    pub fn get_override<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &Snapshot,
        type_manager: &'this TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'this, Option<Owns<'static>>>, ConceptReadError> {
        type_manager.get_owns_overridden(snapshot, self.clone().into_owned())
    }

    pub fn set_override<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        overridden: Owns<'static>,
    ) -> Result<(), ConceptWriteError> {
        // TODO: Validation
        type_manager.set_owns_overridden(snapshot, self.clone().into_owned(), overridden)
    }

    pub(crate) fn get_annotations<'this, Snapshot: ReadableSnapshot>(
        &'this self,
        snapshot: &Snapshot,
        type_manager: &'this TypeManager<Snapshot>,
    ) -> Result<MaybeOwns<'this, HashSet<OwnsAnnotation>>, ConceptReadError> {
        type_manager.get_owns_annotations(snapshot, self.clone())
    }

    pub fn set_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: OwnsAnnotation,
    ) -> Result<(), ConceptWriteError> {
        // type_manager.set_edge_annotation(snapshot, self.clone(), annotation)
        match annotation {
            OwnsAnnotation::Distinct(_) => type_manager.set_edge_annotation_distinct(snapshot, self.clone()),
            OwnsAnnotation::Key(_) => type_manager.set_edge_annotation_key(snapshot, self.clone()),
            OwnsAnnotation::Cardinality(cardinality) => {
                type_manager.set_edge_annotation_cardinality(snapshot, self.clone(), cardinality)
            }
        }
        Ok(()) // TODO
    }

    pub fn delete_annotation<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        annotation: OwnsAnnotation,
    ) {
        match annotation {
            OwnsAnnotation::Distinct(_) => type_manager.delete_edge_annotation_distinct(snapshot, self.clone()),
            OwnsAnnotation::Key(_) => type_manager.delete_edge_annotation_key(snapshot, self.clone()),
            OwnsAnnotation::Cardinality(_) => {
                type_manager.delete_edge_annotation_cardinality(snapshot, self.clone())
            }
        }
    }

    pub fn set_ordering<Snapshot: WritableSnapshot>(
        &self,
        snapshot: &mut Snapshot,
        type_manager: &TypeManager<Snapshot>,
        ordering: Ordering,
    ) {
        type_manager.set_owns_ordering(snapshot, self.clone(), ordering)
    }

    pub fn get_ordering<Snapshot: ReadableSnapshot>(
        &self,
        snapshot: &Snapshot,
        type_manager: &TypeManager<Snapshot>,
    ) -> Result<Ordering, ConceptReadError> {
        type_manager.get_owns_ordering(snapshot, self.clone().into_owned())
    }

    fn into_owned(self) -> Owns<'static> {
        Owns { owner: ObjectType::new(self.owner.vertex().into_owned()), attribute: self.attribute.into_owned() }
    }
}

impl<'a> EncodableParametrisedTypeEdge<'a> for Owns<'a> {
    const CANONICAL_PREFIX: Prefix = Prefix::EdgeOwns;
    const REVERSE_PREFIX: Prefix = Prefix::EdgeOwnsReverse;
    type From = ObjectType<'a>;
    type To = AttributeType<'a>;

    fn from_vertices(from: ObjectType<'a>, to: AttributeType<'a>) -> Self {
        Owns::new(from, to)
    }

    fn canonical_from(&self) -> Self::From {
        self.owner()
    }

    fn canonical_to(&self) -> Self::To {
        self.attribute()
    }
}

impl<'a> InterfaceImplementation<'a> for Owns<'a> {
    type AnnotationType = OwnsAnnotation;
    type ObjectType = ObjectType<'a>;
    type InterfaceType = AttributeType<'a>;


    fn object(&self) -> ObjectType<'a> {
        self.owner.clone()
    }

    fn interface(&self) -> AttributeType<'a> {
        self.attribute.clone()
    }

    fn unwrap_annotation(annotation: OwnsAnnotation) -> Annotation {
        match annotation {
            OwnsAnnotation::Distinct(distinct) => Annotation::Distinct(distinct),
            OwnsAnnotation::Key(key) => Annotation::Key(key),
            OwnsAnnotation::Cardinality(cardinality) => Annotation::Cardinality(cardinality)
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum OwnsAnnotation {
    Distinct(AnnotationDistinct),
    Key(AnnotationKey),
    Cardinality(AnnotationCardinality),
}

impl From<Annotation> for OwnsAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Distinct(annotation) => OwnsAnnotation::Distinct(annotation),
            Annotation::Key(annotation) => OwnsAnnotation::Key(annotation),
            Annotation::Cardinality(annotation) => OwnsAnnotation::Cardinality(annotation),

            Annotation::Abstract(_) => unreachable!("Independent annotation not available for Owns."),
            Annotation::Independent(_) => unreachable!("Independent annotation not available for Owns."),
            Annotation::Regex(_) => unreachable!("Regex annotation not available for Owns."),
        }
    }
}
//
// impl EdgeAnnotation for OwnsAnnotation {
//
// }
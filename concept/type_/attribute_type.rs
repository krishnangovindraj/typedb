/*
 *  Copyright (C) 2023 Vaticle
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

use std::collections::HashSet;

use encoding::{
    graph::type_::vertex::TypeVertex,
    layout::prefix::Prefix,
    value::{label::Label, value_type::ValueType},
    Prefixed,
};
use primitive::maybe_owns::MaybeOwns;
use storage::snapshot::{ReadableSnapshot, WritableSnapshot};

use crate::{
    error::{ConceptReadError, ConceptWriteError},
    type_::{
        annotation::{Annotation, AnnotationAbstract},
        owns::Owns,
        type_manager::TypeManager,
        TypeAPI,
    },
    ConceptAPI,
};
use crate::type_::annotation::AnnotationIndependent;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct AttributeType<'a> {
    vertex: TypeVertex<'a>,
}

impl<'a> AttributeType<'a> {
    pub fn new(vertex: TypeVertex<'a>) -> AttributeType<'_> {
        if vertex.prefix() != Prefix::VertexAttributeType {
            panic!(
                "Type IID prefix was expected to be Prefix::AttributeType ({:?}) but was {:?}",
                Prefix::VertexAttributeType,
                vertex.prefix()
            )
        }
        AttributeType { vertex }
    }
}

impl<'a> ConceptAPI<'a> for AttributeType<'a> {}

impl<'a> TypeAPI<'a> for AttributeType<'a> {
    fn vertex<'this>(&'this self) -> &'this TypeVertex<'a> {
        &self.vertex
    }

    fn into_vertex(self) -> TypeVertex<'a> {
        self.vertex
    }
}

impl<'a> AttributeType<'a> {
    pub fn is_root(&self, type_manager: &TypeManager<'_, impl ReadableSnapshot>) -> Result<bool, ConceptReadError> {
        type_manager.get_attribute_type_is_root(self.clone().into_owned())
    }

    pub fn set_value_type(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, value_type: ValueType) {
        type_manager.set_storage_value_type(self.vertex().clone().into_owned(), value_type)
    }

    pub fn get_value_type(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<Option<ValueType>, ConceptReadError> {
        type_manager.get_attribute_type_value_type(self.clone().into_owned())
    }

    pub fn get_label<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Label<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_label(self.clone().into_owned())
    }

    fn set_label(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, label: &Label<'_>) {
        // TODO: setLabel should fail is setting label on Root type
        type_manager.set_storage_label(self.vertex().clone().into_owned(), label)
    }

    fn get_supertype(
        &self,
        type_manager: &TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<Option<AttributeType<'static>>, ConceptReadError> {
        type_manager.get_attribute_type_supertype(self.clone().into_owned())
    }

    fn set_supertype(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, supertype: AttributeType<'static>) {
        type_manager.set_storage_supertype(self.vertex().clone().into_owned(), supertype.vertex().clone().into_owned())
    }

    fn get_supertypes<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, Vec<AttributeType<'static>>>, ConceptReadError> {
        type_manager.get_attribute_type_supertypes(self.clone().into_owned())
    }

    // fn get_subtypes(&self) -> MaybeOwns<'m, Vec<AttributeType<'static>>>;

    pub fn get_annotations<'m>(
        &self,
        type_manager: &'m TypeManager<'_, impl ReadableSnapshot>,
    ) -> Result<MaybeOwns<'m, HashSet<AttributeTypeAnnotation>>, ConceptReadError> {
        type_manager.get_attribute_type_annotations(self.clone().into_owned())
    }

    pub(crate) fn set_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: AttributeTypeAnnotation) {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_manager.set_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_manager.set_storage_annotation_independent(self.vertex().clone().into_owned())
            }
        }
    }

    fn delete_annotation(&self, type_manager: &TypeManager<'_, impl WritableSnapshot>, annotation: AttributeTypeAnnotation) {
        match annotation {
            AttributeTypeAnnotation::Abstract(_) => {
                type_manager.delete_storage_annotation_abstract(self.vertex().clone().into_owned())
            }
            AttributeTypeAnnotation::Independent(_) => {
                type_manager.delete_storage_annotation_independent(self.vertex().clone().into_owned())
            }
        }
    }

    pub fn into_owned(self) -> AttributeType<'static> {
        AttributeType { vertex: self.vertex.into_owned() }
    }
}

// --- Owned API ---
impl<'a> AttributeType<'a> {
    fn get_owns<'m>(&self, _type_manager: &'m TypeManager<'_, impl ReadableSnapshot>) -> MaybeOwns<'m, HashSet<Owns<'static>>> {
        todo!()
    }

    fn get_owns_owners(&self) {
        // return iterator of Owns
        todo!()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum AttributeTypeAnnotation {
    Abstract(AnnotationAbstract),
    Independent(AnnotationIndependent),
}

impl From<Annotation> for AttributeTypeAnnotation {
    fn from(annotation: Annotation) -> Self {
        match annotation {
            Annotation::Abstract(annotation) => AttributeTypeAnnotation::Abstract(annotation),
            Annotation::Independent(annotation) => AttributeTypeAnnotation::Independent(annotation),
            Annotation::Duplicate(_) => unreachable!("Duplicate annotation not available for Attribute type."),
        }
    }
}

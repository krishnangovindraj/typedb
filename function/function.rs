/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use answer::Type;
use encoding::{
    graph::definition::{definition_key::DefinitionKey, function::FunctionDefinition},
    value::value_type::ValueType,
};
use ir::{
    pattern::variable_category::{VariableCategory, VariableOptionality},
    program::function::{FunctionID, FunctionIDTrait, Reducer},
};

use crate::FunctionDefinitionError;

// TODO: Krishnan: Introduced this to push through with functions
pub(crate) type PlaceholderForTypeQLFunctionalBlock = String;
/// Function represents the user-defined structure:
/// fun <name>(<args>) -> <return type> { <body> }
///
pub type StoredFunction = Function<DefinitionKey<'static>>;

pub struct Function<IDType: FunctionIDTrait> {
    pub(crate) function_id: IDType,

    // parsed representation
    pub(crate) name: String,
    pub(crate) arguments: Vec<FunctionArgument>,
    pub(crate) return_type: FunctionReturn,
    // pub(crate) body: PlaceholderForTypeQLFunctionalBlock,
    // pub(crate) return_statement: (FunctionReturnOperation, Vec<String>), // TODO: There was a todo on how to encode the return statement.
}

pub struct FunctionArgument {
    pub(crate) name: String,
    pub(crate) type_: FunctionValuePrototype,
}

pub enum FunctionReturn {
    Stream(Vec<FunctionValuePrototype>),
    Single(Vec<FunctionValuePrototype>),
}

pub enum FunctionReturnOperation {
    Stream(Vec<String>),
    Single(Reducer, Vec<String>),
}

impl<IDType: FunctionIDTrait> Function<IDType> {
    // TODO: receive a string, which can either come from the User or from Storage (deserialised)
    //       will require type manager to convert labels into Types and Definitions etc. // No. We can resolve functions but type-inference
    pub(crate) fn build<'a>(
        function_id: FunctionID,
        definition: FunctionDefinition<'a>,
    ) -> Result<Self, FunctionDefinitionError> {
        // 1. parse into TypeQL
        // 2. extract into data structures
        // possible later: // TODO: what if recursive - the function call will need information from this function! -> defer?
        // 3. create IR & apply inference
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FunctionValuePrototype {
    Thing(Type),
    ThingOptional(Type),
    Value(ValueType),
    ValueOptional(ValueType),
    ThingList(Type),
    ThingListOptional(Type),
    ValueList(ValueType),
    ValueListOptional(ValueType),
}

impl Into<VariableCategory> for FunctionValuePrototype {
    fn into(self) -> VariableCategory {
        match self {
            FunctionValuePrototype::Thing(type_) | FunctionValuePrototype::ThingOptional(type_) => match type_ {
                Type::Entity(_) | Type::Relation(_) => VariableCategory::Object,
                Type::Attribute(_) => VariableCategory::Attribute,
                Type::RoleType(_) => unreachable!("A function cannot use role typed instances"),
            },
            FunctionValuePrototype::Value(_) | FunctionValuePrototype::ValueOptional(_) => VariableCategory::Value,
            FunctionValuePrototype::ThingList(type_) | FunctionValuePrototype::ThingListOptional(type_) => {
                match type_ {
                    Type::Entity(_) | Type::Relation(_) => VariableCategory::ObjectList,
                    Type::Attribute(_) => VariableCategory::AttributeList,
                    Type::RoleType(_) => unreachable!("A function cannot use role-list typed instances"),
                }
            }
            FunctionValuePrototype::ValueList(_) | FunctionValuePrototype::ValueListOptional(_) => {
                VariableCategory::ValueList
            }
        }
    }
}

impl Into<VariableOptionality> for FunctionValuePrototype {
    fn into(self) -> VariableOptionality {
        match self {
            FunctionValuePrototype::Thing(_)
            | FunctionValuePrototype::Value(_)
            | FunctionValuePrototype::ThingList(_)
            | FunctionValuePrototype::ValueList(_) => VariableOptionality::Required,
            FunctionValuePrototype::ThingOptional(_)
            | FunctionValuePrototype::ValueOptional(_)
            | FunctionValuePrototype::ThingListOptional(_)
            | FunctionValuePrototype::ValueListOptional(_) => VariableOptionality::Optional,
        }
    }
}

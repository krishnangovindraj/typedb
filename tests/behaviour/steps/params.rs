/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::str::FromStr;
use cucumber::Parameter;
use concept::type_::annotation;
use concept::type_::annotation::{Annotation as TypeDBAnnotation};
use encoding::{
    graph::type_::Kind as TypeDBTypeKind,
    value::{
        label::Label as TypeDBLabel,
        value_type::ValueType as TypeDBValueType,
    },
};

#[derive(Debug, Default, Parameter)]
#[param(name = "may_error", regex = "(fails|)")]
pub(crate) enum MayError {
    #[default]
    False,
    True,
}

impl MayError {
    pub fn check<T,E>(&self, res: &Result<T,E>) {
        match self {
            MayError::False => assert!(res.is_ok()),
            MayError::True => assert!(res.is_err()),
        };
    }
}

impl FromStr for MayError {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "fails" => Self::True,
            "" => Self::False,
            invalid => return Err(format!("Invalid `MayError`: {invalid}")),
        })
    }
}


#[derive(Debug, Default, Parameter)]
#[param(name = "boolean", regex = "(true|false)")]
pub(crate) enum Boolean {
    #[default]
    False,
    True,
}

impl Boolean {
    pub fn check(&self, res: bool) {
        match self {
            Boolean::False => assert_eq!(false, res),
            Boolean::True => assert_eq!(true, res),
        };
    }
}

impl FromStr for Boolean {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "true" => Self::True,
            "false" => Self::False,
            invalid => return Err(format!("Invalid `Boolean`: {invalid}")),
        })
    }
}

#[derive(Debug, Default, Parameter)]
#[param(name = "contains_or_doesnt", regex = "(contain|do not contain)")]
pub(crate) enum ContainsOrDoesnt {
    #[default]
    Contains,
    DoesNotContain,
}

impl ContainsOrDoesnt {
    pub fn check<T: PartialEq + std::fmt::Debug>(&self, expected: Vec<T>, actual: Vec<T>) {
        let expected_contains: bool = self.expected_contains();

        expected.iter().for_each(|expected_item| {
            println!("Check contains({:?}) : {:?} = {:?}", expected_item, expected_contains, actual.contains(&expected_item));
            assert_eq!(expected_contains, actual.contains(&expected_item));
        });
    }

    pub fn expected_contains(&self) -> bool {
        match self {
            ContainsOrDoesnt::Contains => true,
            ContainsOrDoesnt::DoesNotContain => false,
        }
    }
}

impl FromStr for ContainsOrDoesnt {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "contain" => Self::Contains,
            "do not contain" => Self::DoesNotContain,
            invalid => return Err(format!("Invalid `ContainsOrDoesnt`: {invalid}")),
        })
    }
}


#[derive(Debug, Parameter)]
#[param(name = "type_label", regex = r"[A-Za-z0-9_\-:]+")]
pub(crate) struct Label {
    label_string: String,
}

impl Default for Label {
    fn default() -> Self {
        unreachable!("Why is default called?");
    }
}

impl Label {
    pub fn to_typedb(&self) -> TypeDBLabel<'static> {
        TypeDBLabel::build(&self.label_string)
    }
}

impl FromStr for Label {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok( Self { label_string: s.to_string() })
    }
}

#[derive(Debug, Parameter)]
#[param(name = "root_label", regex = r"(attribute|entity|relation)")]
pub(crate) struct RootLabel {
    kind: TypeDBTypeKind,
}

impl RootLabel {
    pub fn to_typedb(&self) -> TypeDBTypeKind {
         self.kind
    }
}

impl FromStr for RootLabel {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let kind = match s {
            "attribute" => TypeDBTypeKind::Attribute,
            "entity" => TypeDBTypeKind::Entity,
            "relation" => TypeDBTypeKind::Relation,
            _ => unreachable!()
        };
        Ok(RootLabel { kind })
    }
}


#[derive(Debug, Default, Parameter)]
#[param(name = "value_type", regex = "(boolean|long|double|string|datetime)")]
pub(crate) enum ValueType {
    #[default]
    Boolean,
    Long,
    Double,
    String,
    DateTime
}

impl ValueType {
    pub fn to_typedb(&self) -> TypeDBValueType {
        match self {
            ValueType::Boolean => TypeDBValueType::Boolean,
            ValueType::Long => TypeDBValueType::Long,
            ValueType::Double => TypeDBValueType::Double,
            ValueType::String => TypeDBValueType::String,
            ValueType::DateTime => todo!(),
        }
    }
}

impl FromStr for ValueType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "long" => Self::Long,
            "string" => Self::String,
            "boolean" => Self::Boolean,
            "double" => Self::Double,
            "datetime" => Self::DateTime,
            _ => panic!("Unrecognised value type")
        })
    }
}



#[derive(Debug, Parameter)]
#[param(name = "annotation", regex = r"(@[a-z]+\([^\)]+\)|@[a-z]+)")]
pub(crate) struct  Annotation {
    typedb_annotation: TypeDBAnnotation,
}

impl Annotation {
    pub fn to_typedb(&self) -> TypeDBAnnotation {
        self.typedb_annotation
    }

}

impl FromStr for crate::params::Annotation {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // This will have to be smarter to parse annotations out.
        let typedb_annotation = match s {
            "@abstract" =>  { TypeDBAnnotation::Abstract(annotation::AnnotationAbstract::new()) },
            _ => panic!("Unrecognised (or unimplemented) annotation: {s}")
        };
        Ok(Self { typedb_annotation })
    }

}


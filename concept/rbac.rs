/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;

use encoding::graph::{
    rbac::{RBACAccessKind, RBACRoleVertex},
    type_::vertex::TypeVertex,
};

use crate::type_::{attribute_type::AttributeType, object_type::ObjectType};

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RBACAccess {
    ReadObject(ObjectType),
    WriteObject(ObjectType),
    ReadAttribute(AttributeType),
    WriteAttribute(AttributeType),
}

impl RBACAccess {
    pub fn new_type_access(access_kind: RBACAccessKind, type_: TypeVertex) -> Self {
        match access_kind {
            RBACAccessKind::ReadObjectVertex => Self::ReadObject(ObjectType::new(type_)),
            RBACAccessKind::WriteObjectVertex => Self::WriteObject(ObjectType::new(type_)),
            RBACAccessKind::ReadAttributeVertex => Self::ReadAttribute(AttributeType::new(type_)),
            RBACAccessKind::WriteAttributeVertex => Self::WriteAttribute(AttributeType::new(type_)),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RBACRole {
    vertex: RBACRoleVertex,
}

impl fmt::Debug for RBACRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RBACRole[{:?}]", self.vertex.role_id())
    }
}

impl RBACRole {
    pub fn new(vertex: RBACRoleVertex) -> Self {
        Self { vertex }
    }

    pub fn vertex(&self) -> RBACRoleVertex {
        self.vertex
    }
}

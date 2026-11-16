/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;
use encoding::graph::rbac::{RBACAccessKind, RBACRoleVertex};
use encoding::graph::type_::vertex::TypeVertex;
use crate::type_::object_type::ObjectType;
use crate::type_::TypeAPI;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RBACAccess {
    ReadObject(ObjectType),
    WriteObject(ObjectType),
    // More to come?
}

impl RBACAccess {
    pub(crate) fn new_type_access(access_kind: RBACAccessKind, type_: TypeVertex) -> Self {
        match access_kind {
            RBACAccessKind::ReadObjectVertex => Self::ReadObject(ObjectType::new(type_)),
            RBACAccessKind::WriteObjectVertex => Self::WriteObject(ObjectType::new(type_)),
        }
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct RBACRole {
    vertex: RBACRoleVertex,
}

impl fmt::Debug for RBACRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Entity[{:?}]", self.vertex.role_id())
    }
}


impl RBACRole {
    // Do we need to copy the type manager methods here for consistency w/ concepts?
    pub fn new(vertex: RBACRoleVertex) -> Self {
        Self { vertex }
    }

    pub(crate) fn vertex(&self) -> RBACRoleVertex {
        self.vertex
    }
}

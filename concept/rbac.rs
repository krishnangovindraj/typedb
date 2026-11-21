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
use resource::profile::StorageCounters;
use storage::snapshot::ReadableSnapshot;

use crate::{
    error::ConceptReadError,
    type_::{TypeAPI, attribute_type::AttributeType, object_type::ObjectType, type_manager::TypeManager},
};
compile_error!("You want rbac to be its own top level module");

#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum RBACAccess {
    ReadObject(ObjectType),
    WriteObject(ObjectType),
    // More to come?
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
        write!(f, "Entity[{:?}]", self.vertex.role_id())
    }
}

impl RBACRole {
    // Do we need to copy the type manager methods here for consistency w/ concepts?
    pub fn new(vertex: RBACRoleVertex) -> Self {
        Self { vertex }
    }

    pub fn vertex(&self) -> RBACRoleVertex {
        self.vertex
    }

    pub fn check_has_access(
        &self,
        snapshot: &impl ReadableSnapshot,
        type_manager: &TypeManager,
        access: &RBACAccess,
        storage_counters: StorageCounters,
    ) -> Result<bool, Box<ConceptReadError>> {
        check_has_access(snapshot, type_manager, self, access, storage_counters)
    }

}

fn check_has_access(
    snapshot: &impl ReadableSnapshot,
    type_manager: &TypeManager,
    role: &RBACRole,
    access: &RBACAccess,
    storage_counters: StorageCounters,
) -> Result<bool, Box<ConceptReadError>> {
    let all_accesses = type_manager.list_rbac_declared_access(snapshot, role, storage_counters)?;
    Ok(all_accesses.contains(access))
}

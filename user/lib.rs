/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use database::database::DatabaseUid;
use system::concepts::SystemPermission;

pub mod errors;
pub mod permission_manager;
pub mod user_manager;

pub enum UserSystemPermission {
    ManageUsers,
    ManageDatabases,
    GrantPermissions,
}

pub enum UserDatabasePermission {
    TransactionRead,
    TransactionWrite,
    SchemaTypes,
    SchemaRoles,
    // GrantRoles, // TODO
}

impl UserSystemPermission {
    pub fn to_system_permission(&self) -> SystemPermission {
        match self {
            Self::ManageUsers => SystemPermission::ManageUsers,
            Self::ManageDatabases => SystemPermission::ManageDatabases,
            Self::GrantPermissions => SystemPermission::GrantPermissions,
        }
    }
}


impl UserDatabasePermission {
    pub fn to_system_permission(&self, database: DatabaseUid,) -> SystemPermission {
        match self {
            Self::TransactionRead => SystemPermission::TransactionRead { database },
            Self::TransactionWrite => SystemPermission::TransactionWrite { database },
            Self::SchemaTypes => SystemPermission::SchemaTypes { database },
            Self::SchemaRoles => SystemPermission::SchemaRoles { database },
        }
    }
}

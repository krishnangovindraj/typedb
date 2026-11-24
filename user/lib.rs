/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::fmt;

use database::database::DatabaseUid;
use system::concepts::{DatabasePermission, SystemPermission};

pub mod errors;
pub mod permission_manager;
pub mod user_manager;

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum UserSystemPermission {
    ManageUsers,
    ManageDatabases,
    GrantPermissions,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
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
    pub fn to_database_permission(&self, database: DatabaseUid) -> DatabasePermission {
        match self {
            Self::TransactionRead => DatabasePermission::TransactionRead { database },
            Self::TransactionWrite => DatabasePermission::TransactionWrite { database },
            Self::SchemaTypes => DatabasePermission::SchemaTypes { database },
            Self::SchemaRoles => DatabasePermission::SchemaRoles { database },
        }
    }
}

impl fmt::Display for UserSystemPermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManageDatabases => write!(f, "manage-databases"),
            Self::ManageUsers => write!(f, "manage-users"),
            Self::GrantPermissions => write!(f, "grant-permissions"),
        }
    }
}

impl fmt::Display for UserDatabasePermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TransactionRead => write!(f, "transaction-read"),
            Self::TransactionWrite => write!(f, "transaction-write"),
            Self::SchemaTypes => write!(f, "schema-types"),
            Self::SchemaRoles => write!(f, "schema-roles"),
        }
    }
}

impl From<SystemPermission> for UserSystemPermission {
    fn from(permission: SystemPermission) -> Self {
        match permission {
            SystemPermission::ManageDatabases => UserSystemPermission::ManageDatabases,
            SystemPermission::ManageUsers => UserSystemPermission::ManageUsers,
            SystemPermission::GrantPermissions => UserSystemPermission::GrantPermissions,
        }
    }
}

impl From<DatabasePermission> for UserDatabasePermission {
    fn from(permission: DatabasePermission) -> Self {
        match permission {
            DatabasePermission::TransactionRead { .. } => UserDatabasePermission::TransactionRead,
            DatabasePermission::TransactionWrite { .. } => UserDatabasePermission::TransactionWrite,
            DatabasePermission::SchemaTypes { .. } => UserDatabasePermission::SchemaTypes,
            DatabasePermission::SchemaRoles { .. } => UserDatabasePermission::SchemaRoles,
        }
    }
}

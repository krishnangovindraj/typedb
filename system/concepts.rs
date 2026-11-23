/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::fmt;

use database::database::DatabaseUid;
use encoding::graph::rbac::RBACRoleId;
use pwhash::bcrypt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct User {
    pub name: String,
}

impl User {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

#[derive(Clone, Debug)]
pub enum Credential {
    PasswordType { password_hash: PasswordHash },
}

impl Credential {
    pub fn new_password(password: &str) -> Self {
        Self::PasswordType { password_hash: PasswordHash::from_password(password) }
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct PasswordHash {
    pub value: String,
}

impl PasswordHash {
    pub fn new(hash: String) -> Self {
        Self { value: hash }
    }

    pub fn from_password(password: &str) -> Self {
        Self { value: bcrypt::hash(password).unwrap() }
    }

    pub fn matches(&self, password: &str) -> bool {
        bcrypt::verify(password, self.value.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UserGroup {
    pub name: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Grantee {
    User(User),
    UserGroup(UserGroup),
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SystemPermission {
    ManageDatabases,
    ManageUsers,
    GrantPermissions,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum DatabasePermission {
    TransactionRead { database: DatabaseUid },
    TransactionWrite { database: DatabaseUid },
    SchemaTypes { database: DatabaseUid },
    SchemaRoles { database: DatabaseUid },
}


#[derive(Debug, Clone, Eq, PartialEq)]
pub struct SchemaDefinedRole {
    pub database: DatabaseUid,
    pub role_id: RBACRoleId,
}

impl Grantee {
    pub(crate) fn type_to_typeql_value_string(&self) -> &str {
        match self {
            Grantee::User(_) => "user",
            Grantee::UserGroup(_) => "user-group",
        }
    }

    pub(crate) fn name_to_typeql_value_string(&self) -> String {
        match self {
            Grantee::User(user) => user.name.to_string(),
            Grantee::UserGroup(group) => group.name.to_string(),
        }
    }
}

impl fmt::Display for Grantee {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Grantee::User(user) => write!(f, "User('{}')", user.name),
            Grantee::UserGroup(group) => write!(f, "UserGroup('{}')", group.name),
        }
    }
}

impl fmt::Display for SystemPermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemPermission::ManageDatabases => write!(f, "manage-databases"),
            SystemPermission::ManageUsers => write!(f, "manage-users"),
            SystemPermission::GrantPermissions => write!(f, "grant-permissions"),
        }
    }
}

impl SystemPermission {
    pub(crate) fn type_to_typeql_string(&self) -> &str {
        match self {
            SystemPermission::ManageDatabases => "manage-databases",
            SystemPermission::ManageUsers => "manage-users",
            SystemPermission::GrantPermissions => "grant-permissions",
        }
    }
}

impl fmt::Display for DatabasePermission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DatabasePermission::TransactionRead { database } => write!(f, "transaction-read:{}", database.0),
            DatabasePermission::TransactionWrite { database } => write!(f, "transaction-write:{}", database.0),
            DatabasePermission::SchemaTypes { database } => write!(f, "schema-types:{}", database.0),
            DatabasePermission::SchemaRoles { database } => write!(f, "schema-roles:{}", database.0),
        }
    }
}

impl DatabasePermission {
    pub(crate) fn type_to_typeql_string(&self) -> &str {
        match self {
            DatabasePermission::TransactionRead { .. } => "transaction-read",
            DatabasePermission::TransactionWrite { .. } => "transaction-write",
            DatabasePermission::SchemaTypes { .. } => "schema-types",
            DatabasePermission::SchemaRoles { .. } => "schema-roles",
        }
    }

    pub(crate) fn database(&self) -> DatabaseUid {
        match self {
            DatabasePermission::TransactionRead { database }
            | DatabasePermission::TransactionWrite { database }
            | DatabasePermission::SchemaTypes { database }
            | DatabasePermission::SchemaRoles { database } => database.clone(),
        }
    }
}

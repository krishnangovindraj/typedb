/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use pwhash::bcrypt;
use uuid::Uuid;

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct UserGroup {
    pub name: String,
}

#[derive(Clone, Debug)]
pub enum Grantee {
    User(User),
    UserGroup(UserGroup),
}

#[derive(Debug)]
pub struct RoleUid(pub u64);

#[derive(Debug)]
pub struct DatabaseUid(pub Uuid);

#[derive(Debug)]
pub enum GrantedPermission {
    ManageDatabases,
    ManageUsers,
    GrantPermissions,

    TransactionRead { database: DatabaseUid },
    TransactionWrite { database: DatabaseUid },
    SchemaTypes { database: DatabaseUid },
    SchemaRoles { database: DatabaseUid },
    Role { database: DatabaseUid, role: RoleUid },
}

#[derive(Debug)]
pub struct Grant {
    grantee: Grantee,
    permission: GrantedPermission,
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

impl RoleUid {
    pub(crate) fn to_typeql_value_string(&self) -> String {
        self.0.to_string()
    }
}

impl DatabaseUid {
    pub(crate) fn to_typeql_value_string(&self) -> String {
        self.0.to_string()
    }
}

impl GrantedPermission {
    pub(crate) fn type_to_typeql_string(&self) -> &str {
        match self {
            GrantedPermission::ManageDatabases => "manage-databases",
            GrantedPermission::ManageUsers => "manage-users",
            GrantedPermission::GrantPermissions => "grant-permissions",
            GrantedPermission::TransactionRead { .. } => "transaction-read",
            GrantedPermission::TransactionWrite { .. } => "transaction-write",
            GrantedPermission::SchemaTypes { .. } => "schema-types",
            GrantedPermission::SchemaRoles { .. } => "schema-roles",
            GrantedPermission::Role { .. } => "",
        }
    }
}

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use concept::{
    rbac::{RBACAccess, RBACRole},
    type_::object_type::ObjectType,
};
use cucumber::gherkin::Step;
use database::{database::DatabaseUid, transaction::TransactionRead};
use encoding::{graph::rbac::RBACRoleId, value::label::Label};
use macro_rules_attribute::apply;
use options::TransactionOptions;
use params::{HasOrDoesnt, MayError, SchemaDefinedRoleParam, SystemPermissionParam};
use resource::{constants::server::DEFAULT_USER_NAME, profile::StorageCounters};
use server::authentication::Accessor;
use system::concepts::{Credential, Grantee, SchemaDefinedRole, User};
use user::{UserDatabasePermission, UserSystemPermission};

use crate::{Context, generic_step, transaction_context::with_read_tx, util::iter_table_map};

fn admin_accessor() -> Accessor {
    Accessor(DEFAULT_USER_NAME.to_owned())
}

async fn resolve_database_uid(context: &Context, name: &str) -> DatabaseUid {
    let server = context.server.as_ref().unwrap().lock().unwrap();
    let db_opt = server.server_state().databases().get(name).await;
    db_opt.unwrap().expect("Database with name '{name}' not found ").database_id()
}

async fn resolve_role_uid(
    context: &Context,
    database_name: &str,
    role_name: &str,
) -> Option<RBACRoleId> {
    let server = context.server.as_ref().unwrap().lock().unwrap();
    let db_opt = server.server_state().databases().get(database_name).await;
    let database = db_opt.unwrap().expect("Database with name '{name}' not found ");
    let tx = TransactionRead::open(database, TransactionOptions::default()).expect("Should be able to open");
    let role = tx
        .rbac_manager
        .get_rbac_role(&*tx.snapshot, role_name, StorageCounters::DISABLED)
        .unwrap();
    tx.close();
    role.map(|r| r.vertex().role_id())
}

async fn schema_defined_role_param_to_typedb(
    context: &Context,
    param: SchemaDefinedRoleParam,
) -> Result<SchemaDefinedRole, String> {
    let database = resolve_database_uid(context, param.database.as_str()).await;
    let role_id = resolve_role_uid(context, param.database.as_str(), param.role.as_str()).await;
    match role_id {
        Some(role_id) => Ok(SchemaDefinedRole { database, role_id }),
        None => Err(format!("Role '{}' not found in database '{}'", param.role, param.database)),
    }
}

enum BDDPermission {
    System(UserSystemPermission),
    Database { database: String, permission: UserDatabasePermission },
}

fn permission_param_to_bdd(param: SystemPermissionParam) -> BDDPermission {
    match param.kind.as_str() {
        "manage-databases" => BDDPermission::System(UserSystemPermission::ManageDatabases),
        "manage-users" => BDDPermission::System(UserSystemPermission::ManageUsers),
        "grant-permissions" => BDDPermission::System(UserSystemPermission::GrantPermissions),
        "transaction-read" => BDDPermission::Database { database: param.database.unwrap(), permission: UserDatabasePermission::TransactionRead },
        "transaction-write" => BDDPermission::Database { database: param.database.unwrap(), permission: UserDatabasePermission::TransactionWrite },
        "schema-types" => BDDPermission::Database { database: param.database.unwrap(), permission: UserDatabasePermission::SchemaTypes },
        "schema-roles" => BDDPermission::Database { database: param.database.unwrap(), permission: UserDatabasePermission::SchemaRoles },
        other => panic!("Unknown permission kind '{other}'"),
    }
}

fn resolve_rbac_access(
    type_manager: &concept::type_::type_manager::TypeManager,
    snapshot: &impl storage::snapshot::ReadableSnapshot,
    type_label: &Label,
    access: &str,
) -> Option<RBACAccess> {
    if let Some(t) = type_manager.get_entity_type(snapshot, type_label).expect("Error getting entity type") {
        Some(match access {
            "read" => RBACAccess::ReadObject(ObjectType::Entity(t)),
            "write" => RBACAccess::WriteObject(ObjectType::Entity(t)),
            other => panic!("Unknown access type '{other}'; expected 'read' or 'write'"),
        })
    } else if let Some(t) = type_manager.get_relation_type(snapshot, type_label).expect("Error getting relation type") {
        Some(match access {
            "read" => RBACAccess::ReadObject(ObjectType::Relation(t)),
            "write" => RBACAccess::WriteObject(ObjectType::Relation(t)),
            other => panic!("Unknown access type '{other}'; expected 'read' or 'write'"),
        })
    } else if let Some(t) = type_manager.get_attribute_type(snapshot, type_label).expect("Error getting attribute type")
    {
        Some(match access {
            "read" => RBACAccess::ReadAttribute(t),
            "write" => RBACAccess::WriteAttribute(t),
            other => panic!("Unknown access type '{other}'; expected 'read' or 'write'"),
        })
    } else {
        None
    }
}

fn check_rbac_access_on_type(
    type_manager: &concept::type_::type_manager::TypeManager,
    rbac_manager: &rbac::rbac_manager::RBACManager,
    snapshot: &impl storage::snapshot::ReadableSnapshot,
    role: &RBACRole,
    type_label: &Label,
    access: &str,
) -> bool {
    let access = resolve_rbac_access(type_manager, snapshot, type_label, access)
        .unwrap_or_else(|| panic!("Could not resolve type '{type_label}'"));
    rbac_manager.check_has_access(snapshot, role, &access, StorageCounters::DISABLED).unwrap()
}

#[apply(generic_step)]
#[step(expr = "list rbac declared access for role: {word}")]
async fn rbac_role_has_direct_grants(context: &mut Context, role_name: String, step: &Step) {
    with_read_tx!(context, |tx| {
        let role = tx
            .rbac_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_name, StorageCounters::DISABLED)
            .expect("Error getting RBAC role")
            .unwrap_or_else(|| panic!("RBAC role '{role_name}' not found"));

        let declared_roles = tx
            .rbac_manager
            .list_rbac_declared_access(tx.snapshot.as_ref(), &role, StorageCounters::DISABLED)
            .expect("Error getting reading RBAC permissions for role");

        let expected: HashSet<RBACAccess> = iter_table_map(step)
            .map(|row| {
                let type_name = row["type"];
                let type_label = Label::build(type_name, None);
                resolve_rbac_access(&tx.type_manager, tx.snapshot.as_ref(), &type_label, row["access"])
                    .unwrap_or_else(|| panic!("Type '{type_name}' not found"))
            })
            .collect();

        assert_eq!(*declared_roles, expected, "Declared RBAC accesses for role '{role_name}' did not match table");
    });
}

#[apply(generic_step)]
#[step(expr = "rbac role '{word}' has {word} access on type '{word}': {boolean}")]
async fn rbac_role_has_access_on_type(
    context: &mut Context,
    role_name: String,
    access: String,
    type_name: String,
    expected_has_access: params::Boolean,
) {
    with_read_tx!(context, |tx| {
        let role = tx
            .rbac_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_name, StorageCounters::DISABLED)
            .expect("Error getting RBAC role")
            .unwrap_or_else(|| panic!("RBAC role '{role_name}' not found"));

        let type_label = Label::build(&type_name, None);
        params::check_boolean!(
            expected_has_access,
            check_rbac_access_on_type(
                &tx.type_manager,
                &tx.rbac_manager,
                tx.snapshot.as_ref(),
                &role,
                &type_label,
                &access
            )
        );
    });
}

#[apply(generic_step)]
#[step(expr = "create user with username {string}, password {string}")]
async fn create_user(context: &mut Context, username: String, password: String) {
    context
        .server_state()
        .users()
        .create(admin_accessor(), User::new(username), Credential::new_password(&password), None, None)
        .await
        .expect("Failed to create user");
}

#[apply(generic_step)]
#[step(expr = "grant user {string} permission: {permission}")]
async fn grant_user_permission(context: &mut Context, username: String, permission: SystemPermissionParam) {
    let grantee = Grantee::User(User::new(username));
    match permission_param_to_bdd(permission) {
        BDDPermission::System(perm) => context
            .server_state()
            .users()
            .grant_system_permission(admin_accessor(), grantee, perm)
            .await
            .expect("Failed to grant system permission"),
        BDDPermission::Database { database, permission: perm } => context
            .server_state()
            .users()
            .grant_database_permission(admin_accessor(), grantee, &database, perm)
            .await
            .expect("Failed to grant database permission"),
    }
}

#[apply(generic_step)]
#[step(expr = "user {string} {has_or_doesnt} permission: {permission}")]
async fn user_has_permission(
    context: &mut Context,
    username: String,
    has_or_doesnt: HasOrDoesnt,
    permission: SystemPermissionParam,
) {
    let grantee = Grantee::User(User::new(username));
    match permission_param_to_bdd(permission) {
        BDDPermission::System(expected) => {
            let permissions = context
                .server_state()
                .users()
                .list_system_permissions(admin_accessor(), grantee)
                .await
                .expect("Failed to list system permissions");
            has_or_doesnt.check_contains(&permissions, &expected);
        }
        BDDPermission::Database { database, permission: expected } => {
            let permissions = context
                .server_state()
                .users()
                .list_database_permissions(admin_accessor(), grantee, &database)
                .await
                .expect("Failed to list database permissions");
            has_or_doesnt.check_contains(&permissions, &expected);
        }
    }
}

#[apply(generic_step)]
#[step(expr = "grant user {string} database role: {schema_defined_role}{may_error}")]
async fn grant_user_database_role(
    context: &mut Context,
    username: String,
    role: SchemaDefinedRoleParam,
    may_error: MayError,
) {
    let grantee = Grantee::User(User::new(username));
    let result: Result<(), String> = match schema_defined_role_param_to_typedb(context, role).await {
        Err(e) => Err(e),
        Ok(granted) => context
            .server_state()
            .users()
            .grant_role(admin_accessor(), grantee, granted)
            .await
            .map_err(|e| format!("{e:?}")),
    };
    may_error.check(result);
}

#[apply(generic_step)]
#[step(expr = "user {string} {has_or_doesnt} database role: {schema_defined_role}")]
async fn user_has_database_role(
    context: &mut Context,
    username: String,
    has_or_doesnt: HasOrDoesnt,
    role: SchemaDefinedRoleParam,
) {
    let grantee = Grantee::User(User::new(username));
    let expected = schema_defined_role_param_to_typedb(context, role).await.expect("Role not found");
    let roles =
        context.server_state().users().list_roles(admin_accessor(), grantee).await.expect("Failed to list roles");
    has_or_doesnt.check_contains(&roles, &expected);
}

#[apply(generic_step)]
#[step(expr = "rbac role '{word}' exists: {boolean}")]
async fn rbac_role_exists(context: &mut Context, role_name: String, expected_exists: params::Boolean) {
    with_read_tx!(context, |tx| {
        let role = tx
            .rbac_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_name, StorageCounters::DISABLED)
            .expect("Error getting RBAC role");
        params::check_boolean!(expected_exists, role.is_some());
    });
}

#[apply(generic_step)]
#[step(expr = "ungrant user {string} permission: {permission}")]
async fn ungrant_user_permission(context: &mut Context, username: String, permission: SystemPermissionParam) {
    let grantee = Grantee::User(User::new(username));
    match permission_param_to_bdd(permission) {
        BDDPermission::System(perm) => context
            .server_state()
            .users()
            .revoke_system_permission(admin_accessor(), grantee, perm)
            .await
            .expect("Failed to revoke system permission"),
        BDDPermission::Database { database, permission: perm } => context
            .server_state()
            .users()
            .revoke_database_permission(admin_accessor(), grantee, &database, perm)
            .await
            .expect("Failed to revoke database permission"),
    }
}

#[apply(generic_step)]
#[step(expr = "ungrant user {string} database role: {schema_defined_role}{may_error}")]
async fn ungrant_user_database_role(
    context: &mut Context,
    username: String,
    role: SchemaDefinedRoleParam,
    may_error: MayError,
) {
    let grantee = Grantee::User(User::new(username));
    let result: Result<(), String> = match schema_defined_role_param_to_typedb(context, role).await {
        Err(e) => Err(e),
        Ok(revoked) => context
            .server_state()
            .users()
            .revoke_role(admin_accessor(), grantee, revoked)
            .await
            .map_err(|e| format!("{e:?}")),
    };
    may_error.check(result);
}

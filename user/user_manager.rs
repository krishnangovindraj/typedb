/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::{Database, database::DatabaseUid};
use resource::constants::server::DEFAULT_USER_NAME;
use storage::durability_client::WALClient;
use system::{
    concepts::{Credential, DatabasePermission, Grantee, SchemaDefinedRole, SystemPermission, User},
    repositories::{SystemDBError, rbac_repository, user_repository},
    util::transaction_util::TransactionUtil,
};
use uuid::Uuid;

use crate::errors::{PermissionManagementError, UserCreateError, UserDeleteError, UserGetError, UserUpdateError};

#[derive(Debug)]
pub struct UserManager {
    transaction_util: TransactionUtil,
}

impl UserManager {
    pub fn new(system_db: Arc<Database<WALClient>>) -> Self {
        UserManager { transaction_util: TransactionUtil::new(system_db.clone()) }
    }

    pub fn all(&self) -> Vec<User> {
        self.transaction_util.read_transaction(user_repository::list)
    }

    pub fn get(&self, username: &str) -> Result<Option<(User, Credential)>, UserGetError> {
        self.transaction_util.read_transaction(|tx| {
            user_repository::get(tx, username).map_err(|query_error| match query_error {
                SystemDBError::IllegalQueryInput { .. } => UserGetError::IllegalUsername {},
                SystemDBError::EmptyUpdate { .. }
                | SystemDBError::QueryFailed { .. }
                | SystemDBError::UnexpectedParseFailed { .. } => UserGetError::Unexpected {},
            })
        })
    }

    pub fn contains(&self, username: &str) -> Result<bool, UserGetError> {
        self.get(username).map(|opt| opt.is_some())
    }

    pub fn create(
        &self,
        user: &User,
        credential: &Credential,
        user_uuid: Uuid,
        credential_uuid: Uuid,
    ) -> Result<(), UserCreateError> {
        if self.contains(&user.name)? {
            return Err(UserCreateError::UserAlreadyExist {});
        }
        let create_result = self
            .transaction_util
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::create(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    user,
                    credential,
                    user_uuid,
                    credential_uuid,
                )
            })
            .1;
        match create_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(UserCreateError::IllegalUsername {}),
            Err(_) => Err(UserCreateError::Unexpected {}),
        }
    }

    pub fn update(
        &self,
        username: &str,
        user: &Option<User>,
        credential: &Option<Credential>,
    ) -> Result<(), UserUpdateError> {
        if !self.contains(username)? {
            return Err(UserUpdateError::UserNotFound {});
        }

        let update_result = self
            .transaction_util
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::update(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    username,
                    user,
                    credential,
                )
            })
            .1;
        match update_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(UserUpdateError::IllegalUsername {}),
            Err(_) => Err(UserUpdateError::Unexpected {}),
        }
    }

    pub fn delete(&self, username: &str) -> Result<(), UserDeleteError> {
        if username == DEFAULT_USER_NAME {
            return Err(UserDeleteError::DefaultUserCannotBeDeleted {});
        }
        if !self.contains(username)? {
            return Err(UserDeleteError::UserNotFound {});
        }

        let delete_result = self
            .transaction_util
            .write_transaction(|snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                user_repository::delete(snapshot, &type_mgr, thing_mgr.clone(), fn_mgr.clone(), &query_mgr, username)
            })
            .1;
        match delete_result {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(UserDeleteError::IllegalUsername {}),
            Err(_) => Err(UserDeleteError::Unexpected {}),
        }
    }

    pub fn grant_system_permission(
        &self,
        grantee: &Grantee,
        granted_permission: &SystemPermission,
    ) -> Result<(), PermissionManagementError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_system_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    granted_permission,
                )
            })
            .1;
        match grant_result {
            Ok(Ok(_row_count)) => {
                debug_assert!(*_row_count >= 1);
                Ok(())
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn grant_database_permission(
        &self,
        grantee: &Grantee,
        granted_permission: &DatabasePermission,
    ) -> Result<(), PermissionManagementError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_database_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    granted_permission,
                )
            })
            .1;
        match grant_result {
            Ok(Ok(_row_count)) => {
                debug_assert!(*_row_count >= 1);
                Ok(())
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn list_system_permissions(
        &self,
        grantee: &Grantee,
    ) -> Result<Vec<SystemPermission>, PermissionManagementError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_system_permissions(tx, grantee))
            .map_err(|typedb_source| PermissionManagementError::UnexpectedSystemDB { typedb_source })
    }

    pub fn list_database_permissions(
        &self,
        grantee: &Grantee,
        database_id: DatabaseUid,
    ) -> Result<Vec<DatabasePermission>, PermissionManagementError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_database_permissions(tx, grantee, database_id))
            .map_err(|typedb_source| PermissionManagementError::UnexpectedSystemDB { typedb_source })
    }

    pub fn grant_role(
        &self,
        grantee: &Grantee,
        granted_role: &SchemaDefinedRole,
    ) -> Result<(), PermissionManagementError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_role(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    granted_role,
                )
            })
            .1;
        match grant_result {
            Ok(Ok(_row_count)) => {
                debug_assert!(*_row_count >= 1);
                Ok(())
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn list_roles(
        &self,
        grantee: &Grantee,
        database: DatabaseUid,
    ) -> Result<Vec<SchemaDefinedRole>, PermissionManagementError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_roles(tx, grantee))
            .map_err(|typedb_source| PermissionManagementError::UnexpectedSystemDB { typedb_source })
    }

    pub fn revoke_system_permission(
        &self,
        grantee: &Grantee,
        permission: &SystemPermission,
    ) -> Result<(), PermissionManagementError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_system_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    permission,
                )
            })
            .1;
        match revoke_result {
            Ok(Ok(_row_count)) => {
                if *_row_count >= 1 {
                    Ok(())
                } else {
                    Err(PermissionManagementError::GranteeDidNotHaveRevokedSystemPermission {
                        grantee: grantee.clone(),
                        permission: permission.clone(),
                    })
                }
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn revoke_database_permission(
        &self,
        grantee: &Grantee,
        permission: &DatabasePermission,
    ) -> Result<(), PermissionManagementError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_database_permission(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    permission,
                )
            })
            .1;
        match revoke_result {
            Ok(Ok(_row_count)) => {
                if *_row_count >= 1 {
                    Ok(())
                } else {
                    Err(PermissionManagementError::GranteeDidNotHaveRevokedDatabasePermission {
                        grantee: grantee.clone(),
                        permission: permission.clone(),
                    })
                }
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn revoke_role(&self, grantee: &Grantee, role: &SchemaDefinedRole) -> Result<(), PermissionManagementError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_role(
                    snapshot,
                    &type_mgr,
                    thing_mgr.clone(),
                    fn_mgr.clone(),
                    &query_mgr,
                    grantee,
                    role,
                )
            })
            .1;
        match revoke_result {
            Ok(Ok(_row_count)) => {
                if *_row_count >= 1 {
                    Ok(())
                } else {
                    Err(PermissionManagementError::GranteeDidNotHaveRevokedRole {
                        grantee: grantee.clone(),
                        role: role.clone(),
                    })
                }
            }
            Ok(Err(typedb_source)) => Err(PermissionManagementError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionManagementError::UnexpectedDataCommit { typedb_source }),
        }
    }
}

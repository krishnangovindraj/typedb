/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;

use database::Database;
use resource::constants::server::DEFAULT_USER_NAME;
use storage::durability_client::WALClient;
use system::{
    concepts::{Credential, Grantee, SchemaDefinedRole, SystemPermission, User},
    repositories::{SystemDBError, rbac_repository, user_repository},
    util::transaction_util::TransactionUtil,
};
use uuid::Uuid;

use crate::errors::{
    PermissionGrantError, PermissionListError, PermissionRevokeError, RoleGrantError, RoleListError, RoleRevokeError,
    UserCreateError, UserDeleteError, UserGetError, UserUpdateError,
};

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

    pub fn grant_permission(
        &self,
        grantee: &Grantee,
        granted_permission: &SystemPermission,
    ) -> Result<(), PermissionGrantError> {
        let grant_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::grant_permission(
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
            },
            Ok(Err(typedb_source)) => Err(PermissionGrantError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionGrantError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn list_permissions(&self, grantee: &Grantee) -> Result<Vec<SystemPermission>, PermissionListError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_permissions(tx, grantee))
            .map_err(|typedb_source| PermissionListError::UnexpectedSystemDB { typedb_source })
    }

    pub fn grant_role(&self, grantee: &Grantee, granted_role: &SchemaDefinedRole) -> Result<(), RoleGrantError> {
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
            },
            Ok(Err(typedb_source)) => Err(RoleGrantError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(RoleGrantError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn list_roles(&self, grantee: &Grantee) -> Result<Vec<SchemaDefinedRole>, RoleListError> {
        self.transaction_util
            .read_transaction(move |tx| rbac_repository::list_roles(tx, grantee))
            .map_err(|typedb_source| RoleListError::UnexpectedSystemDB { typedb_source })
    }

    pub fn revoke_permission(
        &self,
        grantee: &Grantee,
        permission: &SystemPermission,
    ) -> Result<(), PermissionRevokeError> {
        let revoke_result = self
            .transaction_util
            .write_transaction(move |snapshot, type_mgr, thing_mgr, fn_mgr, query_mgr, _db, _tx_opts| {
                rbac_repository::revoke_permission(
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
                debug_assert!(*_row_count >= 1);
                Ok(())
            },
            Ok(Err(typedb_source)) => Err(PermissionRevokeError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(PermissionRevokeError::UnexpectedDataCommit { typedb_source }),
        }
    }

    pub fn revoke_role(&self, grantee: &Grantee, role: &SchemaDefinedRole) -> Result<(), RoleRevokeError> {
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
                debug_assert!(*_row_count >= 1);
                Ok(())
            },
            Ok(Err(typedb_source)) => Err(RoleRevokeError::UnexpectedSystemDB { typedb_source }),
            Err(typedb_source) => Err(RoleRevokeError::UnexpectedDataCommit { typedb_source }),
        }
    }
}

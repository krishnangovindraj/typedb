/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

pub const SCHEMA: &str = include_str!("schema.tql");

fn is_valid_typeql_value(value: &str) -> bool {
    typeql::common::identifier::is_valid_label(value)
}

pub mod user_repository {
    use std::{collections::HashMap, sync::Arc};

    use answer::variable_value::VariableValue;
    use concept::{thing::thing_manager, type_::type_manager::TypeManager};
    use database::transaction::TransactionRead;
    use error::typedb_error;
    use function::function_manager::FunctionManager;
    use query::{error::QueryError, query_manager::QueryManager};
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use thing_manager::ThingManager;
    use typeql::parse_query;
    use uuid::Uuid;

    use crate::{
        concepts::{Credential, PasswordHash, User},
        repositories::is_valid_typeql_value,
        util::{
            answer_util::get_string,
            query_util::{execute_read_pipeline, execute_write_pipeline},
        },
    };

    pub fn list(tx: TransactionRead<WALClient>) -> Vec<User> {
        let unexpected_error_msg = "An unexpected error occurred when acquiring the list of users";
        let query_str = "match (user: $u, credentials: $c) isa user-credentials; $u has name $n;";
        let query = parse_query(query_str).expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), query_str);
        let rows = result.expect(unexpected_error_msg);
        let users = rows.iter().map(|row| User::new(get_string(&tx, row, "n"))).collect();
        users
    }

    pub fn get(tx: TransactionRead<WALClient>, username: &str) -> Result<Option<(User, Credential)>, SystemDBError> {
        if !is_valid_typeql_value(username) {
            return Err(SystemDBError::IllegalQueryInput {});
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to retrieve a user";
        let query_str = format!(
            "match
                (user: $u, credentials: $p) isa user-credentials;
                $u has name '{username}';
                $p has hash $h;"
        );
        let query = parse_query(&query_str).expect(unexpected_error_msg);
        let (tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), &query_str);
        let mut rows: Vec<HashMap<String, VariableValue>> = match result {
            Ok(rows) => rows,
            Err(_) => return Err(SystemDBError::QueryFailed {}),
        };
        if !rows.is_empty() {
            let row = rows.pop().expect(unexpected_error_msg);
            let hash = get_string(&tx, &row, "h");
            Ok(Some((
                User::new(username.to_string()),
                Credential::PasswordType { password_hash: PasswordHash::new(hash) },
            )))
        } else {
            Ok(None)
        }
    }

    pub fn create(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        user: &User,
        credentials: &Credential,
        user_uuid: Uuid,
        credential_uuid: Uuid,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        if !is_valid_typeql_value(&user.name) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to create a new user";
        let (query, query_string) = match credentials {
            Credential::PasswordType { password_hash: PasswordHash { value: hash } } => {
                let query_string = format!(
                    "insert $u isa user, has uuid '{user_uuid}', has name '{name}';
                        $p isa password, has uuid '{cred_uuid}', has hash '{hash}';
                        (user: $u, credentials: $p) isa user-credentials;",
                    user_uuid = user_uuid,
                    cred_uuid = credential_uuid,
                    name = user.name,
                    hash = hash
                );
                (parse_query(query_string.as_str()).expect(unexpected_error_msg), query_string)
            }
        };
        let (_, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            &query_string,
        );
        (Ok(()), snapshot)
    }

    // TODO: support updating user's username
    pub fn update(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        username: &str,
        _user: &Option<User>,
        credential: &Option<Credential>,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let unexpected_error_msg = "An unexpected error occurred when attempting to update a user";
        match credential {
            Some(Credential::PasswordType { password_hash: PasswordHash { value: hash } }) => {
                let query_string = format!(
                    "match (user: $u, credentials: $p) isa user-credentials; $u has name '{username}'; $p has hash $h; delete has $h of $p; insert $p has hash '{password_hash}';",
                    username = username,
                    password_hash = hash
                );
                let query = parse_query(&query_string).expect(unexpected_error_msg);
                let (_, snapshot) = execute_write_pipeline(
                    snapshot,
                    type_manager,
                    thing_manager,
                    function_manager,
                    query_manager,
                    query.into_structure().into_pipeline(),
                    &query_string,
                );
                (Ok(()), snapshot)
            }
            None => (Err(SystemDBError::EmptyUpdate {}), Arc::new(snapshot)),
        }
    }

    pub fn delete(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        username: &str,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        if !is_valid_typeql_value(username) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let unexpected_error_msg = "An unexpected error occurred when attempting to delete a user";
        let query_string = format!(
            "match $uc isa user-credentials, links (user: $u, credentials: $c);
                $u isa user, has name '{username}';
                delete $u; $c; $uc;",
            username = username
        );
        let query = parse_query(&query_string).expect(unexpected_error_msg);
        let (result, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            &query_string,
        );
        match result {
            Ok(_) => (Ok(()), snapshot),
            Err(typedb_source) => (Err(SystemDBError::QueryFailedBecause { typedb_source }), snapshot),
        }
    }

    typedb_error! {
        pub SystemDBError(component = "System database", prefix = "SDB") {
            EmptyUpdate(1, "There is nothing to update"),
            IllegalQueryInput(2, "The specified input contains one or more illegal character(s)"),
            QueryFailed(3, "System database query could not be executed. The system database may not be fully initialised yet."),
            UnexpectedParseFailed(4, "Parsing a system database query failed unexpectedly"),
            QueryFailedBecause(5, "System database query could not be executed.", typedb_source: Box<QueryError>),
        }
    }
}

pub mod rbac_repository {
    use std::sync::Arc;

    use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
    use function::function_manager::FunctionManager;
    use query::query_manager::QueryManager;
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use typeql::parse_query;

    use crate::{
        concepts::{DatabaseUid, GrantedPermission, Grantee, RoleUid},
        repositories::{is_valid_typeql_value, user_repository::SystemDBError},
        util::query_util::execute_write_pipeline,
    };

    const DATABASE_VARIABLE: &'static str = "database";
    const GRANTED_PERMISSION_VARIABLE: &'static str = "granted-variable";
    const GRANTEE_VARIABLE: &'static str = "grantee";

    pub fn grant(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        granted_permission: &GrantedPermission,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let grantee_type = grantee.type_to_typeql_value_string();
        let grantee_name = grantee.name_to_typeql_value_string();
        if !is_valid_typeql_value(grantee_name.as_str()) {
            return (Err(SystemDBError::IllegalQueryInput {}), Arc::new(snapshot));
        }
        let permission_stub = permission_typeql_stub(&granted_permission);
        let query_string = format!(
            r#"
            match $u isa {grantee_type}, has name "{grantee_name}";
            put {permission_stub}"#,
        );
        let Ok(query) = parse_query(query_string.as_str()) else {
            return (Err(SystemDBError::UnexpectedParseFailed {}), Arc::new(snapshot));
        };
        let (_, snapshot) = execute_write_pipeline(
            snapshot,
            type_manager,
            thing_manager,
            function_manager,
            query_manager,
            query.into_structure().into_pipeline(),
            &query_string,
        );
        (Ok(()), snapshot)
    }

    fn permission_typeql_stub(granted_permission: &GrantedPermission) -> String {
        let permission_type = granted_permission.type_to_typeql_string();
        match granted_permission {
            GrantedPermission::ManageDatabases
            | GrantedPermission::ManageUsers
            | GrantedPermission::GrantPermissions => global_permission_typeql_stub(permission_type),
            GrantedPermission::TransactionRead { database }
            | GrantedPermission::TransactionWrite { database }
            | GrantedPermission::SchemaTypes { database }
            | GrantedPermission::SchemaRoles { database } => database_permission_typeql_stub(permission_type, database),
            GrantedPermission::Role { database, role } => role_permission_typeql_stub(permission_type, database, role),
        }
    }

    fn global_permission_typeql_stub(permission: &str) -> String {
        format!(
            r#"
            ${GRANTED_PERMISSION_VARIABLE} isa {permission},
                links (grantee: ${GRANTEE_VARIABLE});
            "#
        )
    }

    fn database_permission_typeql_stub(permission: &str, database: &DatabaseUid) -> String {
        let database_id = database.to_typeql_value_string();
        format!(
            r#"
            ${DATABASE_VARIABLE} isa database, has database-id "{database_id}";
            ${GRANTED_PERMISSION_VARIABLE} isa {permission},
                links (grantee: ${GRANTEE_VARIABLE}, database: ${DATABASE_VARIABLE});
        "#
        )
    }

    fn role_permission_typeql_stub(permission: &str, database: &DatabaseUid, role: &RoleUid) -> String {
        let database_id = database.to_typeql_value_string();
        let role_id = role.to_typeql_value_string();
        format!(
            r#"
            ${DATABASE_VARIABLE} isa database, has database-id "{database_id}";
            ${GRANTED_PERMISSION_VARIABLE} isa {permission},
                links (grantee: ${GRANTEE_VARIABLE}, database: ${DATABASE_VARIABLE}),
                has role-id {role_id};
        "#
        )
    }
}

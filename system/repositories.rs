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
    use std::{
        collections::{HashMap, HashSet},
        sync::Arc,
    };

    use answer::variable_value::VariableValue;
    use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
    use database::transaction::TransactionRead;
    use function::function_manager::FunctionManager;
    use query::query_manager::QueryManager;
    use storage::{durability_client::WALClient, snapshot::WriteSnapshot};
    use typeql::parse_query;

    use crate::{
        concepts::{DatabaseUid, GrantedPermission, Grantee, RoleUid},
        repositories::{is_valid_typeql_value, user_repository::SystemDBError},
        util::query_util::{execute_read_pipeline, execute_write_pipeline},
    };

    const GRANTEE_NAME_VALUE_VARIABLE: &'static str = "grantee-name-value";
    const DATABASE_ID_VALUE_VARIABLE: &'static str = "database-id-value";
    const ROLE_ID_VALUE_VARIABLE: &'static str = "role-id-value";
    const PERMISSION_LABEL_VALUE_VARIABLE: &'static str = "permission-type";

    const GRANTEE_TYPE_VARIABLE: &'static str = "grantee-type";
    const PERMISSION_TYPE_VARIABLE: &'static str = "permission-type";

    const DATABASE_VARIABLE: &'static str = "database";
    const GRANTEE_VARIABLE: &'static str = "grantee";
    const GRANTED_PERMISSION_VARIABLE: &'static str = "granted-variable";

    pub fn list_permissions(
        tx: TransactionRead<WALClient>,
        grantee: &Grantee,
    ) -> Result<Vec<GrantedPermission>, SystemDBError> {
        let unexpected_error_msg = "An unexpected error occurred while listing user permissions";
        let query_string = build_list_permissions_query(grantee)?;
        let query = parse_query(query_string.as_str()).expect(unexpected_error_msg);
        let (_tx, result) = execute_read_pipeline(tx, query.into_structure().into_pipeline(), &query_string);
        let rows: Vec<HashMap<String, VariableValue>> = match result {
            Ok(rows) => rows,
            Err(_) => return Err(SystemDBError::QueryFailed {}),
        };
        let permissions = rows.into_iter().filter_map(|row| try_valid_granted_permission_from_row(row)).collect();
        Ok(permissions)
    }

    pub fn grant(
        snapshot: WriteSnapshot<WALClient>,
        type_manager: &TypeManager,
        thing_manager: Arc<ThingManager>,
        function_manager: Arc<FunctionManager>,
        query_manager: &QueryManager,
        grantee: &Grantee,
        granted_permission: &GrantedPermission,
    ) -> (Result<(), SystemDBError>, Arc<WriteSnapshot<WALClient>>) {
        let query_string = match build_grant_permission_query(grantee, granted_permission) {
            Ok(query_string) => query_string,
            Err(err) => return (Err(err), Arc::new(snapshot)),
        };
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
            query_string.as_str(),
        );
        (Ok(()), snapshot)
    }

    fn check_valid_value<T: AsRef<str>>(value: T) -> Result<T, SystemDBError> {
        match is_valid_typeql_value(value.as_ref()) {
            true => Ok(value),
            false => Err(SystemDBError::IllegalQueryInput {}),
        }
    }

    fn try_valid_granted_permission_from_row(row: HashMap<String, VariableValue>) -> Option<GrantedPermission> {
        fn extract_database_value(database_id_value_opt: &VariableValue<'_>) -> Option<DatabaseUid> {
            let VariableValue::Value(database_id_value) = database_id_value_opt else {
                debug_assert!(
                    false,
                    "Unexpected VariableValue for database_id: {}",
                    database_id_value_opt.variant_name()
                );
                return None;
            };
            match uuid::Uuid::parse_str(database_id_value.unwrap_string_ref()) {
                Err(err) => {
                    debug_assert!(
                        false,
                        "DatabaseUid did not parse: {}. Reason: {}",
                        database_id_value_opt.as_value().unwrap_string_ref(),
                        err.to_string()
                    );
                    None
                }
                Ok(uuid) => Some(DatabaseUid(uuid)),
            }
        }
        fn extract_role_value(role_id_value_opt: &VariableValue<'_>) -> Option<RoleUid> {
            let VariableValue::Value(role_id_value) = role_id_value_opt else {
                debug_assert!(false, "Unexpected VariableValue for role_id: {}", role_id_value_opt.variant_name());
                return None;
            };
            Some(RoleUid(role_id_value.to_owned().unwrap_integer() as u64))
        }
        let permission_value =
            row.get(PERMISSION_LABEL_VALUE_VARIABLE).expect("Expected permission label value").as_value();
        let database_id_value_opt = row.get(DATABASE_ID_VALUE_VARIABLE).expect("Expected database id value");
        let role_id_value_opt = row.get(ROLE_ID_VALUE_VARIABLE).expect("Expected role id value");
        match permission_value.unwrap_string_ref() {
            "manage-databases" => Some(GrantedPermission::ManageDatabases),
            "manage-users" => Some(GrantedPermission::ManageUsers),
            "grant-permissions" => Some(GrantedPermission::GrantPermissions),
            "transaction-read" => {
                debug_assert!(role_id_value_opt.is_none());
                let database = extract_database_value(database_id_value_opt)?;
                Some(GrantedPermission::TransactionRead { database })
            }
            "transaction-write" => {
                debug_assert!(role_id_value_opt.is_none());
                let database = extract_database_value(database_id_value_opt)?;
                Some(GrantedPermission::TransactionWrite { database })
            }
            "schema-types" => {
                debug_assert!(role_id_value_opt.is_none());
                let database = extract_database_value(database_id_value_opt)?;
                Some(GrantedPermission::SchemaTypes { database })
            }
            "schema-roles" => {
                debug_assert!(role_id_value_opt.is_none());
                let database = extract_database_value(database_id_value_opt)?;
                Some(GrantedPermission::SchemaRoles { database })
            }
            "role-assignment" => {
                let database = extract_database_value(database_id_value_opt)?;
                let role = extract_role_value(role_id_value_opt)?;
                Some(GrantedPermission::Role { database, role })
            }
            _other => {
                debug_assert!(false, "Unrecognised permission label: {_other}");
                None
            }
        }
    }

    fn build_list_permissions_query(grantee: &Grantee) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;
        let query_string = format!(
            r#"
            match
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
            match
                # grantee
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};

                # permission type
                ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE}, links (grantee: ${GRANTEE_VARIABLE});
                let ${PERMISSION_LABEL_VALUE_VARIABLE} = label(${PERMISSION_TYPE_VARIABLE});
                try {{ # database if it has one
                    ${DATABASE_VARIABLE} isa database, has database-id $database-id;
                    ${GRANTED_PERMISSION_VARIABLE} links (database: ${DATABASE_VARIABLE});
                    let ${DATABASE_ID_VALUE_VARIABLE} = $database-id;
                    try {{
                        # role if it has one
                        ${GRANTED_PERMISSION_VARIABLE} has role-id == $role-id-attr;
                        let ${ROLE_ID_VALUE_VARIABLE} = $role-id-attr;
                    }};
                }};
                select ${PERMISSION_LABEL_VALUE_VARIABLE}, ${DATABASE_ID_VALUE_VARIABLE}, ${ROLE_ID_VALUE_VARIABLE},
            "#,
        );
        Ok(query_string)
    }

    fn build_grant_permission_query(
        grantee: &Grantee,
        granted_permission: &GrantedPermission,
    ) -> Result<String, SystemDBError> {
        let grantee_type = check_valid_value(grantee.type_to_typeql_value_string())?;
        let grantee_name = check_valid_value(grantee.name_to_typeql_value_string())?;

        let permission_stub = format!(
            r#"
            ${GRANTED_PERMISSION_VARIABLE} isa! ${PERMISSION_TYPE_VARIABLE}, links (grantee: ${GRANTEE_VARIABLE});
        "#
        );
        let (database_value_stub, database_put_stub) = match granted_permission.database() {
            None => ("".to_owned(), "".to_owned()),
            Some(database_id) => (
                format!(r#"let ${DATABASE_ID_VALUE_VARIABLE} = "{id}";"#, id = database_id.0),
                format!(
                    r#"
                    ${DATABASE_VARIABLE} isa database, has database-id == ${DATABASE_ID_VALUE_VARIABLE};
                    ${GRANTED_PERMISSION_VARIABLE} links (database: ${DATABASE_VARIABLE});
                "#
                ),
            ),
        };
        let (role_value_stub, role_put_stub) = match granted_permission.role() {
            None => ("".to_owned(), "".to_owned()),
            Some(role_id) => (
                format!(r#"let ${ROLE_ID_VALUE_VARIABLE} = {id};"#, id = role_id.0),
                format!("${GRANTED_PERMISSION_VARIABLE} has role-id == ${ROLE_ID_VALUE_VARIABLE};"),
            ),
        };

        let query_string = format!(
            r#"
            match
                ${PERMISSION_TYPE_VARIABLE} label {grantee_type};
                let ${GRANTEE_NAME_VALUE_VARIABLE} = "{grantee_name}";
                ${GRANTEE_TYPE_VARIABLE} label {grantee_type};
                {database_value_stub}
                {role_value_stub}
            match
                ${GRANTEE_VARIABLE} isa! ${GRANTEE_TYPE_VARIABLE}, has name == ${GRANTEE_NAME_VALUE_VARIABLE};
            put
                {permission_stub}
                {database_put_stub}
                {role_put_stub}
        "#
        );
        Ok(query_string)
    }
}

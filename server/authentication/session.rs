/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::collections::HashMap;
use tokio::time::Instant;

type DatabaseID = String;
type DatabaseRole = String; // This has to be a type of its own like Entity.

pub enum GlobalPermissions {
    ManageUsers,
    ManageDatabases,
    GrantPermissions,
    AssignRoles,
}

pub enum DatabasePermissions {
    TransactionRead,
    TransactionWrite,
    ModifySchema,
    Role(DatabaseRole),
}

struct UserSession {
    created: Instant,
    global_permissions: Vec<GlobalPermissions>,
    database_permissions: HashMap<DatabaseID, DatabasePermissions>,
}

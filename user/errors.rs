/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use error::typedb_error;
use system::repositories::user_repository::SystemDBError;

typedb_error! {
    pub UserGetError(component = "User get", prefix = "USG") {
        IllegalUsername(1, "Invalid credential supplied."),
        Unexpected(2, "An unexpected error has occurred in the process of getting a user."),
    }
}

typedb_error! {
    pub UserCreateError(component = "User create", prefix = "USC") {
        IllegalUsername(1, "Invalid credential supplied."),
        UserAlreadyExist(2, "User already exists."),
        IncompleteUserDetail(3, "Incomplete user detail."),
        Unexpected(4, "An unexpected error has occurred in the process of creating a new user."),
        GetFailed(5, "Failed to check user existence.", typedb_source: UserGetError),
    }
}

typedb_error! {
    pub UserUpdateError(component = "User update", prefix = "USU") {
        UserDetailNotProvided(1, "User detail not provided."),
        IllegalUsername(2, "Invalid credential supplied,"),
        Unexpected(3, "An unexpected error has occurred in the process of updating a user."),
        UserNotFound(4, "User not found."),
        GetFailed(5, "Failed to check user existence.", typedb_source: UserGetError),
    }
}

typedb_error! {
    pub UserDeleteError(component = "User delete", prefix = "USD") {
        DefaultUserCannotBeDeleted(1, "Default user cannot be deleted."),
        IllegalUsername(2, "Invalid credential supplied."),
        UserNotFound(3, "User not found."),
        Unexpected(4, "An unexpected error has occurred in the process of deleting a user."),
        GetFailed(5, "Failed to check user existence.", typedb_source: UserGetError),
    }
}

// Do we really need a different error class per operation?
typedb_error! {
    pub PermissionGrantError(component = "Permission management", prefix="UPG") {
        Unexpected(1, "An unexpected error has occurred while managing user permissions."),
        SystemDB(2, "An error occured while managing user permissions.", typedb_source: SystemDBError),
    }
}

typedb_error! {
    pub PermissionListError(component = "Permission listing", prefix="UPL") {
        Unexpected(1, "An unexpected error has occurred while listing user permissions."),
        SystemDB(2, "An error occured while managing user permissions.", typedb_source: SystemDBError),
    }
}

typedb_error! {
    pub RoleGrantError(component = "Role management", prefix="URG") {
        Unexpected(1, "An unexpected error has occurred while listing user permissions."),
        SystemDB(2, "An error occured while managing user permissions.", typedb_source: SystemDBError),
    }
}

typedb_error! {
    pub RoleListError(component = "Role Listing", prefix="URL") {
        Unexpected(1, "An unexpected error has occurred while listing user permissions."),
        SystemDB(2, "An error occured while managing user permissions.", typedb_source: SystemDBError),
    }
}

impl From<UserGetError> for UserCreateError {
    fn from(err: UserGetError) -> Self {
        match err {
            UserGetError::IllegalUsername { .. } => UserCreateError::IllegalUsername {},
            UserGetError::Unexpected { .. } => UserCreateError::GetFailed { typedb_source: err },
        }
    }
}

impl From<UserGetError> for UserUpdateError {
    fn from(err: UserGetError) -> Self {
        match err {
            UserGetError::IllegalUsername { .. } => UserUpdateError::IllegalUsername {},
            UserGetError::Unexpected { .. } => UserUpdateError::GetFailed { typedb_source: err },
        }
    }
}

impl From<UserGetError> for UserDeleteError {
    fn from(err: UserGetError) -> Self {
        match err {
            UserGetError::IllegalUsername { .. } => UserDeleteError::IllegalUsername {},
            UserGetError::Unexpected { .. } => UserDeleteError::GetFailed { typedb_source: err },
        }
    }
}

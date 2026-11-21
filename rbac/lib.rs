/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::error::ConceptReadError;
use encoding::error::EncodingError;
use error::typedb_error;

pub mod rbac_cache;
pub mod rbac_manager;

typedb_error! {
    pub RBACError(component = "RBAC", prefix = "RBC") {
        RBACRoleAlreadyExists(1, "An RBAC role with name '{name}' already exists.", name: String),
        CreateRBACRoleEncoding(2, "Encoding error while trying to create RBAC role.", source: EncodingError),
        RBACRoleNotFound(3, "RBAC role was not found."),
        ConceptRead(4, "Concept read failed.", typedb_source: Box<ConceptReadError>),
    }
}

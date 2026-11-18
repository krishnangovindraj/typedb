/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashSet;

use concept::{
    rbac::{RBACAccess, RBACRole, check_role_has_access},
    type_::object_type::ObjectType,
};
use cucumber::gherkin::Step;
use encoding::value::label::Label;
use macro_rules_attribute::apply;
use resource::profile::StorageCounters;

use crate::{Context, generic_step, transaction_context::with_read_tx, util::iter_table_map};

#[apply(generic_step)]
#[step(expr = "list rbac declared access for role: {word}")]
async fn rbac_role_has_direct_grants(context: &mut Context, role_name: String, step: &Step) {
    with_read_tx!(context, |tx| {
        let role = tx
            .type_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_name, StorageCounters::DISABLED)
            .expect("Error getting RBAC role")
            .unwrap_or_else(|| panic!("RBAC role '{role_name}' not found"));

        let declared_roles = tx
            .type_manager
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
            .type_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_name, StorageCounters::DISABLED)
            .expect("Error getting RBAC role")
            .unwrap_or_else(|| panic!("RBAC role '{role_name}' not found"));

        let type_label = Label::build(&type_name, None);
        params::check_boolean!(
            expected_has_access,
            check_rbac_access_on_type(&tx.type_manager, tx.snapshot.as_ref(), &role, &type_label, &access)
        );
    });
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
    snapshot: &impl storage::snapshot::ReadableSnapshot,
    role: &RBACRole,
    type_label: &Label,
    access: &str,
) -> bool {
    let access = resolve_rbac_access(type_manager, snapshot, type_label, access)
        .unwrap_or_else(|| panic!("Could not resolve type '{type_label}'"));
    check_role_has_access(snapshot, type_manager, role, &access, StorageCounters::DISABLED).unwrap()
}

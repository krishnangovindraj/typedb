/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use encoding::value::label::Label;
use macro_rules_attribute::apply;
use resource::profile::StorageCounters;

use crate::{
    Context, generic_step,
    transaction_context::with_read_tx,
    util::iter_table_map,
};

#[apply(generic_step)]
#[step("list rbac declared access for role: {string}")]
async fn rbac_role_has_direct_grants(context: &mut Context, role_name: String, step: &Step) {
    with_read_tx!(context, |tx| {
        let role_label = Label::build(&role_name);
        let role = tx
            .type_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_label, StorageCounters::DISABLED)
            .expect("Error getting RBAC role")
            .unwrap_or_else(|| panic!("RBAC role '{role_name}' not found"));

        let decalred_roles = tx
            .type_manager
            .list_rbac_declared_access(tx.snapshot.as_ref(), &role, StorageCounters::DISABLED)
            .expect("Error getting reading RBAC permissions for role");
    });
}

#[apply(generic_step)]
#[step("rbac role {string} has {word} access on type: {word}")]
async fn rbac_role_has_access_on_type(context: &mut Context, role_name: String, access: String, type_name: String) {
    with_read_tx!(context, |tx| {
        let role_label = Label::build(&role_name);
        let role = tx
            .type_manager
            .get_rbac_role(tx.snapshot.as_ref(), &role_label, StorageCounters::DISABLED)
            .expect("Error getting RBAC role")
            .unwrap_or_else(|| panic!("RBAC role '{role_name}' not found"));

        let type_label = Label::build(&type_name);
        let has_access = check_rbac_access_on_type(&tx.type_manager, tx.snapshot.as_ref(), &role, &type_label, &access)
            .unwrap_or_else(|| panic!("Type '{type_name}' not found"));

        assert!(has_access, "Expected RBAC role '{role_name}' to have {access} access on '{type_name}'");
    });
}

fn check_rbac_access_on_type(
    type_manager: &concept::type_::type_manager::TypeManager,
    snapshot: &impl storage::snapshot::ReadableSnapshot,
    role: &encoding::graph::rbac::RBACRoleVertex,
    type_label: &Label,
    access: &str,
) -> Option<bool> {
    let check = |has_read: bool, has_write: bool| match access {
        "read" => has_read,
        "write" => has_write,
        other => panic!("Unknown access type '{other}'; expected 'read' or 'write'"),
    };

    if let Some(t) = type_manager.get_entity_type(snapshot, type_label).expect("Error getting entity type") {
        let has_read = type_manager.has_rbac_read_access(snapshot, role, t, StorageCounters::DISABLED).expect("Error checking RBAC access");
        let has_write = type_manager.has_rbac_write_access(snapshot, role, t, StorageCounters::DISABLED).expect("Error checking RBAC access");
        return Some(check(has_read, has_write));
    }
    if let Some(t) = type_manager.get_relation_type(snapshot, type_label).expect("Error getting relation type") {
        let has_read = type_manager.has_rbac_read_access(snapshot, role, t, StorageCounters::DISABLED).expect("Error checking RBAC access");
        let has_write = type_manager.has_rbac_write_access(snapshot, role, t, StorageCounters::DISABLED).expect("Error checking RBAC access");
        return Some(check(has_read, has_write));
    }
    if let Some(t) = type_manager.get_attribute_type(snapshot, type_label).expect("Error getting attribute type") {
        let has_read = type_manager.has_rbac_read_access(snapshot, role, t, StorageCounters::DISABLED).expect("Error checking RBAC access");
        let has_write = type_manager.has_rbac_write_access(snapshot, role, t, StorageCounters::DISABLED).expect("Error checking RBAC access");
        return Some(check(has_read, has_write));
    }
    None
}



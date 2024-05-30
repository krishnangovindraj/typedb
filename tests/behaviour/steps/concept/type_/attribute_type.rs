/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use cucumber::gherkin::Step;
use itertools::Itertools;
use macro_rules_attribute::apply;
use concept::type_::object_type::ObjectType;
use concept::type_::TypeAPI;

use crate::{generic_step, params, transaction_context::{with_read_tx, with_schema_tx}, Context, util};
use crate::params::ContainsOrDoesnt;

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) set value-type: {value_type}")]
pub async fn attribute_type_set_value_type(
    context: &mut Context,
    type_label: params::Label,
    value_type: params::ValueType,
) {
    with_schema_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        attribute_type.set_value_type(&mut tx.snapshot, &tx.type_manager, value_type.to_typedb()).unwrap();
    });
}

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) get value type: {value_type}")]
pub async fn attribute_type_get_value_type(
    context: &mut Context,
    type_label: params::Label,
    value_type: params::ValueType,
) {
    with_read_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();
        assert_eq!(
            value_type.to_typedb(),
            attribute_type.get_value_type(&tx.snapshot, &tx.type_manager).unwrap().unwrap()
        );
    });
}

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) get owners {contains_or_doesnt}:")]
pub async fn get_owners_contain(context: &mut Context, type_label: params::Label, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    with_read_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();

        let mut actual_labels = Vec::new();
        attribute_type
                .get_owners_transitive(&tx.snapshot, &tx.type_manager)
                .unwrap()
                .iter()
                .for_each(|(owner, _owns)| {
                    let owner_label = match owner {
                        ObjectType::Entity(owner) => {owner.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_owned()},
                        ObjectType::Relation(owner) => {owner.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_owned()},
                    };
                    actual_labels.push(owner_label);
                });
            contains.check(&expected_labels, &actual_labels);
    });
}

#[apply(generic_step)]
#[step(expr = "attribute\\({type_label}\\) get owners explicit {contains_or_doesnt}:")]
pub async fn get_declaring_owners_contain(context: &mut Context, type_label: params::Label, contains: ContainsOrDoesnt, step: &Step) {
    let expected_labels = util::iter_table(step).map(|str| str.to_owned()).collect_vec();
    with_read_tx!(context, |tx| {
        let attribute_type =
            tx.type_manager.get_attribute_type(&tx.snapshot, &type_label.to_typedb()).unwrap().unwrap();

        let mut actual_labels = Vec::new();
        attribute_type
                .get_owner_owns(&tx.snapshot, &tx.type_manager)
                .unwrap()
                .iter()
                .for_each(|owns| {
                    let owner_label = match owns.owner() {
                        ObjectType::Entity(owner) => {owner.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_owned()},
                        ObjectType::Relation(owner) => {owner.get_label(&tx.snapshot, &tx.type_manager).unwrap().scoped_name().as_str().to_owned()},
                    };
                    actual_labels.push(owner_label);
                });
            contains.check(&expected_labels, &actual_labels);
    });
}

// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners, with annotations: {annotations}; contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners, with annotations: {annotations}; do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit, with annotations: {annotations}; contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
//
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit, with annotations: {annotations}; do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[apply(generic_step)]
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }
// #[step(expr = "attribute\\({type_label}\\)) get owners explicit do not contain:")]
// pub async fn TODO(context: &mut Context, type_label: params::Label, ...) { todo!(); }

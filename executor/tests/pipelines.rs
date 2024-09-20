/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    sync::Arc,
};

use concept::{thing::thing_manager::ThingManager, type_::type_manager::TypeManager};
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::{
    pipeline::{StageAPI, StageIterator},
    ExecutionInterrupt,
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot},
    MVCCStorage,
};
use storage::snapshot::ReadableSnapshot;
use test_utils_concept::{load_managers, setup_concept_storage};
use test_utils_encoding::create_core_storage;

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");

struct Context {
    storage: Arc<MVCCStorage<WALClient>>,
    type_manager: Arc<TypeManager>,
    thing_manager: Arc<ThingManager>,
    function_manager: FunctionManager,
    query_manager: QueryManager,
}

fn setup_common() -> Context {
    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);

    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new();
    let schema = r#"
    define
        attribute age value long;
        attribute name value string;
        entity person owns age @card(0..), owns name @card(0..), plays membership:member;
        entity organisation plays membership:group;
        relation membership relates member, relates group;
    "#;
    let mut snapshot = storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, define).unwrap();
    let seq = snapshot.commit().unwrap();

    // reload to obtain latest vertex generators and statistics entries
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    Context { storage, type_manager, function_manager, query_manager, thing_manager }
}

#[test]
fn test_insert() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = "insert $p isa person, has age 10;";
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let (pipeline, _named_outputs) = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
        )
        .unwrap();
    let (mut iterator, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            context.thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test_insert_insert() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
    insert
        $p isa person, has age 10;
        $org isa organisation;
    insert
        (group: $org, member: $p) isa membership;
    "#;
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let pipeline = context.query_manager.prepare_write_pipeline(
        snapshot,
        &context.type_manager,
        context.thing_manager.clone(),
        &context.function_manager,
        &query,
    );
    if let Err((_, err)) = pipeline {
        dbg!(err);
    }
    //
    // let (mut iterator, snapshot) = pipeline.into_iterator().unwrap();
    // let row = iterator.next();
    // assert!(matches!(&row, &Some(Ok(_))));
    // assert_eq!(row.unwrap().unwrap().len(), 3);
    // assert!(matches!(iterator.next(), None));
    // let snapshot = Arc::into_inner(snapshot).unwrap();
    // snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let membership_type = context.type_manager.get_relation_type(&snapshot, &MEMBERSHIP_LABEL).unwrap().unwrap();
        assert_eq!(context.thing_manager.get_relations_in(&snapshot, membership_type).count(), 1);
        snapshot.close_resources()
    }
}

#[test]
fn test_match() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let query_str = r#"
       insert
       $p isa person, has age 10, has name 'John';
       $q isa person, has age 20, has name 'Alice';
       $r isa person, has age 30, has name 'Harry';
   "#;
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let (pipeline, _named_outputs) = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &query,
        )
        .unwrap();
    let (mut iterator, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let _ = iterator.count();
    // must consume iterator to ensure operation completed
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "match $p isa person;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    let (pipeline, _named_outputs) = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &match_,
        )
        .unwrap();
    let (iterator, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 3);

    let query = "match $person isa person, has name 'John', has age $age;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    let (pipeline, _named_outputs) = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &match_,
        )
        .unwrap();
    let (iterator, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 1);
    let snapshot = Arc::into_inner(snapshot);
}

#[test]
fn test_match_match() {
    todo!()
}

#[test]
fn test_match_delete_has() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 10;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let (insert_pipeline, _named_outputs) = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, snapshot) = insert_pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            context.thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
        snapshot.close_resources()
    }

    let snapshot = context.storage.clone().open_snapshot_write();
    let delete_query_str = r#"
        match $p isa person, has age $a;
        delete has $a of $p;
    "#;

    let delete_query = typeql::parse_query(delete_query_str).unwrap().into_pipeline();
    let (delete_pipeline, _named_outputs) = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &delete_query,
        )
        .unwrap();
    let (mut iterator, snapshot) = delete_pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = context.storage.clone().open_snapshot_read();
        let age_type = context.type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            context.thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(0, attr_age_10.get_owners(&snapshot, &context.thing_manager).count());
        snapshot.close_resources()
    }
}

#[test]
fn test_insert_match_insert() {
    todo!()
}

#[test]
fn test_match_sort() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = "insert $p isa person, has age 1, has age 2, has age 3, has age 4;";
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let (insert_pipeline, _named_outputs) = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, snapshot) = insert_pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let snapshot = Arc::new(context.storage.open_snapshot_read());
    let query = "match $age isa age; sort $age desc;";
    let match_ = typeql::parse_query(query).unwrap().into_pipeline();
    let (pipeline, named_outputs) = context
        .query_manager
        .prepare_read_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &match_,
        )
        .unwrap();
    let (iterator, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    let batch = iterator.collect_owned().unwrap();
    assert_eq!(batch.len(), 4);
    let pos = named_outputs.get("age").unwrap().clone();
    let mut batch_iter = batch.into_iterator_mut();
    let values = batch_iter
        .map_static(move |res| {
            let snapshot_borrow: &ReadSnapshot<WALClient> = &snapshot; // Can't get it to compile inline
            res.get(pos)
                .as_thing()
                .as_attribute()
                .get_value(snapshot_borrow, &context.thing_manager)
                .clone()
                .unwrap()
                .unwrap_long()
        })
        .collect::<Vec<_>>();
    assert_eq!([4, 3, 2, 1], values.as_slice());
}

#[test]
fn test_select() {
    let context = setup_common();
    let snapshot = context.storage.clone().open_snapshot_write();
    let insert_query_str = r#"insert
        $p1 isa person, has name "Alice", has age 1;
        $p2 isa person, has name "Bob", has age 2;"#;
    let insert_query = typeql::parse_query(insert_query_str).unwrap().into_pipeline();
    let (insert_pipeline, _named_outputs) = context
        .query_manager
        .prepare_write_pipeline(
            snapshot,
            &context.type_manager,
            context.thing_manager.clone(),
            &context.function_manager,
            &insert_query,
        )
        .unwrap();
    let (mut iterator, snapshot) = insert_pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();

    assert!(matches!(iterator.next(), Some(Ok(_))));
    assert!(matches!(iterator.next(), None));
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age;";
        let match_ = typeql::parse_query(query).unwrap().into_pipeline();
        let (pipeline, named_outputs) = context
            .query_manager
            .prepare_read_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                &context.function_manager,
                &match_,
            )
            .unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(named_outputs.contains_key("p"));
    }
    {
        let snapshot = Arc::new(context.storage.clone().open_snapshot_read());
        let query = "match $p isa person, has name \"Alice\", has age $age; select $age;";
        let match_ = typeql::parse_query(query).unwrap().into_pipeline();
        let (pipeline, named_outputs) = context
            .query_manager
            .prepare_read_pipeline(
                snapshot,
                &context.type_manager,
                context.thing_manager.clone(),
                &context.function_manager,
                &match_,
            )
            .unwrap();
        assert!(named_outputs.contains_key("age"));
        assert!(!named_outputs.contains_key("p"));
    }
}

fn run_adhoc(storage: Arc<MVCCStorage<WALClient>>, define_opt: Option<&str>, insert_opt: Option<&str>, match_opt: Option<&str>) {
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new();

    if let Some(define_str) = define_opt {
        let mut snapshot = storage.clone().open_snapshot_schema();
        let schema_query = typeql::parse_query(define_str).unwrap().into_schema();
        query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, schema_query).unwrap();
        snapshot.commit().unwrap();
    }
    if let Some(insert_str) = insert_opt {
        let mut snapshot = storage.clone().open_snapshot_write();
        let insert_query = typeql::parse_query(insert_str).unwrap().into_pipeline();
        let (pipeline, outputs) = query_manager.prepare_write_pipeline(snapshot, &type_manager, thing_manager.clone(), &function_manager, &insert_query).unwrap();
        let (it, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        let inserted_batch = it.collect_owned().unwrap();
        assert_eq!(1, inserted_batch.len());
        let snapshot = Arc::into_inner(snapshot).unwrap();
        snapshot.commit().unwrap();
    }

    if let Some(match_str) = match_opt {
        let mut snapshot = Arc::new(storage.clone().open_snapshot_read());
        let match_query = typeql::parse_query(match_str).unwrap().into_pipeline();
        let (pipeline, outputs) = query_manager.prepare_read_pipeline(snapshot.clone(), &type_manager, thing_manager.clone(), &function_manager, &match_query).unwrap();
        let (it, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
        let read_batch = it.collect_owned().unwrap();
        assert_eq!(2, read_batch.len());
        Arc::into_inner(snapshot).unwrap().close_resources();
    }
}

#[test]
fn foo() {

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let (type_manager, thing_manager) = load_managers(storage.clone(), None);
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let query_manager = QueryManager::new();

    let define_str = r#"
    define
        entity user;
        entity group;
        entity admin sub user;
        user owns username;
        group owns name;
        attribute id @abstract, value string;
        attribute username sub id;
        attribute name sub id;
        relation membership @abstract, relates parent, relates member;
        relation group-membership sub membership;
        group-membership relates group as parent;
        group-membership relates group-member as member;
        user plays group-membership:group-member;
        group plays group-membership:group;
    "#;
    let insert_str = r#"
        insert
            $james isa user, has username "james";
            $christoph isa admin, has username "christoph";
            $research isa group, has name "research";
            (group: $research, group-member: $james) isa group-membership;
            (group: $research, group-member: $christoph) isa group-membership;
    "#;
    let mut snapshot = storage.clone().open_snapshot_schema();
    let schema_query = typeql::parse_query(define_str).unwrap().into_schema();
    query_manager.execute_schema(&mut snapshot, &type_manager, &thing_manager, schema_query).unwrap();
    snapshot.commit().unwrap();

    let mut snapshot = storage.clone().open_snapshot_write();
    let insert_query = typeql::parse_query(insert_str).unwrap().into_pipeline();
    let (pipeline, outputs) = query_manager.prepare_write_pipeline(snapshot, &type_manager, thing_manager.clone(), &function_manager, &insert_query).unwrap();
    let (it, snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let inserted_batch = it.collect_owned().unwrap();
    assert_eq!(1, inserted_batch.len());
    let snapshot = Arc::into_inner(snapshot).unwrap();
    snapshot.commit().unwrap();

    let mut snapshot = Arc::new(storage.clone().open_snapshot_read());
    let match_str = r#"
        match
            (group: $group, member: $member) isa group-membership;
    "#;
    let match_query = typeql::parse_query(match_str).unwrap().into_pipeline();
    let (pipeline, outputs) = query_manager.prepare_read_pipeline(snapshot.clone(), &type_manager, thing_manager.clone(), &function_manager, &match_query).unwrap();
    let (it,snapshot) = pipeline.into_iterator(ExecutionInterrupt::new_uninterruptible()).unwrap();
    let read_batch = it.collect_owned().unwrap();
    assert_eq!(2, read_batch.len());
    Arc::into_inner(snapshot).unwrap().close_resources();
}

#[test]
fn foo1() {

    let (_tmp_dir, mut storage) = create_core_storage();
    setup_concept_storage(&mut storage);
    let define_str =
    r#"define
  entity user,
    owns id @key,
    owns name,
    owns birth-date,
    plays purchase:buyer;
  entity order,
    owns id @key,
    owns timestamp,
    owns status,
    plays purchase:order;

  relation purchase,
    relates order,
    relates buyer;

  attribute id, value string;
  attribute name, value string;
  attribute birth-date, value date;
  attribute timestamp, value datetime;
  attribute status,
    value string
    @regex("^(paid|dispatched|delivered|returned|canceled)$");"#;

    let insert_str = r#"
    insert
  $user-1 isa user,
    has id "u0001",
    has name "Kevin Morrison",
    has birth-date 1995-10-29;
  $user-2 isa user,
    has id "u0002",
    has name "Cameron Osborne",
    has birth-date 1954-11-11;
  $user-3 isa user,
    has id "u0003",
    has name "Keyla Pineda";
  $order-1 isa order,
    has id "o0001",
    has timestamp 2022-08-03T19:51:24.324,
    has status "canceled";
  $order-2 isa order,
    has id "o0002",
    has timestamp 2021-04-27T05:02:39.672,
    has status "dispatched";
  $order-6 isa order,
    has id "o0006",
    has timestamp 2020-08-19T20:21:54.194,
    has status "paid";
   $p1 isa purchase, links (order: $order-1, buyer: $user-1);
   $p2 isa purchase, links (order: $order-2, buyer: $user-1);
   $p3 isa purchase, links (order: $order-6, buyer: $user-2);
"#;

    let match_str = r#"
    match $x isa purchase, links ($y);
    "#;

    run_adhoc(storage.clone(), Some(define_str), Some(insert_str), None);

    let delete_str = r#"
        match
$order-6 isa order,
  has id "o0006",
  has status $old-status;
  $order-2 isa order, has id "o0002";
delete
  $order-2;
  $old-status of $order-6;
insert
  $order-6 has status "dispatched";"#;

    run_adhoc(storage.clone(), None, Some(delete_str), Some(match_str));
}
/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

mod common;

use std::sync::Arc;

use compiler::match_::inference::annotated_functions::IndexedAnnotatedFunctions;
use concept::thing::statistics::Statistics;
use encoding::{
    graph::definition::definition_key_generator::DefinitionKeyGenerator,
    value::{label::Label, value::Value},
};
use executor::{
    batch::ImmutableRow,
    pipeline::{PipelineContext, PipelineError, PipelineStageAPI},
};
use function::function_manager::FunctionManager;
use lending_iterator::LendingIterator;
use query::query_manager::QueryManager;
use storage::snapshot::CommittableSnapshot;

use crate::common::{load_managers, setup_storage};

const PERSON_LABEL: Label = Label::new_static("person");
const AGE_LABEL: Label = Label::new_static("age");
const NAME_LABEL: Label = Label::new_static("name");
const MEMBERSHIP_LABEL: Label = Label::new_static("membership");
const MEMBERSHIP_MEMBER_LABEL: Label = Label::new_static_scoped("member", "membership", "membership:member");

#[test]
fn test_match_as_pipeline() {
    let (_tmp_dir, storage) = setup_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let qm = QueryManager::new();
    let schema = r#"
    define
        attribute age value long;
        attribute name value string;
        entity person;
        relation membership, relates member;
    "#;
    storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();

    qm.execute_schema(&mut snapshot, &type_manager, define).unwrap();
    todo!()
    //
    // let age = [
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(10)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(11)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(12)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(13)).unwrap(),
    //     thing_manager.create_attribute(&mut snapshot, age_type.clone(), Value::Long(14)).unwrap(),
    // ];
    //
    // let name = [
    //     thing_manager.create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("John"))).unwrap(),
    //     thing_manager
    //         .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Alice")))
    //         .unwrap(),
    //     thing_manager
    //         .create_attribute(&mut snapshot, name_type.clone(), Value::String(Cow::Borrowed("Leila")))
    //         .unwrap(),
    // ];
    //
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, age[1].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, age[2].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, name[0].clone()).unwrap();
    // person[0].set_has_unordered(&mut snapshot, &thing_manager, name[1].clone()).unwrap();
    //
    // person[1].set_has_unordered(&mut snapshot, &thing_manager, age[4].clone()).unwrap();
    // person[1].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    // person[1].set_has_unordered(&mut snapshot, &thing_manager, age[0].clone()).unwrap();
    //
    // person[2].set_has_unordered(&mut snapshot, &thing_manager, age[3].clone()).unwrap();
    // person[2].set_has_unordered(&mut snapshot, &thing_manager, name[2].clone()).unwrap();
    //
    // let finalise_result = thing_manager.finalise(&mut snapshot);
    // assert!(finalise_result.is_ok());
    // snapshot.commit().unwrap();
    //
    // let mut statistics = Statistics::new(SequenceNumber::new(0));
    // statistics.may_synchronise(&storage).unwrap();
    //
    // let query = "match $person isa person, has name $name, has age $age;";
    // let match_ = typeql::parse_query(query).unwrap().into_pipeline().stages.remove(0).into_match();
    //
    // // IR
    // let empty_function_index = HashMapFunctionSignatureIndex::empty();
    // let mut translation_context = TranslationContext::new();
    // let builder = translate_match(&mut translation_context, &empty_function_index, &match_).unwrap();
    // // builder.add_limit(3);
    // // builder.add_filter(vec!["person", "age"]).unwrap();
    // let block = builder.finish();
    //
    // // Executor
    // let snapshot = storage.clone().open_snapshot_read();
    // let (type_manager, thing_manager) = load_managers(storage.clone());
    // let (entry_annotations, annotated_functions) = infer_types(
    //     &block,
    //     vec![],
    //     &snapshot,
    //     &type_manager,
    //     &IndexedAnnotatedFunctions::empty(),
    //     &translation_context.variable_registry,
    // )
    //     .unwrap();
    // let pattern_plan = PatternPlan::from_block(
    //     &block,
    //     &entry_annotations,
    //     &translation_context.variable_registry,
    //     &HashMap::new(),
    //     &statistics,
    // );
    // let program_plan = ProgramPlan::new(pattern_plan, HashMap::new(), HashMap::new());
    // let executor = ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap();
    // let iterator = executor.into_iterator(Arc::new(snapshot), Arc::new(thing_manager));
    //
    // let rows = iterator
    //     .map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone()))
    //     .into_iter()
    //     .try_collect::<_, Vec<_>, _>()
    //     .unwrap();
    //
    // assert_eq!(rows.len(), 7);
    //
    // for row in rows {
    //     for value in row {
    //         print!("{}, ", value);
    //     }
    //     println!()
    // }
}

#[test]
fn test_insert() {
    let (_tmp_dir, storage) = setup_storage();
    let mut snapshot = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let qm = QueryManager::new();
    let schema = r#"
    define
        attribute age value long;
        attribute name value string;
        entity person owns age, owns name, plays membership:member;
        relation membership relates member;
    "#;
    storage.clone().open_snapshot_schema();
    let define = typeql::parse_query(schema).unwrap().into_schema();
    qm.execute_schema(&mut snapshot, &type_manager, define).unwrap();
    let seq = snapshot.commit().unwrap();
    let mut statistics = Statistics::new(seq.unwrap());
    statistics.may_synchronise(&storage).unwrap();

    let mut snapshot = storage.clone().open_snapshot_write();
    let query_str = "insert $p isa person, has age 10;";
    let query = typeql::parse_query(query_str).unwrap().into_pipeline();
    let function_manager = FunctionManager::new(Arc::new(DefinitionKeyGenerator::new()), None);
    let mut pipeline = qm
        .prepare_writable_pipeline(
            snapshot,
            thing_manager,
            &type_manager,
            &function_manager,
            &statistics,
            &IndexedAnnotatedFunctions::empty(),
            &query,
        )
        .unwrap();

    assert!(pipeline.next().is_some());
    assert!(pipeline.next().is_none());
    let PipelineContext::Owned(mut snapshot, mut thing_manager) = pipeline.finalise() else { unreachable!() };
    snapshot.commit().unwrap();
    {
        let snapshot = storage.clone().open_snapshot_read();
        let age_type = type_manager.get_attribute_type(&snapshot, &AGE_LABEL).unwrap().unwrap();
        let attr_age_10 =
            thing_manager.get_attribute_with_value(&snapshot, age_type, Value::Long(10)).unwrap().unwrap();
        assert_eq!(1, attr_age_10.get_owners(&snapshot, &thing_manager).count());
        snapshot.close_resources()
    }
}

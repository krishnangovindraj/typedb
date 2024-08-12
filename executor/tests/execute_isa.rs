/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{collections::HashMap, sync::Arc};
use compiler::{
    inference::{annotated_functions::IndexedAnnotatedFunctions, type_inference::infer_types},
    instruction::constraint::instructions::{ConstraintInstruction, Inputs},
    planner::{
        pattern_plan::{IntersectionStep, PatternPlan, Step},
        program_plan::ProgramPlan,
    },
};
use concept::error::ConceptReadError;
use encoding::value::label::Label;
use executor::{batch::ImmutableRow, program_executor::ProgramExecutor};
use ir::{
    pattern::constraint::IsaKind,
    program::{block::FunctionalBlock},
};
use ir::program::block::MultiBlockContext;
use lending_iterator::LendingIterator;
use storage::{
    durability_client::WALClient,
    snapshot::{CommittableSnapshot, ReadSnapshot, WriteSnapshot},
    MVCCStorage,
};

use crate::common::{load_managers, setup_storage};

mod common;

const ANIMAL_LABEL: Label = Label::new_static("animal");
const CAT_LABEL: Label = Label::new_static("cat");
const DOG_LABEL: Label = Label::new_static("dog");

fn setup_database(storage: Arc<MVCCStorage<WALClient>>) {
    let mut snapshot: WriteSnapshot<WALClient> = storage.clone().open_snapshot_write();
    let (type_manager, thing_manager) = load_managers(storage.clone());

    let animal_type = type_manager.create_entity_type(&mut snapshot, &ANIMAL_LABEL).unwrap();
    let dog_type = type_manager.create_entity_type(&mut snapshot, &DOG_LABEL).unwrap();
    let cat_type = type_manager.create_entity_type(&mut snapshot, &CAT_LABEL).unwrap();
    dog_type.set_supertype(&mut snapshot, &type_manager, animal_type.clone()).unwrap();
    cat_type.set_supertype(&mut snapshot, &type_manager, animal_type.clone()).unwrap();

    let _animal_1 = thing_manager.create_entity(&mut snapshot, animal_type.clone()).unwrap();
    let _animal_2 = thing_manager.create_entity(&mut snapshot, animal_type.clone()).unwrap();

    let _dog_1 = thing_manager.create_entity(&mut snapshot, dog_type.clone()).unwrap();
    let _dog_2 = thing_manager.create_entity(&mut snapshot, dog_type.clone()).unwrap();
    let _dog_3 = thing_manager.create_entity(&mut snapshot, dog_type.clone()).unwrap();

    let _cat_1 = thing_manager.create_entity(&mut snapshot, cat_type.clone()).unwrap();

    let finalise_result = thing_manager.finalise(&mut snapshot);
    assert!(finalise_result.is_ok());
    snapshot.commit().unwrap();
}

#[test]
fn traverse_isa_unbounded_sorted_thing() {
    let (_tmp_dir, storage) = setup_storage();
    setup_database(storage.clone());

    // query:
    //   match $x isa $t; $t label dog;

    // IR
    let mut context = MultiBlockContext::new();
    let mut builder = FunctionalBlock::builder(&mut context);
    let mut conjunction = builder.conjunction_mut();
    let var_dog_type = conjunction.get_or_declare_variable("dog_type").unwrap();
    let var_dog = conjunction.get_or_declare_variable("dog").unwrap();

    // add all constraints to make type inference return correct types, though we only plan Has's
    let isa = conjunction.constraints_mut().add_isa(IsaKind::Subtype, var_dog, var_dog_type).unwrap().clone();
    conjunction.constraints_mut().add_label(var_dog_type, DOG_LABEL.scoped_name().as_str()).unwrap();

    let block = builder.finish();
    let (entry_annotations, annotated_functions) = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (type_manager, _) = load_managers(storage.clone());
        infer_types(&block, &context, vec![], &snapshot, &type_manager, &IndexedAnnotatedFunctions::empty()).unwrap()
    };

    // Plan
    let steps = vec![Step::Intersection(IntersectionStep::new(
        var_dog,
        vec![ConstraintInstruction::IsaReverse(isa.clone(), Inputs::None([]))],
        &[var_dog, var_dog_type],
    ))];

    let pattern_plan = PatternPlan::new(steps);
    let program_plan =
        ProgramPlan::new(context, pattern_plan, entry_annotations.clone(), HashMap::new(), HashMap::new());

    // Executor
    let executor = {
        let snapshot: ReadSnapshot<WALClient> = storage.clone().open_snapshot_read();
        let (_, thing_manager) = load_managers(storage.clone());
        ProgramExecutor::new(&program_plan, &snapshot, &thing_manager).unwrap()
    };

    {
        let snapshot: Arc<ReadSnapshot<WALClient>> = Arc::new(storage.clone().open_snapshot_read());
        let (_, thing_manager) = load_managers(storage.clone());
        let thing_manager = Arc::new(thing_manager);

        let iterator = executor.into_iterator(snapshot, thing_manager);

        let rows: Vec<Result<ImmutableRow<'static>, ConceptReadError>> =
            iterator.map_static(|row| row.map(|row| row.into_owned()).map_err(|err| err.clone())).collect();
        assert_eq!(rows.len(), 3);

        for row in rows {
            let row = row.unwrap();
            assert_eq!(row.get_multiplicity(), 1);
            print!("{}", row);
        }
    }
}

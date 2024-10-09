/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::Arc;
use compiler::executable::match_::planner::match_executable::{ExecutionStep, MatchExecutable};
use concept::error::ConceptReadError;
use concept::thing::thing_manager::ThingManager;
use storage::snapshot::ReadableSnapshot;
use crate::read::step_executors::StepExecutor;

pub(super) mod step_executors;
pub mod expression_executor;

pub(crate) struct EntryStep {} // TODO: This is a subpattern step.
pub(crate) enum Step {
    Fundamental(StepExecutor),

    // Internal
    Entry(EntryStep),
    PopFailure,
    Yield,
}

pub(crate) fn prepare_executors(
    match_executable: &MatchExecutable, snapshot: &Arc<impl ReadableSnapshot + 'static>, thing_manager: &Arc<ThingManager>
) -> Result<Vec<Step>, ConceptReadError> {
    let mut steps = Vec::with_capacity(2 + match_executable.steps().len());
    for step in match_executable.steps() {
        let step = match step {
            ExecutionStep::Intersection(_) => Step::Fundamental(StepExecutor::new(step, snapshot, thing_manager)?),
            ExecutionStep::UnsortedJoin(_) => Step::Fundamental(StepExecutor::new(step, snapshot, thing_manager)?),
            ExecutionStep::Assignment(_) => Step::Fundamental(StepExecutor::new(step, snapshot, thing_manager)?),
            ExecutionStep::Check(_) => Step::Fundamental(StepExecutor::new(step, snapshot, thing_manager)?),

            // SubPatterns
            ExecutionStep::Negation(_) => Step::Fundamental(StepExecutor::new(step, snapshot, thing_manager)?),
            ExecutionStep::Optional(_) => todo!(),
            ExecutionStep::Disjunction(_) => todo!(),
        };
        steps.push(step)
    }
    Ok(steps)
}

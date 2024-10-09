/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{
    batch::FixedBatch,
    read::recursive_executor::{InstructionPointerUpdate, StackInstruction},
};

type TakeButPutBack<T> = Option<T>;

pub(super) enum SubPatternStep {
    Entry(EntryStep),
}

pub(super) trait SubPatternTrait {
    fn take_pattern(&mut self) -> Option<Vec<StackInstruction>>;

    fn record_pattern_result(
        &mut self,
        inner: Vec<StackInstruction>,
        batch: FixedBatch,
    ) -> (InstructionPointerUpdate, Option<FixedBatch>);

    fn record_pattern_exhausted(
        &mut self,
        inner: Vec<StackInstruction>,
    ) -> (InstructionPointerUpdate, Option<FixedBatch>);
}

impl SubPatternTrait for SubPatternStep {
    fn take_pattern(&mut self) -> Option<Vec<StackInstruction>> {
        match self {
            SubPatternStep::Entry(step) => step.take_pattern(),
        }
    }

    fn record_pattern_result(
        &mut self,
        inner: Vec<StackInstruction>,
        batch: FixedBatch,
    ) -> (InstructionPointerUpdate, Option<FixedBatch>) {
        match self {
            SubPatternStep::Entry(step) => step.record_pattern_result(inner, batch),
        }
    }

    fn record_pattern_exhausted(
        &mut self,
        inner: Vec<StackInstruction>,
    ) -> (InstructionPointerUpdate, Option<FixedBatch>) {
        match self {
            SubPatternStep::Entry(step) => step.record_pattern_exhausted(inner),
        }
    }
}

// Steps
pub(super) struct EntryStep {
    entry: TakeButPutBack<Vec<StackInstruction>>,
}

impl EntryStep {
    pub(crate) fn new(entry_pattern: Vec<StackInstruction>) -> EntryStep {
        Self { entry: Some(entry_pattern) }
    }
}

impl SubPatternTrait for EntryStep {
    fn take_pattern(&mut self) -> Option<Vec<StackInstruction>> {
        debug_assert!(self.entry.is_some());
        self.entry.take()
    }

    fn record_pattern_result(
        &mut self,
        inner: Vec<StackInstruction>,
        batch: FixedBatch,
    ) -> (InstructionPointerUpdate, Option<FixedBatch>) {
        debug_assert!(self.entry.is_none());
        self.entry = Some(inner);
        (InstructionPointerUpdate::Remain, Some(batch))
    }

    fn record_pattern_exhausted(
        &mut self,
        _inner: Vec<StackInstruction>,
    ) -> (InstructionPointerUpdate, Option<FixedBatch>) {
        debug_assert!(self.entry.is_none());
        // Don't put it back
        (InstructionPointerUpdate::Remain, None)
    }
}

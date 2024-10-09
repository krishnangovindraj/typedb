/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use crate::{batch::FixedBatch, read::recursive_executor::StackInstruction};

pub(super) enum SubPatternExecutor {}

trait SubPatternStep {
    fn process_subpattern_result(&mut self, current_index: usize) -> (Option<FixedBatch>, usize); // next stack index

    fn take_pattern(&mut self) -> Vec<StackInstruction>;

    fn restore_pattern(inner: Vec<StackInstruction>);
}

pub(super) struct EntryStep {
    entry: Vec<StackInstruction>,
}

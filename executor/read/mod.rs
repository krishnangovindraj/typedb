/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use storage::snapshot::ReadableSnapshot;

mod expression_executor;
pub(crate) mod pattern_executor;
mod pattern_instructions;
mod immediate_executors;
mod subpattern_executor;

/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use concept::thing::thing_manager::ThingManager;
use storage::snapshot::ReadableSnapshot;

pub struct PipelineContext<Snapshot: ReadableSnapshot> {
    snapshot: Snapshot,
    thing_manager: ThingManager,
}

pub trait PipelineStage<Snapshot: ReadableSnapshot> {

    fn finalise(self) -> PipelineContext<Snapshot>;
}
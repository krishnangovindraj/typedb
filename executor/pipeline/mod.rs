/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{marker::PhantomData, sync::Arc};

use concept::{error::ConceptReadError, thing::thing_manager::ThingManager};
use lending_iterator::LendingIterator;
use storage::snapshot::ReadableSnapshot;

use crate::{batch::ImmutableRow, pattern_executor::MatchStage};

pub enum PipelineContext<Snapshot: ReadableSnapshot> {
    Arced(Arc<Snapshot>, Arc<ThingManager>),
    Owned(Snapshot, ThingManager),
}

impl<Snapshot: ReadableSnapshot> PipelineContext<Snapshot> {
    pub(crate) fn borrow_parts(&self) -> (&Snapshot, &ThingManager) {
        match self {
            PipelineContext::Arced(snapshot, thing_manager) => (&snapshot, &thing_manager),
            PipelineContext::Owned(snapshot, thing_manager) => (&snapshot, &thing_manager),
        }
    }
}

#[derive(Clone)]
pub enum PipelineError {
    ConceptRead(ConceptReadError),
}

pub trait PipelineStageAPI<Snapshot: ReadableSnapshot>:
    for<'a> LendingIterator<Item<'a> = Result<ImmutableRow<'a>, PipelineError>>
{
    fn finalise(self) -> PipelineContext<Snapshot>;

    // fn next(&mut self) -> ImmutableRow<'static>; // TODO: See if we can LendingIterator instead
}

type InsertStage<Snapshot> = PhantomData<Snapshot>;
pub enum PipelineStage<Snapshot: ReadableSnapshot + 'static> {
    Match(MatchStage<Snapshot>),
    // Insert(InsertStage<Snapshot>),
}

impl<Snapshot: ReadableSnapshot + 'static> PipelineStage<Snapshot> {
    pub(crate) fn next(&mut self) -> Option<Result<ImmutableRow<'_>, PipelineError>> {
        match self {
            PipelineStage::Match(match_) => match_.next(),
            // PipelineStage::Insert(insert.next()) => insert.next(),
        }
    }

    pub(crate) fn finalise(self) -> PipelineContext<Snapshot> {
        match self {
            PipelineStage::Match(match_) => match_.finalise(),
        }
    }
}

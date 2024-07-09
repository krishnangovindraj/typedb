/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{error::Error, fmt, sync::Arc};

use encoding::error::EncodingError;
use ir::PatternDefinitionError;
use storage::snapshot::{iterator::SnapshotIteratorError, SnapshotGetError};

pub mod function;
mod function_cache;
pub mod function_manager;
pub mod ir_builder;

#[derive(Debug)]
pub enum FunctionDefinitionError {
    PatternDefinition { source: PatternDefinitionError },
    FunctionAlreadyExists { name: String },
    SnapshotGet { source: SnapshotGetError },
    Encoding { source: EncodingError },
    FunctionRead { source: FunctionReadError },
}

impl fmt::Display for FunctionDefinitionError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionDefinitionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::PatternDefinition { source } => Some(source),
            Self::Encoding { source } => Some(source),
            Self::SnapshotGet { source } => Some(source),
            Self::FunctionRead { source } => Some(source),
            Self::FunctionAlreadyExists { .. } => None,
        }
    }
}

#[derive(Debug)]
pub enum FunctionReadError {
    FunctionNotFound { name: String },
    SnapshotGet { source: SnapshotGetError },
    SnapshotIterate { source: Arc<SnapshotIteratorError> },
    PatternDefinition { source: PatternDefinitionError },
}

impl fmt::Display for FunctionReadError {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Error for FunctionReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotGet { source } => Some(source),
            Self::SnapshotIterate { source } => Some(source),
            Self::PatternDefinition { source } => Some(source),
            Self::FunctionNotFound { .. } => None,
        }
    }
}

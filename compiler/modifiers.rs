/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
};

use answer::{variable::Variable, Type};
use encoding::value::value_type::ValueTypeCategory;
use ir::{
    pattern::IrID,
    program::{function::Reducer, modifier::SortVariable, reduce::Reduce},
};

use crate::VariablePosition;

pub struct SelectProgram {
    pub retained_positions: HashSet<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

pub struct SortProgram {
    pub sort_on: Vec<SortVariable>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

pub struct OffsetProgram {
    pub offset: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

pub struct LimitProgram {
    pub limit: u64,
    pub output_row_mapping: HashMap<Variable, VariablePosition>,
}

// TODO: Maybe move to its own file
pub struct ReduceProgram {
    pub reduction_inputs: Vec<ReduceOperation<VariablePosition>>,
    pub input_group_positions: Vec<VariablePosition>,
    pub output_row_mapping: HashMap<Variable, VariablePosition>, // output_row = (group_vars, reduce_outputs)
}

pub enum ReduceOperation<ID: IrID> {
    SumLong(ID),
    Count(ID),
}

impl<ID: IrID> ReduceOperation<ID> {
    pub fn output_type(&self) -> ValueTypeCategory {
        match self {
            Self::Count(_) => ValueTypeCategory::Long,
            Self::SumLong(_) => ValueTypeCategory::Long,
        }
    }
}

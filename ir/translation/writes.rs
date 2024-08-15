/*
* This Source Code Form is subject to the terms of the Mozilla Public
* License, v. 2.0. If a copy of the MPL was not distributed with this
* file, You can obtain one at https://mozilla.org/MPL/2.0/.
*/

use answer::variable::Variable;
use typeql::query::stage::delete::DeletableKind;

use crate::{
    program::{
        block::{BlockContext, FunctionalBlock, FunctionalBlockBuilder},
        function_signature::HashMapFunctionSignatureIndex,
    },
    translation::{
        constraints::{add_statement, add_typeql_relation, register_typeql_var},
        TranslationContext,
    },
    PatternDefinitionError,
};
use crate::pattern::constraint::{Constraints, ConstraintsBuilder};
use crate::pattern::ScopeId;

pub fn translate_insert<'a>(
    context: &'a mut TranslationContext,
    insert: &typeql::query::stage::Insert,
) -> Result<Constraints, PatternDefinitionError> {
    let block_context = &mut context.next_block_context();
    let mut constraints = Constraints::new(ScopeId::ROOT);
    let mut builder = ConstraintsBuilder::new(block_context, &mut constraints);
    let function_index = HashMapFunctionSignatureIndex::empty();
    for statement in &insert.statements {
        add_statement(&function_index, &mut builder, statement)?;
    }
    Ok(constraints)
}

pub fn translate_delete<'a>(
    context: &'a mut TranslationContext,
    delete: &typeql::query::stage::Delete,
) -> Result<(Constraints, Vec<Variable>), PatternDefinitionError> {
    let block_context = &mut context.next_block_context();
    let mut constraints = Constraints::new(ScopeId::ROOT);
    let mut builder = ConstraintsBuilder::new(block_context, &mut constraints);
    let mut deleted_concepts = Vec::new();
    for deletable in &delete.deletables {
        match &deletable.kind {
            DeletableKind::Has { attribute, owner } => {
                let translated_owner = register_typeql_var(&mut builder, owner)?;
                let translated_attribute = register_typeql_var(&mut builder, attribute)?;
                builder.add_has(translated_owner, translated_attribute)?;
            }
            DeletableKind::Links { players, relation } => {
                let translated_relation = register_typeql_var(&mut builder, relation)?;
                add_typeql_relation(&mut builder, translated_relation, players)?;
            }
            DeletableKind::Concept { variable } => {
                let translated_variable = builder.get_or_declare_variable(variable.name().unwrap())?;
                deleted_concepts.push(translated_variable);
            }
        }
    }

    Ok((constraints, deleted_concepts))
}

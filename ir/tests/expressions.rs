/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::HashMap;
use itertools::Itertools;
use typeql::query::stage::Stage;
use answer::variable::Variable;
use encoding::value::value::Value;
use encoding::value::value_type::ValueTypeCategory;
use ir::expressions::evaluator::ExpressionEvaluator;
use ir::program::function_signature::HashMapFunctionIndex;
use ir::translator::match_::translate_match;
use ir::expressions::expression_compiler::{CompiledExpressionTree, ExpressionTreeCompiler};
use ir::expressions::ExpressionCompilationError;
use ir::pattern::constraint::Constraint;

fn compile_expression_via_match(s: &str, variable_types: HashMap<&str, ValueTypeCategory>) ->  Result<(HashMap<String, Variable>, CompiledExpressionTree), ExpressionCompilationError> {
    let query = format!("match $x = {}; filter $x;", s);
    if let Stage::Match(match_) = typeql::parse_query(query.as_str()).unwrap().into_pipeline().stages.get(0).unwrap() {
        let block = translate_match(&HashMapFunctionIndex::empty(), &match_).unwrap().finish();
        let variable_mapping = variable_types.keys()
            .map(|name| ((*name).to_owned(), block.context().get_variable_named(name, block.scope_id()).unwrap().clone()))
            .collect::<HashMap<_,_>>();
        let variable_types_mapped = variable_types.into_iter()
                .map(|(name, type_)| (block.context().get_variable_named(name, block.scope_id()).unwrap().clone(), type_))
                .collect::<HashMap<_,_>>();


        let expression_binding = match &block.conjunction().constraints()[0] {
            Constraint::ExpressionBinding(binding) => binding,
            _ => unreachable!()
        };
        Ok((variable_mapping, ExpressionTreeCompiler::compile(expression_binding.expression(), variable_types_mapped)?))
    } else {
        unreachable!();
    }
}


#[test]
fn test_basic() {
    {
        let (_, expr) = compile_expression_via_match("3 + 5", HashMap::new()).unwrap();
        let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
        assert_eq!(result, Value::Long(8));
    }


    {
        let (_, expr) = compile_expression_via_match("7.0e0 + 9.0e0", HashMap::new()).unwrap();
        let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
        assert_eq!(result, Value::Double(16.0));
    }

    {
        let (vars, expr) = compile_expression_via_match("$a + $b", HashMap::from([("a", ValueTypeCategory::Long), ("b", ValueTypeCategory::Long)])).unwrap();
        let (a, b) = ["a", "b"].into_iter().map(|name| vars.get(name).unwrap().clone()).collect_tuple().unwrap();

        let result = ExpressionEvaluator::evaluate(expr, HashMap::from([(a, Value::Long(2)), (b, Value::Long(5))])).unwrap();
        assert_eq!(result, Value::Long(7));
    }
}

#[test]
fn test_ops_cast_functions(){
    // Long ops
    {
        {
            let (_, expr) = compile_expression_via_match("12 + 4", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Long(16));
        }
        {
            let (_, expr) = compile_expression_via_match("12 - 4", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Long(8));
        }

        {
            let (_, expr) = compile_expression_via_match("12 * 4", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Long(48));
        }

        {
            let (_, expr) = compile_expression_via_match("12 / 4", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(3.0));
        }

        {
            let (_, expr) = compile_expression_via_match("12 % 5", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Long(2));
        }


        {
            let (_, expr) = compile_expression_via_match("12 ^ 4", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(f64::powf(12.0, 4.0)));
        }
    }

    // Double ops
    {
        {
            let (_, expr) = compile_expression_via_match("12.0e0 + 4.0e0", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(16.0));
        }
        {
            let (_, expr) = compile_expression_via_match("12.0e0 - 4.0e0", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(8.0));
        }

        {
            let (_, expr) = compile_expression_via_match("12.0e0 * 4.0e0", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(48.0));
        }

        {
            let (_, expr) = compile_expression_via_match("12.0e0 / 4.0e0", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(3.0));
        }

        {
            let (_, expr) = compile_expression_via_match("12.0e0 % 5.0e0", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double(2.0));
        }


        {
            let (_, expr) = compile_expression_via_match("12.0e0 ^ 4.0e0", HashMap::new()).unwrap();
            let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
            assert_eq!(result, Value::Double( f64::powf(12.0, 4.0)));
        }
    }

    // Long-double cast ops
    {
        let (_, expr) = compile_expression_via_match("12.0e0 + 4", HashMap::new()).unwrap();
        let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
        assert_eq!(result, Value::Double(16.0));
    }

    {
        let (_, expr) = compile_expression_via_match("12 + 4.0e0", HashMap::new()).unwrap();
        let result = ExpressionEvaluator::evaluate(expr, HashMap::new()).unwrap();
        assert_eq!(result, Value::Double(16.0));
    }
}
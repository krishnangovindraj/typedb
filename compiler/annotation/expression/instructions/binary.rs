/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */
use std::{borrow::Cow, cmp, marker::PhantomData, ops::Rem};

use base64::{engine::general_purpose::STANDARD, Engine};

use encoding::value::{decimal_value::Decimal, value::NativeValueConvertible, value_type::ValueTypeCategory};

use crate::annotation::expression::{
    expression_compiler::ExpressionCompilationContext,
    instructions::{
        op_codes::ExpressionOpCode, CompilableExpression, ExpressionEvaluationError, ExpressionInstruction,
    },
    ExpressionCompileError,
};

pub trait BinaryExpression<
    'a,
    T1: NativeValueConvertible<'a>,
    T2: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
>
{
    const OP_CODE: ExpressionOpCode;
    fn evaluate(a1: T1, a2: T2) -> Result<R, ExpressionEvaluationError>;
}

pub struct Binary<'a, T1, T2, R, F>
where
    T1: NativeValueConvertible<'a>,
    T2: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
    F: BinaryExpression<'a, T1, T2, R>,
{
    pub phantom: PhantomData<&'a (T1, T2, R, F)>,
}

impl<'a, T1, T2, R, F> ExpressionInstruction for Binary<'a, T1, T2, R, F>
where
    T1: NativeValueConvertible<'a>,
    T2: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
    F: BinaryExpression<'a, T1, T2, R>,
{
    const OP_CODE: ExpressionOpCode = F::OP_CODE;
}

impl<'a, T1, T2, R, F> CompilableExpression for Binary<'a, T1, T2, R, F>
where
    T1: NativeValueConvertible<'a>,
    T2: NativeValueConvertible<'a>,
    R: NativeValueConvertible<'a>,
    F: BinaryExpression<'a, T1, T2, R>,
{
    fn return_value_category(&self) -> Option<ValueTypeCategory> {
        Some(R::VALUE_TYPE_CATEGORY)
    }

    fn validate_and_append(builder: &mut ExpressionCompilationContext<'_>) -> Result<(), Box<ExpressionCompileError>> {
        let a2 = builder.pop_type_single()?.category();
        let a1 = builder.pop_type_single()?.category();
        if a1 != T1::VALUE_TYPE_CATEGORY {
            return Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: F::OP_CODE,
                expected: T1::VALUE_TYPE_CATEGORY,
                actual: a1,
            }));
        }
        if a2 != T2::VALUE_TYPE_CATEGORY {
            return Err(Box::new(ExpressionCompileError::ExpressionMismatchedValueType {
                op_code: F::OP_CODE,
                expected: T2::VALUE_TYPE_CATEGORY,
                actual: a2,
            }));
        }
        builder.push_type_single(R::VALUE_TYPE_CATEGORY.try_into_value_type().unwrap());
        builder.append_instruction(Self::OP_CODE);
        Ok(())
    }
}

macro_rules! binary_instruction {
    ( $lt:lifetime $( $name:ident = $impl_name:ident($a1:ident: $t1:ty, $a2:ident: $t2:ty) -> $r:ty $impl_code:block )* ) => { $(
        pub type $name<$lt> = Binary<$lt, $t1, $t2, $r, $impl_name>;
        pub struct $impl_name {}
        impl<$lt> BinaryExpression<$lt, $t1, $t2, $r> for $impl_name {
            const OP_CODE: ExpressionOpCode = ExpressionOpCode::$name;
            fn evaluate($a1: $t1, $a2: $t2) -> Result<$r, ExpressionEvaluationError> {
                $impl_code
            }
        })*
    };
}

pub(crate) use binary_instruction;

binary_instruction! { 'a
    MathRemainderInteger = MathRemainderIntegerImpl(a1: i64, a2: i64) -> i64 { Ok(i64::rem(a1, a2)) }

    MathMinIntegerInteger = MathMinIntegerIntegerImpl(a1: i64, a2: i64) -> i64 { Ok(cmp::min(a1, a2)) }
    MathMinDoubleDouble = MathMinDoubleDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(f64::min(a1, a2)) }
    MathMinDecimalDecimal = MathMinDecimalDecimalImpl(a1: Decimal, a2: Decimal) -> Decimal { Ok(cmp::min(a1, a2)) }

    MathMaxIntegerInteger = MathMaxIntegerIntegerImpl(a1: i64, a2: i64) -> i64 { Ok(cmp::max(a1, a2)) }
    MathMaxDoubleDouble = MathMaxDoubleDoubleImpl(a1: f64, a2: f64) -> f64 { Ok(f64::max(a1, a2)) }
    MathMaxDecimalDecimal = MathMaxDecimalDecimalImpl(a1: Decimal, a2: Decimal) -> Decimal { Ok(cmp::max(a1, a2)) }

    FuzzyMatchStringString = FuzzyMatchStringStringImpl(a1: Cow<'a, str>, a2: Cow<'a, str>) -> f64 {
        Ok(jaro_similarity(&a1, &a2))
    }
    SimilarityStringString = SimilarityStringStringImpl(a1: Cow<'a, str>, a2: Cow<'a, str>) -> f64 {
        base64_vector_similarity(&a1, &a2)
    }
}

fn jaro_similarity(s1: &str, s2: &str) -> f64 {
    if s1.is_empty() && s2.is_empty() {
        return 1.0;
    } else if s1.is_empty() || s2.is_empty() {
        return 0.0;
    }

    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let s1_len = s1_chars.len();
    let s2_len = s2_chars.len();

    let match_distance = (cmp::max(s1_len, s2_len) / 2).saturating_sub(1);

    let mut s1_matches = vec![false; s1_len];
    let mut s2_matches = vec![false; s2_len];

    let mut matches = 0usize;
    let mut transpositions = 0usize;

    for i in 0..s1_len {
        let start = i.saturating_sub(match_distance);
        let end = cmp::min(i + match_distance + 1, s2_len);
        for j in start..end {
            if s2_matches[j] || s1_chars[i] != s2_chars[j] {
                continue;
            }
            s1_matches[i] = true;
            s2_matches[j] = true;
            matches += 1;
            break;
        }
    }

    if matches == 0 {
        return 0.0;
    }

    let mut k = 0;
    for i in 0..s1_len {
        if !s1_matches[i] {
            continue;
        }
        while !s2_matches[k] {
            k += 1;
        }
        if s1_chars[i] != s2_chars[k] {
            transpositions += 1;
        }
        k += 1;
    }

    let m = matches as f64;
    (m / s1_len as f64 + m / s2_len as f64 + (m - transpositions as f64 / 2.0) / m) / 3.0
}

fn decode_base64_to_f32_vec(s: &str) -> Result<Vec<f32>, ExpressionEvaluationError> {
    let bytes = STANDARD
        .decode(s)
        .map_err(|e| ExpressionEvaluationError::Base64DecodeFailed { description: e.to_string() })?;
    if bytes.len() % 4 != 0 {
        return Err(ExpressionEvaluationError::InvalidVectorEncoding { len: bytes.len() });
    }
    Ok(bytes.chunks_exact(4).map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]])).collect())
}

fn base64_vector_similarity(s1: &str, s2: &str) -> Result<f64, ExpressionEvaluationError> {
    let v1 = decode_base64_to_f32_vec(s1)?;
    let v2 = decode_base64_to_f32_vec(s2)?;
    if v1.len() != v2.len() {
        return Err(ExpressionEvaluationError::VectorLengthMismatch { len1: v1.len(), len2: v2.len() });
    }
    let mut dot = 0.0f64;
    let mut norm1 = 0.0f64;
    let mut norm2 = 0.0f64;
    for (a, b) in v1.iter().zip(v2.iter()) {
        let a = *a as f64;
        let b = *b as f64;
        dot += a * b;
        norm1 += a * a;
        norm2 += b * b;
    }
    let denom = norm1.sqrt() * norm2.sqrt();
    if denom == 0.0 {
        Ok(0.0)
    } else {
        Ok(dot / denom)
    }
}

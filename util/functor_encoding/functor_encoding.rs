/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use itertools::Itertools;

pub trait FunctorEncoded<FunctorContext> {
    fn encode_as_functor(&self, context: &FunctorContext) -> String;
}


#[macro_export]
macro_rules! encode_args {
    ($context:ident, { $( $arg:ident, )* } )   => {
        {
            let arr: Vec<&dyn functor_encoding::FunctorEncoded<_>> = vec![ $($arg,)* ];
            arr.into_iter().map(|s| s.encode_as_functor($context)).join(", ")
        }
    }
}

#[macro_export]
macro_rules! encode_functor_impl {
    ($context:ident, $func:ident $args:tt) => {
        std::format!("{}({})", std::stringify!($func), functor_encoding::encode_args!($context, $args))
    }
}

#[macro_export]
macro_rules! add_ignored_fields {
    ($qualified:path { $( $arg:ident, )* }) => {
        $qualified { $( $arg, )* .. }
    };
}

#[macro_export]
macro_rules! encode_functor {
    ($context:ident, $what:ident as struct $struct_name:ident  $fields:tt) => {
        functor_encoding::encode_functor!($context, $what => [ $struct_name => $struct_name $fields, ])
    };
    ($context:ident, $what:ident as struct $struct_name:ident $fields:tt named $renamed:ident ) => {
        functor_encoding::encode_functor!($context, $what => [ $struct_name => $renamed $fields, ])
    };
    ($context:ident, $what:ident as enum $enum_name:ident [ $($variant:ident $fields:tt |)* ]) => {
        functor_encoding::encode_functor!($context, $what => [ $( $enum_name::$variant => $variant $fields ,)* ])
    };
    ($context:ident, $what:ident => [ $($qualified:path => $func:ident $fields:tt, )* ]) => {
        match $what {
            $( functor_encoding::add_ignored_fields!($qualified $fields) => {
                functor_encoding::encode_functor_impl!($context, $func $fields)
            })*
        }
    };
}

#[macro_export]
macro_rules! impl_functor_for_impl {
    ($which:ident => |$self:ident, $context:ident| $block:block) => {
        impl<FunctorContext> functor_encoding::FunctorEncoded<FunctorContext> for $which {
            fn encode_as_functor($self:&Self, $context: &FunctorContext) -> String {
                $block
            }
        }
    };
}

#[macro_export]
macro_rules! impl_functor_for {
    (struct $struct_name:ident $fields:tt) => {
        functor_encoding::impl_functor_for!(struct $struct_name $fields named $struct_name);
    };
    (struct $struct_name:ident $fields:tt named $renamed:ident) => {
        functor_encoding::impl_functor_for_impl!($struct_name => |self, context| {
            functor_encoding::encode_functor!(context, self as struct $struct_name $fields named $renamed)
        });
    };
    (enum $enum_name:ident [ $($func:ident $fields:tt |)* ]) => {
        functor_encoding::impl_functor_for_impl!($enum_name => |self, context| {
            functor_encoding::encode_functor!(context, self as enum $enum_name [ $($func $fields |)* ])
        });
    };
    (primitive $primitive:ident) => {
        functor_encoding::impl_functor_for_impl!($primitive => |self, _context| { self.to_string() });
    };
}

#[macro_export]
macro_rules! impl_functor_for_multi {
    (|$self:ident, $context:ident| [ $( $type_name:ident => $block:block )* ]) => {
        $(functor_encoding::impl_functor_for_impl!($type_name => |$self, $context| $block); )*
    };
}

use crate as functor_encoding;
impl_functor_for!(primitive String);
impl_functor_for!(primitive u64);
impl<FunctorContext, T: FunctorEncoded<FunctorContext>> FunctorEncoded<FunctorContext> for Vec<T> {
    fn encode_as_functor(&self, context: &FunctorContext) -> String {
        std::format!("[{}]", self.iter().map(|v| v.encode_as_functor(context)).join(", "))
    }
}

impl<FunctorContext, T: FunctorEncoded<FunctorContext>> FunctorEncoded<FunctorContext> for Option<T> {
    fn encode_as_functor(&self, context: &FunctorContext) -> String {
        self.as_ref().map(|inner| inner.encode_as_functor(context)).unwrap_or("<NONE>".to_owned())
    }
}

#[cfg(test)]
pub mod test {
    use itertools::Itertools;
    use crate as functor_encoding;

    struct FunctorContext {}
    fn print_encoded(x: impl functor_encoding::FunctorEncoded<FunctorContext>) {
        let dummy = FunctorContext{};
        println!("{}", x.encode_as_functor(&dummy));
    }

    #[test]
    fn testme() {
        print_encoded(TestMeStruct { field: "foo".to_owned() });
        print_encoded(TestMeEnum::First { f1: "abc".to_owned() });
        print_encoded(TestMeEnum::Second { f2: 123 });
        print_encoded(TestMeEnum::Third { f3: TestMeStruct { field: "bar".to_owned() }, ignoreme: 0.456 });
    }

    struct TestMeStruct {
        field: String,
    }

    enum TestMeEnum {
        First { f1: String },
        Second { f2: u64 },
        Third { f3: TestMeStruct, ignoreme: f64 },
    }

    functor_encoding::impl_functor_for!(struct TestMeStruct { field, } );
    functor_encoding::impl_functor_for!(enum TestMeEnum [
        First { f1, } |  Second { f2, } | Third { f3, } |
    ]);
}
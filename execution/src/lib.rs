pub mod execution;
/// We want to execute a DSL for arithmetic operations
/// 1. We have the following operations: Add, Mul, Div, Sub, Lt, Le, Gt, Ge, Eq, Log, Exp, Sin, Cos, Tan, Cast, Const, And, Or, Xor.
/// a. Categories: binary operations, unary operations, zeroary operations, reductions
/// 2. We support the following types: Int, Float, Bool.
/// b. Note that arithmetic operations don't apply for bool, only logical operations.
/// c. No implicit promotion/casting.
/// 3. We operate on Vec<i64>, Vec<f64> and Vec<bool>
pub mod lexer;
pub mod parser;
mod utils;

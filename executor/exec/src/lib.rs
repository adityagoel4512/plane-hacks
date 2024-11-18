use rayon::prelude::*;
mod execution;
/// We want to execute a DSL for arithmetic operations
/// 1. We have the following operations: Add, Mul, Div, Sub, Lt, Le, Gt, Ge, Eq, Log, Exp, Sin, Cos, Tan, Cast, Const, And, Or, Xor.
/// a. Categories: binary operations, unary operations, zeroary operations, reductions
/// 2. We support the following types: Int, Float, Bool.
/// b. Note that arithmetic operations don't apply for bool, only logical operations.
/// c. No implicit promotion/casting.
/// 3. We operate on Vec<i64>, Vec<f64> and Vec<bool>
mod lexer;
mod parser;
mod utils;

pub type EvaluatableResult = Result<String, ()>;
pub trait Evaluatable {
    fn to_owned_string(self) -> EvaluatableResult;
}

impl Evaluatable for String {
    fn to_owned_string(self) -> EvaluatableResult {
        Ok(self)
    }
}

impl Evaluatable for std::fs::File {
    fn to_owned_string(self) -> EvaluatableResult {
        let bufreader = std::io::BufReader::new(self);
        std::io::read_to_string(bufreader).map_err(|_| ())
    }
}

pub fn evaluate(input: impl Evaluatable) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let program = input.to_owned_string().map_err(|_| "Failed")?;
    let tokens = lexer::lex_multiline(&program)?;
    let (asts, fails): (Vec<_>, Vec<_>) = tokens
        .par_iter()
        .map(|tok_stream| parser::parse(tok_stream))
        .partition_map(|res| match res {
            Ok(ast) => itertools::Either::Left(ast),
            Err(e) => itertools::Either::Right(e),
        });

    if !fails.is_empty() {
        println!("Failed to parse: {fails:?}");
    }
    let mut gs: Vec<_> = asts
        .iter()
        .map(execution::ExecutionGraph::build_execution_graph)
        .map(|g| g.unwrap())
        .collect();

    let mut results = vec![];
    for g in &mut gs {
        let handle = g.subscribe().unwrap();
        g.initialize_par_iter().unwrap();
        let result = handle.recv().unwrap();
        results.push(result.as_ref().to_string());
    }
    Ok(results)
}

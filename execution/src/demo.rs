use execution::execution::ExecutionGraph;
use execution::lexer::lex_multiline;
use execution::parser::parse;
use rayon::prelude::*;
use std::io::read_to_string;

fn main() {
    let mut args = std::env::args();
    args.next().expect("program name");
    let program = read_to_string(std::io::BufReader::new(
        std::fs::File::open(args.next().expect("should provide file name").as_str()).unwrap(),
    ))
    .unwrap();

    let tokens = lex_multiline(program.as_str()).expect("Failed to lex");
    // We could parse tok stream to each ast builder
    let (asts, fails): (Vec<_>, Vec<_>) = tokens
        .par_iter()
        .map(|tok_stream| parse(tok_stream))
        .partition_map(|res| match res {
            Ok(ast) => itertools::Either::Left(ast),
            Err(e) => itertools::Either::Right(e),
        });

    if !fails.is_empty() {
        println!("Failed to parse: {fails:?}");
    }
    let mut gs: Vec<_> = asts
        .iter()
        .map(ExecutionGraph::build_execution_graph)
        .map(|g| g.unwrap())
        .collect();

    for g in &mut gs {
        let handle = g.subscribe().unwrap();
        g.initialize_par_iter().unwrap();
        let result = handle.recv().unwrap();
        eprintln!("{result:?}");
    }
}

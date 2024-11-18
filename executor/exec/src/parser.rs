/// hacky parser: &[Token] => Result<AST>
use crate::lexer::{Term, Token, Token::*};
use crate::print_tid;
use std::result::Result;

#[derive(Debug)]
pub struct ParseError;

pub type ParseResult<T> = Result<T, ParseError>;

#[derive(Debug)]
pub struct ParseNode {
    pub(crate) dependencies: Vec<ParseNode>,
    pub(crate) token: Token,
}
// TODO: easy type inference?
// Grammar:
// expr -> term (binop expr)? | unop expr | '(' expr ')'
// term -> Int | Bool | Float | Var

fn parse_term(term: &Term) -> ParseResult<ParseNode> {
    Ok(ParseNode {
        dependencies: vec![],
        token: Token::Term(term.clone()),
    })
}

fn parse_expr<'s>(tokens: &'s [Token]) -> ParseResult<(ParseNode, &'s [Token])> {
    let (node, remaining_slice) = tokens.split_first().ok_or_else(|| ParseError {})?;
    match node {
        LeftParen => {
            // Parse subexpr and then validate ')' matching parenthesis.
            let (subexpr, rest) = parse_expr(remaining_slice)?;
            let (last, restrest) = rest.split_first().ok_or_else(|| ParseError {})?;
            match last {
                RightParen => Ok((subexpr, restrest)),
                _ => Err(ParseError {}),
            }
        }
        Term(term) => {
            let term = parse_term(term)?;
            // Now is there a binary operator?
            if let Some((binop_term, rest)) = remaining_slice.split_first() {
                // Note; no type checking even though it could be feasible here
                match binop_term {
                    RightParen => Ok((term, remaining_slice)),
                    Neg | Plus | Mul | Lt | Le | Gt | Ge | Eq | And | Or | Ne => {
                        // Parse rhs expr
                        let (rhs, residual) = parse_expr(rest)?;
                        Ok((
                            ParseNode {
                                dependencies: vec![term, rhs],
                                token: binop_term.clone(),
                            },
                            residual,
                        ))
                    }
                    _ => Err(ParseError),
                }
            } else {
                Ok((term, remaining_slice))
            }
        }
        Neg | Plus | Sin | Cos => {
            let (subexpr, rest) = parse_expr(remaining_slice)?;
            Ok((
                ParseNode {
                    dependencies: vec![subexpr],
                    token: node.clone(),
                },
                rest,
            ))
        }
        _ => Err(ParseError {}),
    }
}

// There are *zero* type checks or error messages (just if it succeeds or not).
pub fn parse(tokens: &[Token]) -> ParseResult<ParseNode> {
    print_tid!("parse");
    let (node, remaining) = parse_expr(tokens)?;
    if remaining.is_empty() {
        Ok(node)
    } else {
        Err(ParseError {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    #[test]
    fn test_parse() {
        let tokens = lex("12".chars()).unwrap();
        parse(&tokens).unwrap();
        let tokens = lex("( 12 )".chars()).unwrap();
        parse(&tokens).unwrap();
        let tokens = lex(":a + 1".chars()).unwrap();
        parse(&tokens).unwrap();
        let tokens = lex("(1 + (3))".chars()).unwrap();
        parse(&tokens).unwrap();
        let tokens = lex("false || (:input < (10.3 - 9))".chars()).unwrap();
        parse(&tokens).unwrap();
        let tokens = lex(":a * :b < 102".chars()).unwrap();
        parse(&tokens).unwrap();
    }
}

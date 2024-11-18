/// A lexer with minimal error handling
use itertools::Itertools;
use rayon::prelude::*;
use std::collections::VecDeque;

use crate::print_tid;

#[derive(Debug, Clone)]
pub struct LexError {
    substr: String,
}

impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LexError")
    }
}

impl std::error::Error for LexError {}

pub type LexResult<T> = std::result::Result<T, LexError>;

#[derive(Debug, PartialEq, Clone)]
pub enum Term {
    Var(String),
    IntV(i64),
    FloatV(f64),
    BoolV(bool),
}

// TODO: vectors, better error messages.
#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    Term(Term),
    Neg,
    Plus,
    LeftParen,
    RightParen,
    Sin,
    Cos,
    Mul,
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Not,
    And,
    Or,
    Ne,
}

struct PeekIter<'a, Item> {
    deque: VecDeque<Item>,
    iterator: Box<dyn Iterator<Item = Item> + 'a>,
}

impl<'a, V: Copy> PeekIter<'a, V> {
    fn consume_iter<I: Iterator<Item = V> + 'a>(iter: I) -> Self {
        Self {
            deque: VecDeque::new(),
            iterator: Box::new(iter),
        }
    }

    fn peek(&mut self, i: usize) -> Option<V> {
        while self.deque.len() < (i + 1) {
            if let Some(item) = self.iterator.next() {
                self.deque.push_back(item);
            } else {
                return None;
            }
        }
        self.deque.get(i).map(|v| *v)
    }
}

impl<'a, V: Copy + PartialEq> PeekIter<'a, V> {
    fn consume_if_matches<I: Iterator<Item = V>>(&mut self, item: I) -> bool {
        // peek all the way, if it matches advance iterator
        let mut count = 0;
        for (i, v) in item.enumerate() {
            if self.peek(i) != Some(v) {
                return false;
            }
            count += 1;
        }
        for _ in 0..count {
            self.next();
        }
        true
    }
}

impl<'a, V> Iterator for PeekIter<'a, V> {
    type Item = V;
    fn next(&mut self) -> Option<Self::Item> {
        if self.deque.is_empty() {
            if let Some(item) = self.iterator.next() {
                self.deque.push_back(item);
            } else {
                return None;
            }
        }
        self.deque.pop_front()
    }
}

pub fn lex_multiline(program: &str) -> LexResult<Vec<Vec<Token>>> {
    let (successes, failures): (Vec<_>, Vec<_>) = program
        .par_lines()
        .map(|s| lex(s.chars()))
        .partition(|res| res.is_ok());

    if failures.is_empty() {
        Ok(successes
            .iter()
            .map(|ts| ts.as_ref().unwrap().clone())
            .collect())
    } else {
        Err(LexError {
            substr: failures
                .iter()
                .map(|e| e.as_ref().unwrap_err().substr.clone())
                .join("\n"),
        })
    }
}

pub fn lex<I: Iterator<Item = char>>(program: I) -> LexResult<Vec<Token>> {
    print_tid!("lex");
    let mut it = PeekIter::consume_iter(program);
    let mut token_stream = vec![];
    while let Some(c) = it.next() {
        let token = match c {
            '(' => Token::LeftParen,
            ')' => Token::RightParen,
            '+' => Token::Plus,
            '-' => Token::Neg,
            '*' => Token::Mul,
            '<' => {
                if let Some('=') = it.peek(0) {
                    it.next();
                    Token::Le
                } else {
                    Token::Lt
                }
            }
            '>' => {
                if let Some('=') = it.peek(0) {
                    it.next();
                    Token::Ge
                } else {
                    Token::Gt
                }
            }
            '&' => {
                if let Some('&') = it.peek(0) {
                    it.next();
                    Token::And
                } else {
                    return Err(LexError {
                        substr: "Failed to parse `and`".to_owned(),
                    });
                }
            }
            '|' => {
                if let Some('|') = it.peek(0) {
                    it.next();
                    Token::Or
                } else {
                    return Err(LexError {
                        substr: "Failed to parse `or`".to_owned(),
                    });
                }
            }
            '!' => {
                if let Some('=') = it.peek(0) {
                    it.next();
                    Token::Ne
                } else {
                    Token::Not
                }
            }
            '=' => {
                if let Some('=') = it.peek(0) {
                    it.next();
                    Token::Eq
                } else {
                    return Err(LexError {
                        substr: "Failed to parse `eq`".to_owned(),
                    });
                }
            }
            '0'..='9' => {
                let mut numeric_float = false;
                let mut str_rep = c.to_string();
                loop {
                    let peek = it.peek(0);
                    match peek {
                        Some('.') => {
                            if numeric_float {
                                return Err(LexError {
                                    substr: "Failed; cannot have multiple `.` in numeric literal"
                                        .to_string(),
                                });
                            } else {
                                numeric_float = true;
                                str_rep.push(it.next().unwrap());
                            }
                        }
                        Some('0'..='9') => {
                            str_rep.push(it.next().unwrap());
                        }
                        _ => {
                            break;
                        }
                    }
                }
                if numeric_float {
                    Token::Term(Term::FloatV(str_rep.parse().map_err(|_| LexError {
                        substr: "parse error".to_owned(),
                    })?))
                } else {
                    Token::Term(Term::IntV(str_rep.parse().map_err(|_| LexError {
                        substr: "parse error".to_owned(),
                    })?))
                }
            }
            't' => {
                if it.consume_if_matches("rue".chars()) {
                    Token::Term(Term::BoolV(true))
                } else {
                    return Err(LexError {
                        substr: format!("Failed to parse `true`"),
                    });
                }
            }
            'f' => {
                if it.consume_if_matches("alse".chars()) {
                    Token::Term(Term::BoolV(false))
                } else {
                    return Err(LexError {
                        substr: format!("Failed to parse `false`"),
                    });
                }
            }
            's' => {
                if it.consume_if_matches("in".chars()) {
                    Token::Sin
                } else {
                    return Err(LexError {
                        substr: format!("Failed to parse `sin`"),
                    });
                }
            }
            'c' => {
                if it.consume_if_matches("cos".chars()) {
                    Token::Cos
                } else {
                    return Err(LexError {
                        substr: format!("Failed to parse `cos`"),
                    });
                }
            }
            ':' => {
                // Variables signified with ':'
                let mut var_name = String::new();
                while let Some(c) = it.peek(0) {
                    match c {
                        'a'..='z' => {
                            var_name.push(it.next().unwrap());
                        }
                        _ => {
                            break;
                        }
                    }
                }
                Token::Term(Term::Var(var_name))
            }
            ' ' => {
                continue;
            }
            _ => {
                return Err(LexError {
                    substr: format!("Unexpected character: {c}"),
                })
            }
        };
        token_stream.push(token);
    }

    Ok(token_stream)
}

#[cfg(test)]
mod tests {
    use super::Term::*;
    use super::*;
    #[test]
    fn test_literals() {
        let program = "12";
        let result = lex(program.chars()).unwrap();
        assert_eq!(result, vec![Token::Term(Term::IntV(12)),]);

        let program = "-98.232345";
        let result = lex(program.chars()).unwrap();
        assert_eq!(
            result,
            vec![Token::Neg, Token::Term(Term::FloatV(98.232345)),]
        );

        let program = "98.232345";
        let result = lex(program.chars()).unwrap();
        assert_eq!(result, vec![Token::Term(Term::FloatV(98.232345)),]);

        let program = "98.23234.5";
        lex(program.chars()).expect_err("Double dot");

        let program = "94F";
        lex(program.chars()).expect_err("Unexpected character `F`");

        let program = "true";
        let result = lex(program.chars()).unwrap();
        assert_eq!(result, vec![Token::Term(BoolV(true)),]);

        let program = "false";
        let result = lex(program.chars()).unwrap();
        assert_eq!(result, vec![Token::Term(BoolV(false)),]);
    }

    #[test]
    fn test_expressions() {
        let program = "1 + :a";
        let result = lex(program.chars()).unwrap();
        assert_eq!(
            result,
            vec![
                Token::Term(Term::IntV(1)),
                Token::Plus,
                Token::Term(Term::Var("a".to_owned())),
            ]
        );

        let program = "((10.3 - 9) > :input) || false";
        let result = lex(program.chars()).unwrap();
        assert_eq!(
            result,
            vec![
                Token::LeftParen,
                Token::LeftParen,
                Token::Term(Term::FloatV(10.3)),
                Token::Neg,
                Token::Term(Term::IntV(9)),
                Token::RightParen,
                Token::Gt,
                Token::Term(Var("input".to_owned())),
                Token::RightParen,
                Token::Or,
                Token::Term(BoolV(false)),
            ]
        );
    }
}

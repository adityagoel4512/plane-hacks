use crate::lexer::Term;
use crate::lexer::Token::*;
use crate::parser::ParseNode;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;
use rayon::prelude::*;
use std::fmt::Debug;
use std::result::Result;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;

#[derive(Debug)]
pub enum Var {
    IntV(Vec<i64>),
    FloatV(Vec<f64>),
    BoolV(Vec<bool>),
}

#[derive(Debug, PartialEq)]
pub enum Dtype {
    Int,
    Float,
    Bool,
}

impl Var {
    fn dtype(&self) -> Dtype {
        match self {
            Self::IntV(_) => Dtype::Int,
            Self::FloatV(_) => Dtype::Float,
            Self::BoolV(_) => Dtype::Bool,
        }
    }

    fn i64(&self) -> Result<&Vec<i64>, String> {
        match self {
            Self::IntV(i) => Ok(i),
            _ => Err(format!("Failed to parse i64: {self:?}")),
        }
    }
    fn bool(&self) -> Result<&Vec<bool>, String> {
        match self {
            Self::BoolV(b) => Ok(b),
            _ => Err(format!("Failed to parse bool: {self:?}")),
        }
    }

    fn f64(&self) -> Result<&Vec<f64>, String> {
        match self {
            Self::FloatV(f) => Ok(f),
            _ => Err(format!("Failed to parse f64: {self:?}")),
        }
    }
}

impl From<i64> for Var {
    fn from(value: i64) -> Self {
        Self::IntV(vec![value])
    }
}

impl From<f64> for Var {
    fn from(value: f64) -> Self {
        Self::FloatV(vec![value])
    }
}

impl From<bool> for Var {
    fn from(value: bool) -> Self {
        Self::BoolV(vec![value])
    }
}

type ExecutionResult = Result<Arc<Var>, String>;
type SenderChannels = Vec<Sender<Arc<Var>>>;
type ReceiverChannel = Receiver<Arc<Var>>;
// type ReceiverChannels = Vec<ReceiverChannel>;

trait OperatorTrait: Debug {
    fn new(parser: &ParseNode) -> Result<ExecutionGraph, ()>
    where
        Self: Sized;
    fn compute(&self) -> Result<(), String>;
    fn subscribe(&mut self) -> Receiver<Arc<Var>>;
}

enum OperatorEnum {
    Constant(Constant),
    BinOp(BinaryOperator),
}

impl OperatorEnum {
    fn subscribe(&mut self) -> Receiver<Arc<Var>> {
        match self {
            Self::Constant(c) => c.subscribe(),
            Self::BinOp(bop) => bop.subscribe(),
        }
    }

    fn compute(&mut self) -> Result<(), String> {
        match self {
            Self::Constant(c) => c.compute(),
            Self::BinOp(bop) => bop.compute(),
        }
    }
}

pub struct ExecutionGraph {
    ops: Vec<OperatorEnum>,
}

impl ExecutionGraph {
    pub fn build_execution_graph(parser: &ParseNode) -> Result<Self, ()> {
        match parser.token {
            Plus | Neg | Mul if parser.dependencies.len() == 2 => BinaryOperator::new(parser),
            Term(_) => Constant::new(parser),
            _ => Err(()),
        }
    }

    fn merge(&mut self, mut other: Self) -> &Self {
        self.ops.append(&mut other.ops);
        self
    }

    fn current_mut(&mut self) -> Option<&mut OperatorEnum> {
        self.ops.first_mut()
    }

    pub fn initialize(&mut self) -> Result<(), String> {
        for c in self.ops.iter_mut().rev() {
            c.compute()?;
        }
        Ok(())
    }

    pub fn initialize_par_iter(&mut self) -> Result<(), String> {
        let (_, fails): (Vec<_>, Vec<_>) = self
            .ops
            .par_iter_mut()
            .rev()
            .map(|v| v.compute())
            .partition_map(|v| match v {
                Err(_) => itertools::Either::Right(()),
                Ok(res) => itertools::Either::Left(res),
            });
        if fails.is_empty() {
            Ok(())
        } else {
            Err("Failed initialization".to_string())
        }
    }

    pub fn subscribe(&mut self) -> Option<ReceiverChannel> {
        self.current_mut().map(|v| v.subscribe())
    }
}

#[derive(Debug)]
struct Constant {
    broadcasts_to: SenderChannels,
    item: Arc<Var>,
}

struct BinaryOperator {
    broadcasts_to: SenderChannels,
    lhs: ReceiverChannel,
    rhs: ReceiverChannel,
    f: Box<dyn Fn(Arc<Var>, Arc<Var>) -> ExecutionResult + Send + Sync>,
}

impl std::fmt::Debug for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BinaryOp").finish()
    }
}

impl OperatorTrait for Constant {
    fn new(parser: &ParseNode) -> Result<ExecutionGraph, ()> {
        if let Term(t) = &parser.token {
            let ops: Result<Vec<OperatorEnum>, ()> = match t {
                Term::BoolV(b) => Ok(vec![OperatorEnum::Constant(Self {
                    broadcasts_to: vec![],
                    item: Arc::new(Var::BoolV(vec![*b])),
                })]),
                Term::IntV(i) => Ok(vec![OperatorEnum::Constant(Self {
                    broadcasts_to: vec![],
                    item: Arc::new(Var::IntV(vec![*i])),
                })]),
                Term::FloatV(f) => Ok(vec![OperatorEnum::Constant(Self {
                    broadcasts_to: vec![],
                    item: Arc::new(Var::FloatV(vec![*f])),
                })]),
                _ => unreachable!(),
            };
            ops.map(|v| ExecutionGraph { ops: v })
        } else {
            Err(())
        }
    }

    fn compute(&self) -> Result<(), String> {
        dbg!(std::thread::current().id());
        for sender in &self.broadcasts_to {
            sender
                .send(self.item.clone())
                .map_err(|_| "Failed to Send".to_string())?;
        }
        Ok(())
    }

    fn subscribe(&mut self) -> Receiver<Arc<Var>> {
        let (sender, receiver): (Sender<Arc<Var>>, Receiver<Arc<Var>>) = channel();
        self.broadcasts_to.push(sender);
        receiver
    }
}

impl OperatorTrait for BinaryOperator {
    fn new(parser: &ParseNode) -> Result<ExecutionGraph, ()> {
        if let [lhs, rhs] = parser.dependencies.as_slice() {
            let mut lhs_op = ExecutionGraph::build_execution_graph(lhs)?;
            let mut rhs_op = ExecutionGraph::build_execution_graph(rhs)?;
            let lhs = lhs_op.current_mut().unwrap().subscribe();
            let rhs = rhs_op.current_mut().unwrap().subscribe();
            let broadcasts_to: SenderChannels = vec![];
            let f = match &parser.token {
                Plus => |x: Arc<Var>, y: Arc<Var>| match (x.as_ref(), y.as_ref()) {
                    (Var::IntV(i1), Var::IntV(i2)) => Ok(Arc::new(Var::IntV(
                        i1.par_iter().zip(i2).map(|(x, y)| x + y).collect(),
                    ))),
                    (Var::FloatV(f1), Var::FloatV(f2)) => Ok(Arc::new(Var::FloatV(
                        f1.par_iter().zip(f2).map(|(x, y)| x + y).collect(),
                    ))),
                    (Var::FloatV(f1), Var::IntV(i2)) => Ok(Arc::new(Var::FloatV(
                        f1.par_iter()
                            .zip(i2)
                            .map(|(x, y)| x + (*y as f64))
                            .collect(),
                    ))),
                    (Var::IntV(i1), Var::FloatV(f2)) => Ok(Arc::new(Var::FloatV(
                        i1.par_iter()
                            .zip(f2)
                            .map(|(x, y)| (*x as f64) + y)
                            .collect(),
                    ))),
                    _ => Err("Invalid types".to_string()),
                },
                Mul => |x: Arc<Var>, y: Arc<Var>| match (x.as_ref(), y.as_ref()) {
                    (Var::IntV(i1), Var::IntV(i2)) => Ok(Arc::new(Var::IntV(
                        i1.par_iter().zip(i2).map(|(x, y)| x * y).collect(),
                    ))),
                    (Var::FloatV(f1), Var::FloatV(f2)) => Ok(Arc::new(Var::FloatV(
                        f1.par_iter().zip(f2).map(|(x, y)| x * y).collect(),
                    ))),
                    (Var::FloatV(f1), Var::IntV(i2)) => Ok(Arc::new(Var::FloatV(
                        f1.par_iter()
                            .zip(i2)
                            .map(|(x, y)| x * (*y as f64))
                            .collect(),
                    ))),
                    (Var::IntV(i1), Var::FloatV(f2)) => Ok(Arc::new(Var::FloatV(
                        i1.par_iter()
                            .zip(f2)
                            .map(|(x, y)| (*x as f64) * y)
                            .collect(),
                    ))),
                    _ => Err("Invalid types".to_string()),
                },
                Sub => |x: Arc<Var>, y: Arc<Var>| match (x.as_ref(), y.as_ref()) {
                    (Var::IntV(i1), Var::IntV(i2)) => Ok(Arc::new(Var::IntV(
                        i1.par_iter().zip(i2).map(|(x, y)| x - y).collect(),
                    ))),
                    (Var::FloatV(f1), Var::FloatV(f2)) => Ok(Arc::new(Var::FloatV(
                        f1.par_iter().zip(f2).map(|(x, y)| x - y).collect(),
                    ))),
                    (Var::FloatV(f1), Var::IntV(i2)) => Ok(Arc::new(Var::FloatV(
                        f1.par_iter()
                            .zip(i2)
                            .map(|(x, y)| x - (*y as f64))
                            .collect(),
                    ))),
                    (Var::IntV(i1), Var::FloatV(f2)) => Ok(Arc::new(Var::FloatV(
                        i1.par_iter()
                            .zip(f2)
                            .map(|(x, y)| (*x as f64) - y)
                            .collect(),
                    ))),
                    _ => Err("Invalid types".to_string()),
                },
                _ => todo!("Haven't filled in all the `BinOp`"),
            };
            let binop = OperatorEnum::BinOp(Self {
                broadcasts_to,
                lhs,
                rhs,
                f: Box::new(f),
            });
            let mut g = ExecutionGraph { ops: vec![binop] };
            g.merge(lhs_op);
            g.merge(rhs_op);
            Ok(g)
        } else {
            Err(())
        }
    }

    fn compute(&self) -> Result<(), String> {
        // Wait on inputs
        dbg!(std::thread::current().id());
        let lhs = self.lhs.recv().map_err(|e| e.to_string())?;
        let rhs = self.rhs.recv().map_err(|e| e.to_string())?;
        let result = (*self.f)(lhs, rhs)?;
        // Send to all subscribers
        for subscriber in &self.broadcasts_to {
            subscriber.send(result.clone()).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    fn subscribe(&mut self) -> Receiver<Arc<Var>> {
        let (sender, receiver): (Sender<Arc<Var>>, Receiver<Arc<Var>>) = channel();
        self.broadcasts_to.push(sender);
        receiver
    }
}

#[cfg(test)]
mod tests {
    use crate::{lexer::lex, parser::parse};

    use super::*;

    #[test]
    fn test_addition() {
        let a_node = ParseNode {
            dependencies: vec![],
            token: Term(Term::IntV(5)),
        };

        let b_node = ParseNode {
            dependencies: vec![],
            token: Term(Term::IntV(15)),
        };

        let c_node = ParseNode {
            dependencies: vec![a_node, b_node],
            token: Plus,
        };

        let mut g = ExecutionGraph::build_execution_graph(&c_node).unwrap();
        let handle = g.subscribe().unwrap();
        g.initialize().unwrap();
        let result = handle.recv().unwrap();
        assert_eq!(result.i64().unwrap().to_owned(), vec![20i64]);
    }

    #[test]
    fn end_to_end() {
        let program = "5 * (10 + 3)";
        let tokens = lex(program.chars()).unwrap();
        let ast = parse(&tokens).unwrap();
        let mut g = ExecutionGraph::build_execution_graph(&ast).unwrap();
        let handle = g.subscribe().unwrap();
        g.initialize().unwrap();
        let result = handle.recv().unwrap();
        assert_eq!(result.i64().unwrap().to_owned(), vec![65]);
    }

    #[test]
    fn end_to_end_with_par_iter() {
        let program = "5 * (10 + 3)";
        let tokens = lex(program.chars()).unwrap();
        let ast = parse(&tokens).unwrap();
        let mut g = ExecutionGraph::build_execution_graph(&ast).unwrap();
        let handle = g.subscribe().unwrap();
        g.initialize_par_iter().unwrap();
        let result = handle.recv().unwrap();
        assert_eq!(result.i64().unwrap().to_owned(), vec![65]);
    }
}

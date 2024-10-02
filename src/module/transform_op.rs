use metricsql_parser::binaryop::get_scalar_binop_handler;
use metricsql_parser::prelude::{BinopFunc, Operator as BaseOp};
use phf::phf_map;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use valkey_module::ValkeyError;

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum TransformOperator {
    Add,
    And,
    Avg,
    Default,
    Div,
    #[default]
    Eql,
    Mod,
    Mul,
    Pow,
    Sub,
    Gt,
    Gte,
    If,
    IfNot,
    Lt,
    Lte,
    Max,
    Min,
    NotEq,
    Or,
    Unless,
}

pub static BINARY_OPS_MAP: phf::Map<&'static str, TransformOperator> = phf_map! {
    "+" => TransformOperator::Add,
    "-" => TransformOperator::Sub,
    "*" => TransformOperator::Mul,
    "/" => TransformOperator::Div,
    "%" => TransformOperator::Mod,
    "^" => TransformOperator::Pow,

    // cmp ops
    "==" => TransformOperator::Eql,
    "!=" => TransformOperator::NotEq,
    "<" => TransformOperator::Lt,
    ">" => TransformOperator::Gt,
    "<=" => TransformOperator::Lte,
    ">=" => TransformOperator::Gte,

    "add" => TransformOperator::Add,
    "eq" => TransformOperator::Eql,
    "gt" => TransformOperator::Gt,
    "gte" => TransformOperator::Gte,
    "sub" => TransformOperator::Sub,
    "mod" => TransformOperator::Mod,
    "mul" => TransformOperator::Mul,
    "ne"  => TransformOperator::NotEq,
    "lt" => TransformOperator::Lt,
    "lte" => TransformOperator::Lte,
    "div" => TransformOperator::Div,
    "pow" => TransformOperator::Pow,

    // logic set ops
    "and" => TransformOperator::And,
    "or" => TransformOperator::Or,
    "unless" => TransformOperator::Unless,

    "if" => TransformOperator::If,
    "ifnot" => TransformOperator::IfNot,
    "default" => TransformOperator::Default,

    "avg" => TransformOperator::Avg,
    "max" => TransformOperator::Max,
    "min" => TransformOperator::Min,
};

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum BinaryOpKind {
    Arithmetic,
    Comparison,
    Logical,
}


impl TransformOperator {

    #[inline]
    pub const fn kind(&self) -> BinaryOpKind {
        use BinaryOpKind::*;
        use TransformOperator::*;

        match self {
            Add | Sub | Mul | Div | Mod | Pow | Max | Min | Avg => Arithmetic,
            Eql | Gte | Gt | Lt | Lte | NotEq => Comparison,
            And | Unless | Or | If | IfNot | Default => Logical,
        }
    }

    pub const fn is_arithmetic_op(&self) -> bool {
        use TransformOperator::*;
        matches!(self, Add | Sub | Mul | Div | Mod | Pow | Max | Min | Avg)
    }

    pub const fn is_logical_op(&self) -> bool {
        use TransformOperator::*;
        matches!(self, And | Or | Unless | If | IfNot | Default)
    }

    pub const fn is_comparison(&self) -> bool {
        use TransformOperator::*;
        matches!(self, Eql | Gte | Gt | Lt | Lte | NotEq)
    }

    #[inline]
    pub const fn is_set_operator(&self) -> bool {
        use TransformOperator::*;
        matches!(self, And | Or | Unless)
    }

    pub const fn as_str(&self) -> &'static str {
        use TransformOperator::*;
        match self {
            Add => "+",
            And => "and",
            Default => "default",
            Div => "/",
            Eql => "==",
            Gt => ">",
            Gte => ">=",
            If => "if",
            IfNot => "ifNot",
            Mod => "%",
            Mul => "*",
            Lt => "<",
            Lte => "<=",
            NotEq => "!=",
            Or => "or",
            Pow => "^",
            Sub => "-",
            Unless => "unless",
            Avg => "avg",
            Max => "max",
            Min => "min"
        }
    }

    pub const fn get_handler(&self) -> BinopFunc {
        // cheat and use code from base library. We only need to handle max, min, avg

        const fn h(op: BaseOp) -> BinopFunc {
            get_scalar_binop_handler(op, true)
        }

        use TransformOperator::*;
        match self {
            Max => max,
            Min => min,
            Avg => avg,
            Add => h(BaseOp::Add),
            And => h(BaseOp::And),
            Default => h(BaseOp::Default),
            Div => h(BaseOp::Div),
            Eql => h(BaseOp::Eql),
            Mod => h(BaseOp::Mod),
            Mul => h(BaseOp::Mul),
            Pow => h(BaseOp::Pow),
            Sub => h(BaseOp::Sub),
            Gt => h(BaseOp::Gt),
            Gte => h(BaseOp::Gte),
            If => h(BaseOp::If),
            IfNot => h(BaseOp::IfNot),
            Lt => h(BaseOp::Lt),
            Lte => h(BaseOp::Lte),
            NotEq => h(BaseOp::NotEq),
            Or => h(BaseOp::Or),
            Unless => h(BaseOp::Unless),
        }

    }
}

impl FromStr for TransformOperator {
    type Err = ValkeyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        TransformOperator::try_from(s)
    }
}

impl TryFrom<&str> for TransformOperator {
    type Error = ValkeyError;

    fn try_from(op: &str) -> Result<Self, Self::Error> {
        if let Some(ch) = op.chars().next() {
            let value = if !ch.is_alphabetic() {
                BINARY_OPS_MAP.get(op)
            } else {
                // slight optimization - don't lowercase if not needed (save allocation)
                BINARY_OPS_MAP.get(op).or_else(|| {
                    let lower = op.to_ascii_lowercase();
                    BINARY_OPS_MAP.get(&lower)
                })
            };
            if let Some(operator) = value {
                return Ok(*operator);
            }
        }
        Err(ValkeyError::String(format!("Unknown binary op {}", op)))
    }
}



impl fmt::Display for TransformOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.as_str())?;
        Ok(())
    }
}

pub fn is_binary_op(op: &str) -> bool {
    if let Some(ch) = op.chars().next() {
        return if !ch.is_alphabetic() {
            BINARY_OPS_MAP.contains_key(op)
        } else {
            BINARY_OPS_MAP.contains_key(op.to_lowercase().as_str())
        };
    }
    false
}

fn min(x: f64, y: f64) -> f64 {
    x.min(y)
}

fn max(x: f64, y: f64) -> f64 {
    x.max(y)
}

fn avg(x: f64, y: f64) -> f64 {
    (x + y) / 2.0
}

#[cfg(test)]
mod tests {
    use crate::module::transform_op::is_binary_op;

    #[test]
    fn test_is_binary_op_success() {
        let f = |s: &str| assert!(is_binary_op(s), "expecting valid binaryOp: {}", s);

        f("and");
        f("AND");
        f("unless");
        f("unleSS");
        f("==");
        f("!=");
        f(">=");
        f("<=");
        f("or");
        f("Or");
        f("+");
        f("-");
        f("*");
        f("/");
        f("%");
        f("atan2");
        f("^");
        f(">");
        f("<");
    }

    #[test]
    fn test_is_binary_op_error() {
        let f = |s: &str| {
            assert!(!is_binary_op(s), "unexpected valid binaryOp: {}", s);
        };

        f("foobar");
        f("=~");
        f("!~");
        f("=");
        f("<==");
        f("234");
    }
}

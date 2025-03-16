use crate::query::{constant::Constant, expression::Expression as ExpressionForScan};

use std::fmt;

/**
 * Select の where 句で用いられる条件で、A=B などの比較における A または B を表す
 * 同じ名前の struct が query 以下のパッケージにも存在するが、こちらは実行計画を立てるうえで使うことを意図されている
 */
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expression {
    Constant(Constant),
    Field(String),
}

impl Expression {
    pub fn as_constant(&self) -> Option<&Constant> {
        match self {
            Expression::Constant(constant) => Some(constant),
            _ => None,
        }
    }

    pub fn as_field(&self) -> Option<&String> {
        match self {
            Expression::Field(field_name) => Some(field_name),
            _ => None,
        }
    }
    pub fn convert_for_scan(&self) -> ExpressionForScan {
        match self {
            Expression::Field(field_name) => ExpressionForScan::Field(field_name.clone()),
            Expression::Constant(constant) => ExpressionForScan::Constant(constant.clone()),
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Expression::Constant(constant) => write!(f, "{}", constant),
            Expression::Field(field_name) => write!(f, "{}", field_name),
        }
    }
}

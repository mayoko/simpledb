use std::fmt;

use crate::record::schema::Schema;

use super::{constant::Constant, scan::Scan};

use anyhow::Result as AnyhowResult;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expression {
    Constant(Constant),
    Field(String),
}

/**
 * Select の where 句で用いられる条件で、A=B などの比較における A または B を表す
 */
impl Expression {
    pub fn eval(&self, scan: &Scan) -> AnyhowResult<Constant> {
        match self {
            Expression::Constant(constant) => Ok(constant.clone()),
            Expression::Field(field_name) => match scan {
                Scan::ReadOnly(scan) => scan.get_val(field_name),
                Scan::Updatable(scan) => scan.get_val(field_name),
            },
        }
    }

    pub fn is_field(&self) -> bool {
        matches!(self, Expression::Field(_))
    }

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

    /// この式が schema に適用可能かどうかを判定する
    pub fn can_apply(&self, schema: &Schema) -> bool {
        match self {
            Expression::Constant(_) => true,
            Expression::Field(field_name) => schema.has_field(field_name),
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

use crate::record::schema::Schema;

use super::{constant::Constant, scan::ReadScan};

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
    pub fn eval(&self, scan: &dyn ReadScan) -> AnyhowResult<Constant> {
        match self {
            Expression::Constant(constant) => Ok(constant.clone()),
            Expression::Field(field_name) => scan.get_val(field_name),
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

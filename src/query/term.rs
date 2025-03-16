use crate::record::schema::Schema;

use std::fmt;

use super::{expression::Expression, scan::Scan};

use anyhow::Result as AnyhowResult;
use dyn_clone::DynClone;

/**
 * Select の where 句で用いられる条件のうちの一つを表す (A=B, A<B など)
 */
pub trait Term: fmt::Debug + DynClone {
    /// この term が満たされるかどうかを判定する
    fn is_satisfied(&self, scan: &Scan) -> AnyhowResult<bool>;
    /// この term が schema に適用可能かどうかを判定する
    fn can_apply(&self, schema: &Schema) -> bool;
}

/**
 * A = B の条件を表す term
 */
#[derive(Debug, Clone)]
pub struct EqualTerm {
    lhs: Expression,
    rhs: Expression,
}

impl Term for EqualTerm {
    fn is_satisfied(&self, scan: &Scan) -> AnyhowResult<bool> {
        let lhs_val = self.lhs.eval(scan)?;
        let rhs_val = self.rhs.eval(scan)?;

        Ok(lhs_val == rhs_val)
    }

    fn can_apply(&self, schema: &Schema) -> bool {
        self.lhs.can_apply(schema) && self.rhs.can_apply(schema)
    }
}

impl EqualTerm {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }
}

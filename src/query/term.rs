use std::fmt;
use std::ops::Mul;

use crate::record::schema::Schema;

use super::{constant::Constant, expression::Expression, scan::Scan};

use anyhow::Result as AnyhowResult;
use dyn_clone::DynClone;

/**
 * Select の where 句で用いられる条件のうちの一つを表す (A=B, A<B など)
 */
pub trait Term: std::fmt::Display + std::fmt::Debug + DynClone {
    /// この term が満たされるかどうかを判定する
    fn is_satisfied(&self, scan: &Scan) -> AnyhowResult<bool>;
    /// この term が schema に適用可能かどうかを判定する
    fn can_apply(&self, schema: &Schema) -> bool;
    // この term が満たされるときに、どれだけ scan の結果が絞られるかを返す
    // fn reduction_factor(&self) -> ReductionFactor;
}

/**
 * この term が満たされるときに、どれだけ scan の結果が絞られるかを返す
 * 値が大きいほど scan の結果が絞られる
 */
#[derive(Debug, Clone, Copy)]
pub enum ReductionFactor {
    // この term が満たされるときに、scan の結果が 1/n に絞られる
    Constant(f64),
    // この term が満たされる scan の結果が存在しない
    Infinity(),
}

impl Mul for ReductionFactor {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        match (self, rhs) {
            (ReductionFactor::Constant(lhs), ReductionFactor::Constant(rhs)) => {
                ReductionFactor::Constant(lhs * rhs)
            }
            _ => ReductionFactor::Infinity(),
        }
    }
}

/**
 * A = B の条件を表す term
 */
#[derive(Debug, Clone)]
pub struct EqualTerm {
    lhs: Expression,
    rhs: Expression,
}

impl fmt::Display for EqualTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.lhs, self.rhs)
    }
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

    /// 引数で与えた field と対になっている (等号条件のついている) constant の値を返す
    /// TODO: Term にこの method を入れる場合、Constant ではなく値の範囲を表すようにするべき？
    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        if let Expression::Field(name) = &self.lhs {
            if name == field_name {
                return self.rhs.as_constant().cloned();
            }
        }
        if let Expression::Field(name) = &self.rhs {
            if name == field_name {
                return self.lhs.as_constant().cloned();
            }
        }
        None
    }

    /// 引数で与えた field と対になっている (等号条件のついている) field の値を返す
    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        if let Expression::Field(name) = &self.lhs {
            if name == field_name {
                return self.rhs.as_field().cloned();
            }
        }
        if let Expression::Field(name) = &self.rhs {
            if name == field_name {
                return self.lhs.as_field().cloned();
            }
        }
        None
    }
}

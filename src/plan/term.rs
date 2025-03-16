use super::{expression::Expression, plannable::Plannable, reduction_factor::ReductionFactor};
use crate::plan::plan::Plan;

use std::cmp::max;

use crate::query::{
    constant::Constant,
    term::{EqualTerm as EqualTermForScan, Term as TermForScan},
};

use anyhow::Result as AnyhowResult;

use std::fmt;

/**
 * Select の where 句で A = B の条件を表す term
 * 同じ名前の struct が query 以下のパッケージにも存在するが、こちらは実行計画を立てるうえで使うことを意図されている
 */
#[derive(Debug, Clone)]
pub struct EqualTerm {
    lhs: Expression,
    rhs: Expression,
}

/**
 * Select の where 句で用いられる条件のうちの一つを表す (A=B, A<B など)
 * 同じ名前の struct が query 以下のパッケージにも存在するが、こちらは実行計画を立てるうえで使うことを意図されている
 */
#[derive(Debug, Clone)]
pub enum Term {
    Equal(EqualTerm),
}

impl Plannable for Term {
    fn reduction_factor(&self, plan: &dyn Plan) -> AnyhowResult<ReductionFactor> {
        match self {
            Term::Equal(equal_term) => equal_term.reduction_factor(plan),
        }
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Term::Equal(equal_term) => write!(f, "{}", equal_term),
        }
    }
}

impl Term {
    pub fn convert_for_scan(&self) -> Box<dyn TermForScan> {
        match self {
            Term::Equal(equal_term) => Box::new(equal_term.convert_for_scan()),
        }
    }
}

impl Plannable for EqualTerm {
    fn reduction_factor(&self, plan: &dyn Plan) -> AnyhowResult<ReductionFactor> {
        Ok(match (&self.lhs, &self.rhs) {
            (Expression::Field(left_field), Expression::Field(right_field)) => {
                ReductionFactor::Constant(max(
                    plan.get_distinct_value_estimation(left_field)?,
                    plan.get_distinct_value_estimation(right_field)?,
                ) as f64)
            }
            (Expression::Field(left_field), Expression::Constant(_)) => {
                ReductionFactor::Constant(plan.get_distinct_value_estimation(left_field)? as f64)
            }
            (Expression::Constant(_), Expression::Field(right_field)) => {
                ReductionFactor::Constant(plan.get_distinct_value_estimation(right_field)? as f64)
            }
            (Expression::Constant(lhs), Expression::Constant(rhs)) => {
                if lhs == rhs {
                    ReductionFactor::Constant(1.0)
                } else {
                    ReductionFactor::Infinity()
                }
            }
        })
    }
}

impl EqualTerm {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self { lhs, rhs }
    }

    /// 引数で与えた field と対になっている (等号条件のついている) constant の値を返す
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

    pub fn convert_for_scan(&self) -> EqualTermForScan {
        EqualTermForScan::new(self.lhs.convert_for_scan(), self.rhs.convert_for_scan())
    }
}

impl fmt::Display for EqualTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.lhs, self.rhs)
    }
}

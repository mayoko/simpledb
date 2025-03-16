use crate::{plan::plan::Plan, query::constant::Constant};

use super::{plannable::Plannable, reduction_factor::ReductionFactor, term::Term};
use crate::query::predicate::{
    Predicate as PredicateForScan, ProductPredicate as ProductPredicateForScan,
};

use anyhow::Result as AnyhowResult;

use std::fmt;

/**
 * Select の where 句で用いられる条件を表す (A=B AND C<B など)
 * 同じ名前の struct が query 以下のパッケージにも存在するが、こちらは実行計画を立てるうえで使うことを意図されている
 */
pub enum Predicate {
    Product(ProductPredicate),
}

/// 複数の term の論理積を表す predicate
#[derive(Debug, Clone)]
pub struct ProductPredicate {
    terms: Vec<Term>,
}

impl Plannable for Predicate {
    fn reduction_factor(&self, plan: &dyn Plan) -> AnyhowResult<ReductionFactor> {
        match self {
            Predicate::Product(product_predicate) => product_predicate.reduction_factor(plan),
        }
    }
}

impl Predicate {
    pub fn convert_for_scan(&self) -> Box<dyn PredicateForScan> {
        match self {
            Predicate::Product(product_predicate) => Box::new(product_predicate.convert_for_scan()),
        }
    }
}

impl Plannable for ProductPredicate {
    fn reduction_factor(&self, plan: &dyn Plan) -> AnyhowResult<ReductionFactor> {
        let mut reduction_factor = ReductionFactor::Constant(1.);
        for term in &self.terms {
            reduction_factor *= term.reduction_factor(plan)?;
        }

        Ok(reduction_factor)
    }
}

impl ProductPredicate {
    pub fn new(terms: Vec<Term>) -> Self {
        Self { terms }
    }
    /// 引数で与えた field と対になっている (等号条件のついている) constant の値を返す
    pub fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        for term in &self.terms {
            // Term に EqualTerm しかないので if let で match する必要がない
            let Term::Equal(equal_term) = term;
            if let Some(constant) = equal_term.equates_with_constant(field_name) {
                return Some(constant);
            }
        }
        None
    }

    /// 引数で与えた field と対になっている (等号条件のついている) field の値を返す
    pub fn equates_with_field(&self, field_name: &str) -> Option<String> {
        for term in &self.terms {
            // Term に EqualTerm しかないので if let で match する必要がない
            let Term::Equal(equal_term) = term;
            if let Some(field) = equal_term.equates_with_field(field_name) {
                return Some(field);
            }
        }
        None
    }

    /// scan をする際に必要な Predicate に変換する
    pub fn convert_for_scan(&self) -> ProductPredicateForScan {
        ProductPredicateForScan::new(
            self.terms
                .iter()
                .map(|term| term.convert_for_scan())
                .collect(),
        )
    }
}

impl fmt::Display for ProductPredicate {
    /// SQL の where 句のように表示する
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut query = String::new();
        for (i, term) in self.terms.iter().enumerate() {
            query += &term.to_string();
            if i != self.terms.len() - 1 {
                query += " and ";
            }
        }
        write!(f, "{}", query)
    }
}

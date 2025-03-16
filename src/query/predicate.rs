use crate::record::schema::Schema;

use super::{scan::Scan, term::Term};

use anyhow::Result as AnyhowResult;
use mockall::automock;

/**
 * Select の where 句で用いられる条件を表す (A=B AND C<B など)
 */
#[automock]
pub trait Predicate {
    /// この predicate が満たされるかどうかを判定する
    fn is_satisfied(&self, scan: &Scan) -> AnyhowResult<bool>;
    /// この predicate が schema に適用可能かどうかを判定する
    fn can_apply(&self, schema: &Schema) -> bool;
}

dyn_clone::clone_trait_object!(Term);

/// 複数の term の論理積を表す predicate
#[derive(Debug, Clone)]
pub struct ProductPredicate {
    terms: Vec<Box<dyn Term>>,
}

impl Predicate for ProductPredicate {
    fn is_satisfied(&self, scan: &Scan) -> AnyhowResult<bool> {
        for term in &self.terms {
            if !term.is_satisfied(scan)? {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn can_apply(&self, schema: &Schema) -> bool {
        self.terms.iter().all(|term| term.can_apply(schema))
    }
}

impl ProductPredicate {
    pub fn new(terms: Vec<Box<dyn Term>>) -> Self {
        Self { terms }
    }

    /// schema に適用可能な term のみを残した predicate を返す
    pub fn select_sub_pred(&self, schema: &Schema) -> Self {
        let terms = self
            .terms
            .iter()
            .filter(|term| term.can_apply(schema))
            .cloned()
            .collect();

        Self { terms }
    }

    /// ２つの schema を join して初めて適用可能になる term のみを残した predicate を返す
    pub fn join_sub_pred(&self, schema1: &Schema, schema2: &Schema) -> AnyhowResult<Self> {
        let joined_schema = {
            let mut schema = schema1.clone();
            schema.add_all(schema2)?;
            schema
        };
        let terms = self
            .terms
            .iter()
            .filter(|term| {
                !term.can_apply(schema1)
                    && !term.can_apply(schema2)
                    && term.can_apply(&joined_schema)
            })
            .cloned()
            .collect();

        Ok(Self { terms })
    }
}

use crate::{
    query::{
        scan::{ReadScan, Scan},
        select_scan::SelectScan,
    },
    record::schema::Schema,
};

use super::{plannable::Plannable, predicate::Predicate, reduction_factor::ReductionFactor};

use anyhow::Result as AnyhowResult;
use std::cmp::min;

use super::plan::Plan;

pub struct SelectPlan {
    child: Box<dyn Plan>,
    predicate: Box<Predicate>,
}

impl Plan for SelectPlan {
    fn get_block_access_cost(&self) -> AnyhowResult<u64> {
        self.child.get_block_access_cost()
    }
    fn get_record_access_cost(&self) -> AnyhowResult<u64> {
        let reduction_factor = self.predicate.reduction_factor(self.child.as_ref())?;
        Ok(match reduction_factor {
            ReductionFactor::Constant(c) => {
                (self.child.get_record_access_cost()? as f64 / c) as u64
            }
            ReductionFactor::Infinity() => 0,
        })
    }
    fn get_distinct_value_estimation(&self, field_name: &str) -> AnyhowResult<u64> {
        // 現在の実装では Predicate に ProductPredicate しかないので、ここで match する必要はない
        let Predicate::Product(predicate) = &self.predicate.as_ref();
        Ok(if predicate.equates_with_constant(field_name).is_some() {
            // constant と等しい条件付きの field は distinct value が 1 になる
            1
        } else if let Some(other_field) = predicate.equates_with_field(field_name) {
            // 他の field と等しい条件付きの field は、distinct value が小さい方に揃えられる
            min(
                self.child.get_distinct_value_estimation(field_name)?,
                self.child.get_distinct_value_estimation(&other_field)?,
            )
        } else {
            // それ以外の場合は、元の Plan の distinct value をそのまま使う
            self.child.get_distinct_value_estimation(field_name)?
        })
    }
    fn get_schema(&self) -> &Schema {
        self.child.get_schema()
    }
    fn open_read_scan(&self) -> AnyhowResult<Box<dyn ReadScan>> {
        let scan = self.child.open_read_scan()?;
        Ok(Box::new(SelectScan::new(
            Scan::ReadOnly(scan),
            self.predicate.convert_for_scan(),
        )))
    }
    fn open_update_scan(&self) -> AnyhowResult<Box<dyn crate::query::scan::UpdateScan>> {
        let scan = self.child.open_update_scan()?;
        Ok(Box::new(SelectScan::new(
            Scan::Updatable(scan),
            self.predicate.convert_for_scan(),
        )))
    }
}

impl SelectPlan {
    pub fn new(child: Box<dyn Plan>, predicate: Box<Predicate>) -> Self {
        Self { child, predicate }
    }
}

#[cfg(test)]
mod select_plan_test {
    use crate::{
        plan::{
            expression::Expression,
            plan::MockPlan,
            predicate::ProductPredicate,
            term::{EqualTerm, Term},
        },
        query::constant::Constant,
        record::schema::FieldInfo,
    };

    use super::*;
    use anyhow::anyhow;

    fn setup_plan(block_access_cost: u64, record_access_cost: u64) -> Box<dyn Plan> {
        let schema = {
            let mut schema = Schema::new();
            schema.add_field("field1", FieldInfo::Integer);
            schema.add_field("field2", FieldInfo::String(10));
            schema.add_field("field3", FieldInfo::String(10));
            schema
        };
        let mut plan = MockPlan::new();
        plan.expect_get_block_access_cost()
            .returning(move || Ok(block_access_cost));
        plan.expect_get_record_access_cost()
            .returning(move || Ok(record_access_cost));
        plan.expect_get_distinct_value_estimation()
            .returning(move |field_name| {
                if field_name == "field1" {
                    Ok(10)
                } else if field_name == "field2" {
                    Ok(20)
                } else if field_name == "field3" {
                    Ok(50)
                } else {
                    Err(anyhow!("field not found"))
                }
            });
        plan.expect_get_schema().return_const(schema);
        Box::new(plan)
    }

    fn setup_predicate() -> Predicate {
        let predicate: ProductPredicate = ProductPredicate::new(vec![
            // field1 = 1
            Term::Equal(EqualTerm::new(
                Expression::Field("field1".to_string()),
                Expression::Constant(Constant::Int(1)),
            )),
            // and field2 = field3
            Term::Equal(EqualTerm::new(
                Expression::Field("field2".to_string()),
                Expression::Field("field3".to_string()),
            )),
        ]);
        Predicate::Product(predicate)
    }

    #[test]
    fn block_access_cost_test() {
        let p = setup_plan(10, 1000);
        let select_plan = SelectPlan::new(p, Box::new(setup_predicate()));
        assert_eq!(select_plan.get_block_access_cost().unwrap(), 10); // same as child's block access cost
    }

    #[test]
    fn record_access_cost_test_for_no_predicate() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        // 制限がなければ、元の Plan の record access cost がそのまま使われる
        assert_eq!(select_plan.get_record_access_cost().unwrap(), 1000);
    }

    #[test]
    fn record_access_cost_test_for_single_equal_with_constant_condition() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![
            // field1 = 1
            Term::Equal(EqualTerm::new(
                Expression::Field("field1".to_string()),
                Expression::Constant(Constant::Int(1)),
            )),
        ]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        assert_eq!(select_plan.get_record_access_cost().unwrap(), 100); // 1000 / 10
    }

    #[test]
    fn record_access_cost_test_for_single_equal_with_field_condition() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![
            // field2 = field3
            Term::Equal(EqualTerm::new(
                Expression::Field("field2".to_string()),
                Expression::Field("field3".to_string()),
            )),
        ]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        assert_eq!(select_plan.get_record_access_cost().unwrap(), 20); // 1000 / max(20, 50)
    }

    #[test]
    fn record_access_cost_test_for_multiple_condition() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![
            // field1 = 1 and field2 = field3
            Term::Equal(EqualTerm::new(
                Expression::Field("field1".to_string()),
                Expression::Constant(Constant::Int(1)),
            )),
            Term::Equal(EqualTerm::new(
                Expression::Field("field2".to_string()),
                Expression::Field("field3".to_string()),
            )),
        ]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        assert_eq!(select_plan.get_record_access_cost().unwrap(), 2); // 1000 / (max(20, 50) * 10)
    }

    #[test]
    fn distinct_value_estimation_test_for_no_predicate() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        assert_eq!(
            select_plan.get_distinct_value_estimation("field1").unwrap(),
            10
        );
        assert_eq!(
            select_plan.get_distinct_value_estimation("field2").unwrap(),
            20
        );
        assert_eq!(
            select_plan.get_distinct_value_estimation("field3").unwrap(),
            50
        );
    }
    #[test]
    fn distinct_value_estimation_test_for_constant_equal_condition() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![
            // field1 = 1
            Term::Equal(EqualTerm::new(
                Expression::Field("field1".to_string()),
                Expression::Constant(Constant::Int(1)),
            )),
        ]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        assert_eq!(
            select_plan.get_distinct_value_estimation("field1").unwrap(),
            1
        );
        assert_eq!(
            select_plan.get_distinct_value_estimation("field2").unwrap(),
            20
        );
        assert_eq!(
            select_plan.get_distinct_value_estimation("field3").unwrap(),
            50
        );
    }
    #[test]
    fn distinct_value_estimation_test_for_field_equal_condition() {
        let p = setup_plan(10, 1000);
        let predicate = Predicate::Product(ProductPredicate::new(vec![
            // field2 = field3
            Term::Equal(EqualTerm::new(
                Expression::Field("field2".to_string()),
                Expression::Field("field3".to_string()),
            )),
        ]));
        let select_plan = SelectPlan::new(p, Box::new(predicate));
        assert_eq!(
            select_plan.get_distinct_value_estimation("field1").unwrap(),
            10
        );
        assert_eq!(
            select_plan.get_distinct_value_estimation("field2").unwrap(),
            20
        );
        assert_eq!(
            select_plan.get_distinct_value_estimation("field3").unwrap(),
            20
        );
    }
}

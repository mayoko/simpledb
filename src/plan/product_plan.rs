use crate::{
    query::{
        product_scan::ProductScan,
        scan::{ReadScan, UpdateScan},
    },
    record::schema::Schema,
};

use super::plan::{Plan, PlanError};

use anyhow::{anyhow, Result as AnyhowResult};

pub struct ProductPlan {
    p1: Box<dyn Plan>,
    p2: Box<dyn Plan>,
    schema: Schema,
}

impl Plan for ProductPlan {
    fn get_block_access_cost(&self) -> AnyhowResult<u64> {
        Ok(self.p1.get_block_access_cost()?
            + self.p1.get_record_access_cost()? * self.p2.get_block_access_cost()?)
    }
    fn get_record_access_cost(&self) -> AnyhowResult<u64> {
        Ok(self.p1.get_record_access_cost()? * self.p2.get_record_access_cost()?)
    }
    fn get_distinct_value_estimation(&self, field_name: &str) -> AnyhowResult<u64> {
        if self.p1.get_schema().has_field(field_name) {
            self.p1.get_distinct_value_estimation(field_name)
        } else {
            // field が存在しなかった場合は TablePlan まで遡ってエラーが返されることになる
            self.p2.get_distinct_value_estimation(field_name)
        }
    }
    fn get_schema(&self) -> &Schema {
        &self.schema
    }
    fn open_read_scan(&self) -> AnyhowResult<Box<dyn ReadScan>> {
        let s1 = self.p1.open_read_scan()?;
        let s2 = self.p2.open_read_scan()?;
        Ok(Box::new(ProductScan::new(s1, s2)))
    }
    fn open_update_scan(&self) -> AnyhowResult<Box<dyn UpdateScan>> {
        Err(anyhow!(PlanError::InvalidCall(
            "ProductPlan does not support update".to_string()
        )))
    }
}

impl ProductPlan {
    pub fn new(p1: Box<dyn Plan>, p2: Box<dyn Plan>) -> AnyhowResult<Self> {
        let mut schema = Schema::new();
        schema.add_all(p1.get_schema())?;
        schema.add_all(p2.get_schema())?;
        Ok(Self { p1, p2, schema })
    }
}

#[cfg(test)]
mod product_plan_test {
    use crate::{plan::plan::MockPlan, record::schema::FieldInfo};

    use super::*;

    fn setup_plan(
        block_access_cost: u64,
        record_access_cost: u64,
        distinct_value_estimation: u64,
        schema_field_name: String,
    ) -> Box<dyn Plan> {
        let schema = {
            let mut schema = Schema::new();
            schema.add_field(&schema_field_name, FieldInfo::Integer);
            schema
        };
        let mut plan = MockPlan::new();
        plan.expect_get_block_access_cost()
            .returning(move || Ok(block_access_cost));
        plan.expect_get_record_access_cost()
            .returning(move || Ok(record_access_cost));
        plan.expect_get_distinct_value_estimation()
            .returning(move |field_name| {
                if field_name == schema_field_name {
                    Ok(distinct_value_estimation)
                } else {
                    Err(anyhow!("field not found"))
                }
            });
        plan.expect_get_schema().return_const(schema);
        Box::new(plan)
    }

    #[test]
    fn block_access_cost_test() {
        let p1 = setup_plan(10, 1000, 100, "field1".to_string());
        let p2 = setup_plan(40, 2000, 50, "field2".to_string());
        let product_plan = ProductPlan::new(p1, p2).unwrap();
        assert_eq!(product_plan.get_block_access_cost().unwrap(), 40010); // 10 + 1000 * 40
    }
    #[test]
    fn record_access_cost_test() {
        let p1 = setup_plan(10, 1000, 100, "field1".to_string());
        let p2 = setup_plan(40, 2000, 50, "field2".to_string());
        let product_plan = ProductPlan::new(p1, p2).unwrap();
        assert_eq!(product_plan.get_record_access_cost().unwrap(), 2_000_000); // 1000 * 2000
    }
    #[test]
    fn record_distinct_value_estimation_test() {
        let p1 = setup_plan(10, 1000, 100, "field1".to_string());
        let p2 = setup_plan(40, 2000, 50, "field2".to_string());
        let product_plan = ProductPlan::new(p1, p2).unwrap();
        // p1 の distinct value
        assert_eq!(
            product_plan
                .get_distinct_value_estimation("field1")
                .unwrap(),
            100
        );
        // p2 の distinct value
        assert_eq!(
            product_plan
                .get_distinct_value_estimation("field2")
                .unwrap(),
            50
        );
        // 存在しない field
        assert!(product_plan
            .get_distinct_value_estimation("field3")
            .is_err());
    }
}

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

use crate::{
    query::{
        project_scan::ProjectScan,
        scan::{ReadScan, Scan, UpdateScan},
    },
    record::schema::Schema,
};

use super::plan::{Plan, PlanError};

use anyhow::Result as AnyhowResult;

pub struct ProjectPlan {
    child: Box<dyn Plan>,
    schema: Schema,
}

impl Plan for ProjectPlan {
    fn get_schema(&self) -> &Schema {
        &self.schema
    }
    fn get_block_access_cost(&self) -> AnyhowResult<u64> {
        self.child.get_block_access_cost()
    }
    fn get_record_access_cost(&self) -> AnyhowResult<u64> {
        self.child.get_record_access_cost()
    }
    fn get_distinct_value_estimation(&self, field_name: &str) -> AnyhowResult<u64> {
        self.child.get_distinct_value_estimation(field_name)
    }
    fn open_read_scan(&self) -> AnyhowResult<Box<dyn ReadScan>> {
        let scan = self.child.open_read_scan()?;
        Ok(Box::new(ProjectScan::new(
            Scan::ReadOnly(scan),
            self.schema.fields().iter().cloned().collect(),
        )?))
    }
    fn open_update_scan(&self) -> AnyhowResult<Box<dyn UpdateScan>> {
        let scan = self.child.open_update_scan()?;
        Ok(Box::new(ProjectScan::new(
            Scan::Updatable(scan),
            self.schema.fields().iter().cloned().collect(),
        )?))
    }
}

impl ProjectPlan {
    pub fn new(child: Box<dyn Plan>, field_list: Vec<String>) -> AnyhowResult<ProjectPlan> {
        let mut schema = Schema::new();
        for field_name in field_list {
            schema.add_field(
                &field_name,
                child
                    .get_schema()
                    .info(&field_name)
                    .ok_or(PlanError::InvalidCall(format!(
                        "field {} not found",
                        field_name
                    )))?,
            );
        }
        Ok(ProjectPlan { child, schema })
    }
}

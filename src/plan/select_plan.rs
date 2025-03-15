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

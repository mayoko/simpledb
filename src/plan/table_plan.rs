use std::{cell::RefCell, collections::HashMap, rc::Rc};

use anyhow::Result as AnyhowResult;

use crate::{
    metadata::{metadata_manager::MetadataManager, stat_info::StatInfo},
    query::scan::ReadScan,
    record::{
        layout::Layout,
        schema::Schema,
        table_scan_factory::{TableScanFactory, TableScanFactoryImpl},
    },
    tx::transaction::Transaction,
};

use super::plan::{Plan, PlanError};

pub struct TablePlan {
    table_name: String,
    layout: Layout,
    stat_info: HashMap<String, StatInfo>,
    tx: Rc<RefCell<Transaction>>,
}

impl Plan for TablePlan {
    fn get_block_access_cost(&self) -> AnyhowResult<u64> {
        Ok(self
            .stat_info
            .values()
            // 最初にあった値を取り出す
            .find(|_| true)
            .map(|stat| stat.get_num_blocks())
            .ok_or_else(|| {
                PlanError::InvalidCall(format!(
                    "no block access cost found for table {}",
                    self.table_name
                ))
            })?)
    }
    fn get_record_access_cost(&self) -> AnyhowResult<u64> {
        Ok(self
            .stat_info
            .values()
            // 最初にあった値を取り出す
            .find(|_| true)
            .map(|stat| stat.get_num_records())
            .ok_or_else(|| {
                PlanError::InvalidCall(format!(
                    "no record access cost found for table {}",
                    self.table_name
                ))
            })?)
    }
    fn get_distinct_value_estimation(&self, field_name: &str) -> AnyhowResult<u64> {
        Ok(self
            .stat_info
            .get(field_name)
            .map(|stat| stat.get_num_distinct_values())
            .ok_or_else(|| {
                PlanError::InvalidCall(format!(
                    "no distinct value estimation found for field {} in table {}",
                    field_name, self.table_name
                ))
            })?)
    }
    fn get_schema(&self) -> &Schema {
        self.layout.schema()
    }
    fn open_read_scan(&self) -> AnyhowResult<Box<dyn ReadScan>> {
        let table_scan_factory = TableScanFactoryImpl::new();
        let table_scan =
            table_scan_factory.create_read_only(&self.tx, &self.table_name, &self.layout)?;
        Ok(table_scan)
    }
    fn open_update_scan(&self) -> AnyhowResult<Box<dyn crate::query::scan::UpdateScan>> {
        let table_scan_factory = TableScanFactoryImpl::new();
        let table_scan = table_scan_factory.create(&self.tx, &self.table_name, &self.layout)?;
        Ok(table_scan)
    }
}

impl TablePlan {
    /// table plan の初期化
    /// transaction は metadata manager の内容にアクセスするために必要
    pub fn new(
        table_name: String,
        metadata_manager: &dyn MetadataManager,
        tx: Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<TablePlan> {
        let layout = metadata_manager.get_layout(&table_name, &tx)?;
        let stat_info = metadata_manager.get_table_stat(&table_name, &tx)?;
        Ok(TablePlan {
            table_name,
            layout,
            stat_info,
            tx,
        })
    }
}

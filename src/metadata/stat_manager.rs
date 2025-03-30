use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    rc::Rc,
    sync::Mutex,
};

use anyhow::{anyhow, Result as AnyhowResult};
use dashmap::DashMap;
use thiserror::Error;

use crate::{
    query::constant::Constant,
    record::{layout::Layout, table_scan_factory::TableScanFactory},
    tx::transaction::Transaction,
};

use super::{
    constants::{TBLCAT_TABLE_NAME, TBLCAT_TBLNAME_FIELD},
    stat_info::StatInfo,
    table_manager::TableManager,
};

pub trait StatManager {
    /// 指定されたテーブルの指定されたフィールドの統計情報を取得する
    fn get_field_stat(
        &self,
        table_name: &str,
        field_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<StatInfo>;
    /// 指定されたテーブルの統計情報を取得する
    /// field-name -> 統計情報 のマップを返す
    fn get_table_stat(
        &self,
        table_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<HashMap<String, StatInfo>>;
}

pub enum ErrorKind {
    Internal,
    InvalidCall,
}

#[derive(Error, Debug)]
pub enum StatManagerError {
    #[error("[Internal error] {0}")]
    Internal(String),
    #[error("[Invalid call error] {0}")]
    InvalidCall(String),
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct FieldId {
    table_name: String,
    field_name: String,
}

/**
 * 統計情報を管理するための構造体
 *
 * イベントを受け取って統計情報を更新するなどの実装方針も考えられるが、今回の実装では一定の回数問い合わせがあるたびにテーブルを full scan し直して
 * 統計情報を更新するような方針をとる
 */
pub struct StatManagerImpl<'a> {
    table_manager: &'a dyn TableManager,
    table_scan_factory: Box<dyn TableScanFactory>,
    field_stats: DashMap<FieldId, StatInfo>,
    num_calls: Mutex<u64>,
}

impl StatManager for StatManagerImpl<'_> {
    fn get_field_stat(
        &self,
        table_name: &str,
        field_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<StatInfo> {
        {
            let mut num_calls = self
                .num_calls
                .lock()
                .map_err(|_| StatManagerError::Internal("Failed to lock mutex".to_string()))?;
            *num_calls += 1;
            if *num_calls > 100 {
                *num_calls = 0;
                self.refresh_statistics(tx.clone())?;
            }
        }
        let field_id = FieldId {
            table_name: table_name.to_string(),
            field_name: field_name.to_string(),
        };
        match self.field_stats.get(&field_id) {
            Some(stat_info) => Ok(*stat_info.value()),
            None => {
                // 統計情報が見つからない場合は再計算する
                let table_layout = self.table_manager.get_layout(table_name, tx)?;
                let table_stats = self.calc_table_stats(table_name, table_layout, &tx)?;
                for (field_id, stat_info) in table_stats {
                    self.field_stats.insert(field_id, stat_info);
                }
                // 再計算しても見つからない場合はエラーを返す
                Ok(*self
                    .field_stats
                    .get(&field_id)
                    .ok_or(anyhow!(StatManagerError::InvalidCall(format!(
                    "Failed to get stat info for field ({}, {}). Probably the field does not exist",
                    table_name, field_name
                ))))?
                    .value())
            }
        }
    }
    fn get_table_stat(
        &self,
        table_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<HashMap<String, StatInfo>> {
        let layout = self.table_manager.get_layout(table_name, tx)?;
        let schema = layout.schema();
        let mut table_stats = HashMap::new();
        for field in schema.fields() {
            let stat_info = self.get_field_stat(table_name, &field, tx)?;
            table_stats.insert(field, stat_info);
        }
        Ok(table_stats)
    }
}

impl<'a> StatManagerImpl<'a> {
    pub fn new(
        table_manager: &'a dyn TableManager,
        table_scan_factory: Box<dyn TableScanFactory>,
    ) -> Self {
        Self {
            table_manager,
            table_scan_factory,
            field_stats: DashMap::new(),
            num_calls: Mutex::new(0),
        }
    }

    /// 統計情報を更新する
    fn refresh_statistics(&self, tx: Rc<RefCell<Transaction>>) -> AnyhowResult<()> {
        self.field_stats.clear();
        let mut tcat_scan = {
            let tcat_layout = self.table_manager.get_layout(TBLCAT_TABLE_NAME, &tx)?;
            self.table_scan_factory
                .create(&tx, TBLCAT_TABLE_NAME, &tcat_layout)?
        };
        while tcat_scan.move_next()? {
            let table_name = tcat_scan.get_string(TBLCAT_TBLNAME_FIELD)?;
            let table_layout = self.table_manager.get_layout(&table_name, &tx)?;
            let stats_for_table = self.calc_table_stats(&table_name, table_layout, &tx)?;
            for (field_id, stat_info) in stats_for_table {
                self.field_stats.insert(field_id, stat_info);
            }
        }

        Ok(())
    }

    /// 統計情報を取得する
    fn calc_table_stats(
        &self,
        table_name: &str,
        table_layout: Layout,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<DashMap<FieldId, StatInfo>> {
        let mut num_blocks = 0u64;
        let mut num_records = 0;
        // 各フィールドのユニークな値を保持するための HashMap を作成
        // 空の HashSet を持った状態で初期化
        let mut field_to_values = {
            let mut field_to_values = HashMap::new();
            for field in table_layout.schema().fields() {
                let field_id = FieldId {
                    table_name: table_name.to_string(),
                    field_name: field,
                };
                field_to_values.insert(field_id, HashSet::new());
            }
            field_to_values
        };

        let mut table_scan = {
            let table_layout = self.table_manager.get_layout(table_name, tx)?;
            self.table_scan_factory
                .create(tx, table_name, &table_layout)?
        };

        while table_scan.move_next()? {
            num_blocks = (table_scan.get_rid()?.block_number() + 1) as u64;
            num_records += 1;
            for field in table_layout.schema().fields() {
                let constant = table_scan.get_val(&field)?;
                let field_id = FieldId {
                    table_name: table_name.to_string(),
                    field_name: field,
                };
                let set = field_to_values.entry(field_id).or_default();
                set.insert(constant);
            }
        }

        let dash_map = DashMap::new();
        for (field_id, values) in field_to_values {
            let num_distinct_values = values.len() as u64;
            let stat_info = StatInfo::new(num_blocks, num_records, num_distinct_values);
            dash_map.insert(field_id, stat_info);
        }
        Ok(dash_map)
    }
}

pub struct StatManagerFactory {}

impl StatManagerFactory {
    pub fn create<'a>(
        table_manager: &'a dyn TableManager,
        table_scan_factory: Box<dyn TableScanFactory>,
    ) -> Box<dyn StatManager + 'a> {
        let stat_manager = StatManagerImpl::new(table_manager, table_scan_factory);
        Box::new(stat_manager)
    }
}

#[cfg(test)]
mod stat_manager_test {
    use super::*;
    use crate::{
        buffer::buffer_manager::BufferManager,
        file::file_manager::FileManager,
        log::log_manager::LogManager,
        metadata::table_manager::MockTableManager,
        query::scan::MockUpdateScan,
        record::{
            rid::Rid,
            schema::{FieldInfo, Schema},
            table_scan_factory::MockTableScanFactory,
        },
        tx::{concurrency::lock_table::LockTable, transaction::TransactionFactory},
    };
    use mockall::predicate::eq;
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    /// transaction factory を作成するための関数
    fn setup_factory(dir: &TempDir) -> TransactionFactory {
        let file_manager = Arc::new(FileManager::new(dir.path(), 400));
        let log_manager = Arc::new(LogManager::new(file_manager.clone(), "test.log").unwrap());
        let buffer_manager = Arc::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            8,
            Some(10),
        ));
        let lock_table = Arc::new(LockTable::new(Some(10)));
        TransactionFactory::new(file_manager, log_manager, buffer_manager, lock_table)
    }

    #[test]
    fn test_get_stat_info() {
        let dir = tempdir().unwrap();

        let factory = setup_factory(&dir);
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));

        let table_manager = {
            let mut table_manager = MockTableManager::new();
            table_manager.expect_get_layout().returning(|_, _| {
                let mut schema = Schema::new();
                schema.add_field("A", FieldInfo::Integer);
                schema.add_field("B", FieldInfo::String(10));
                let layout = Layout::new(schema).unwrap();
                Ok(layout)
            });

            table_manager
        };
        let table_scan_factory = {
            let mut table_scan_factory = MockTableScanFactory::new();
            table_scan_factory.expect_create().returning(|_, _, _| {
                // 値が 2 個入っているようなテーブルスキャンを行う
                let mut table_scan = MockUpdateScan::new();
                table_scan.expect_move_next().once().returning(|| Ok(true));
                table_scan.expect_move_next().once().returning(|| Ok(true));
                table_scan.expect_move_next().once().returning(|| Ok(false));

                table_scan
                    .expect_get_rid()
                    .returning(|| Ok(Rid::new(0, None)));

                // 1 つめのレコード
                table_scan
                    .expect_get_val()
                    .with(eq("A"))
                    .once()
                    .returning(|_| Ok(Constant::Int(1)));
                table_scan
                    .expect_get_val()
                    .with(eq("B"))
                    .once()
                    .returning(|_| Ok(Constant::String("string 1".to_string())));
                // 2 つめのレコード
                table_scan
                    .expect_get_val()
                    .with(eq("A"))
                    .once()
                    .returning(|_| Ok(Constant::Int(1))); // こちらは重複
                table_scan
                    .expect_get_val()
                    .with(eq("B"))
                    .once()
                    .returning(|_| Ok(Constant::String("string 2".to_string()))); // こちらは別の値

                Ok(Box::new(table_scan))
            });

            table_scan_factory
        };

        let stat_manager = StatManagerImpl::new(&table_manager, Box::new(table_scan_factory));

        {
            let result = stat_manager.get_field_stat("tbl", "A", &tx);
            assert!(result.is_ok());
            let stat_info = result.unwrap();
            assert_eq!(stat_info.get_num_blocks(), 1);
            assert_eq!(stat_info.get_num_records(), 2);
            // 重複しているので 1 つしかない
            assert_eq!(stat_info.get_num_distinct_values(), 1);
        }
        {
            let result = stat_manager.get_field_stat("tbl", "B", &tx);
            assert!(result.is_ok());
            let stat_info = result.unwrap();
            assert_eq!(stat_info.get_num_blocks(), 1);
            assert_eq!(stat_info.get_num_records(), 2);
            // こちらはバラバラの値が入っている
            assert_eq!(stat_info.get_num_distinct_values(), 2);
        }
        {
            let result = stat_manager.get_field_stat("tbl", "C", &tx);
            assert!(result.is_err());
        }
        {
            let result = stat_manager.get_table_stat("tbl", &tx);
            assert!(result.is_ok());
            let table_stats = result.unwrap();
            assert_eq!(table_stats.len(), 2);
            let a_stat = table_stats.get("A").unwrap();
            assert_eq!(a_stat.get_num_blocks(), 1);
            assert_eq!(a_stat.get_num_records(), 2);
            assert_eq!(a_stat.get_num_distinct_values(), 1);
            let b_stat = table_stats.get("B").unwrap();
            assert_eq!(b_stat.get_num_blocks(), 1);
            assert_eq!(b_stat.get_num_records(), 2);
            assert_eq!(b_stat.get_num_distinct_values(), 2);
        }
    }
}

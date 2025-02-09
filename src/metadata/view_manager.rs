use std::{cell::RefCell, rc::Rc};

use thiserror::Error;

use crate::{
    query::{read_scan::ReadScanError, update_scan::UpdateScanError},
    record::{
        schema::{FieldInfo, Schema},
        table_scan_factory::{TableScanFactory, TableScanFactoryError, TableScanFactoryImpl},
    },
    tx::transaction::Transaction,
};

use super::{
    constants::{
        MAX_VIEWDEF_LENGTH, MAX_VIEW_NAME_LENGTH, VIEWCAT_TABLE_NAME, VIEWCAT_VIEW_DEF_FIELD,
        VIEWCAT_VIEW_NAME_FIELD,
    },
    table_manager::{TableManager, TableManagerError},
};

/**
 * View の作成及び View の定義情報の取得を行うためのクラス
 *
 * 内部的には viewcat という table に View の定義情報を保存している
 */
pub struct ViewManager<'a> {
    table_manager: &'a dyn TableManager,
    table_scan_factory: Box<dyn TableScanFactory>,
}

#[derive(Error, Debug)]
pub(crate) enum ViewManagerError {
    #[error("table manager error: {0}")]
    TableManager(#[from] TableManagerError),
    #[error("read scan error: {0}")]
    ReadScan(#[from] ReadScanError),
    #[error("update scan error: {0}")]
    UpdateScan(#[from] UpdateScanError),
    #[error("table scan factory error: {0}")]
    TableScanFactory(#[from] TableScanFactoryError),
    #[error("invalid call error: {0}")]
    InvalidCall(String),
}

impl<'a> ViewManager<'a> {
    pub fn new(
        table_manager: &'a dyn TableManager,
        table_scan_factory: Box<dyn TableScanFactory>,
    ) -> ViewManager<'a> {
        ViewManager {
            table_manager,
            table_scan_factory,
        }
    }

    // view manager が view を管理するために必要なファイルがまだ作成されていない場合、作成する
    // このメソッドは何回呼んでも問題ない
    pub fn setup_if_not_exists(
        &self,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<(), ViewManagerError> {
        let mut schema = Schema::new();
        schema.add_field(
            VIEWCAT_VIEW_NAME_FIELD,
            FieldInfo::String(MAX_VIEW_NAME_LENGTH),
        );
        schema.add_field(
            VIEWCAT_VIEW_DEF_FIELD,
            FieldInfo::String(MAX_VIEWDEF_LENGTH),
        );
        self.table_manager
            .create_table(VIEWCAT_TABLE_NAME, schema, tx)?;

        Ok(())
    }

    // view を作成する
    pub fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<(), ViewManagerError> {
        let layout = self
            .table_manager
            .get_layout(VIEWCAT_TABLE_NAME, tx.clone())?;
        let mut ts = self
            .table_scan_factory
            .create(tx, VIEWCAT_TABLE_NAME, &layout)?;
        ts.insert()?;
        ts.set_string(VIEWCAT_VIEW_NAME_FIELD, view_name)?;
        ts.set_string(VIEWCAT_VIEW_DEF_FIELD, view_def)?;
        Ok(())
    }

    // view の定義情報を取得する
    pub fn get_view_def(
        &self,
        view_name: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<String, ViewManagerError> {
        let layout = self
            .table_manager
            .get_layout(VIEWCAT_TABLE_NAME, tx.clone())?;
        let mut ts = self
            .table_scan_factory
            .create(tx, VIEWCAT_TABLE_NAME, &layout)?;
        while ts.move_next()? {
            if ts.get_string(VIEWCAT_VIEW_NAME_FIELD)? == view_name {
                return Ok(ts.get_string(VIEWCAT_VIEW_DEF_FIELD)?);
            }
        }
        Err(ViewManagerError::InvalidCall(format!(
            "view {} not found",
            view_name
        )))
    }
}

#[cfg(test)]
mod view_manager_test {
    use crate::metadata::table_manager::MockTableManager;

    use super::*;
    use crate::{
        buffer::buffer_manager::BufferManager, file::file_manager::FileManager,
        log::log_manager::LogManager, tx::concurrency::lock_table::LockTable,
        tx::transaction::TransactionFactory,
    };

    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

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
    fn test_setup() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        // table manager が create_table を呼び出すことを確認
        let mut table_manager = MockTableManager::new();
        let mut schema = Schema::new();
        schema.add_field(
            VIEWCAT_VIEW_NAME_FIELD,
            FieldInfo::String(MAX_VIEW_NAME_LENGTH),
        );
        schema.add_field(
            VIEWCAT_VIEW_DEF_FIELD,
            FieldInfo::String(MAX_VIEWDEF_LENGTH),
        );
        table_manager
            .expect_create_table()
            .withf(move |actual_table, actual_schema, _actual_tx| {
                actual_schema.clone() == schema && actual_table == VIEWCAT_TABLE_NAME
            })
            .times(1)
            .returning(|_, _, _| Ok(()));
        let table_scan_factory = TableScanFactoryImpl::new();

        let view_manager = ViewManager::new(&table_manager, Box::new(table_scan_factory));
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));

        view_manager.setup_if_not_exists(tx.clone()).unwrap();
    }
}

use std::{cell::RefCell, rc::Rc};

use thiserror::Error;

use crate::{
    query::scan::{ReadScanError, UpdateScanError},
    record::{
        schema::{FieldInfo, Schema},
        table_scan_factory::{TableScanFactory, TableScanFactoryError},
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

pub trait ViewManager {
    fn setup_if_not_exists(&self, tx: &Rc<RefCell<Transaction>>) -> Result<(), ViewManagerError>;
    fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> Result<(), ViewManagerError>;
    fn get_view_def(
        &self,
        view_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> Result<String, ViewManagerError>;
}

/**
 * View の作成及び View の定義情報の取得を行うためのクラス
 *
 * 内部的には viewcat という table に View の定義情報を保存している
 */
pub struct ViewManagerImpl<'a> {
    table_manager: &'a dyn TableManager,
    table_scan_factory: Box<dyn TableScanFactory>,
}

pub struct ViewManagerFactory {}

impl ViewManagerFactory {
    pub fn create<'a>(
        table_manager: &'a dyn TableManager,
        table_scan_factory: Box<dyn TableScanFactory>,
    ) -> Box<dyn ViewManager + 'a> {
        let view_manager = ViewManagerImpl::new(table_manager, table_scan_factory);
        Box::new(view_manager)
    }
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
    // TODO: 治す
    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

impl<'a> ViewManagerImpl<'a> {
    pub fn new(
        table_manager: &'a dyn TableManager,
        table_scan_factory: Box<dyn TableScanFactory>,
    ) -> ViewManagerImpl<'a> {
        ViewManagerImpl {
            table_manager,
            table_scan_factory,
        }
    }
}

impl<'a> ViewManager for ViewManagerImpl<'a> {
    // view manager が view を管理するために必要なファイルがまだ作成されていない場合、作成する
    // このメソッドは何回呼んでも問題ない
    fn setup_if_not_exists(&self, tx: &Rc<RefCell<Transaction>>) -> Result<(), ViewManagerError> {
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
    fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> Result<(), ViewManagerError> {
        let layout = self.table_manager.get_layout(VIEWCAT_TABLE_NAME, tx)?;
        let mut ts = self
            .table_scan_factory
            .create(tx, VIEWCAT_TABLE_NAME, &layout)?;
        ts.insert()?;
        ts.set_string(VIEWCAT_VIEW_NAME_FIELD, view_name)?;
        ts.set_string(VIEWCAT_VIEW_DEF_FIELD, view_def)?;
        Ok(())
    }

    // view の定義情報を取得する
    fn get_view_def(
        &self,
        view_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> Result<String, ViewManagerError> {
        let layout = self.table_manager.get_layout(VIEWCAT_TABLE_NAME, tx)?;
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
    use crate::{
        metadata::table_manager::MockTableManager,
        query::scan::{MockUpdateScan, UpdateScan},
        record::{
            layout::Layout,
            table_scan_factory::{MockTableScanFactory, TableScanFactoryImpl},
        },
    };

    use super::*;
    use crate::{
        buffer::buffer_manager::BufferManager, file::file_manager::FileManager,
        log::log_manager::LogManager, tx::concurrency::lock_table::LockTable,
        tx::transaction::TransactionFactory,
    };

    use mockall::predicate::eq;
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
        let table_manager = {
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
            table_manager
        };

        let table_scan_factory = TableScanFactoryImpl::new();
        let view_manager = ViewManagerImpl::new(&table_manager, Box::new(table_scan_factory));
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));

        view_manager.setup_if_not_exists(&tx).unwrap();
    }

    #[test]
    fn test_create_view() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        // table manager が get_layout を呼び出すことを確認
        let table_manager = {
            let mut table_manager = MockTableManager::new();
            table_manager
                .expect_get_layout()
                .times(1)
                .returning(|_, _| {
                    let mut schema = Schema::new();
                    schema.add_field(
                        VIEWCAT_VIEW_NAME_FIELD,
                        FieldInfo::String(MAX_VIEW_NAME_LENGTH),
                    );
                    schema.add_field(
                        VIEWCAT_VIEW_DEF_FIELD,
                        FieldInfo::String(MAX_VIEWDEF_LENGTH),
                    );
                    let layout = Layout::new(schema).unwrap();
                    Ok(layout)
                });
            table_manager
        };

        // table scan の挙動を確認
        let table_scan_factory = {
            let mut table_scan_factory = MockTableScanFactory::new();
            // create_view で table_scan_factory が create を呼び出すことを確認
            table_scan_factory
                .expect_create()
                .withf(move |_, actual_table, actual_layout| {
                    actual_table == VIEWCAT_TABLE_NAME
                        && actual_layout.schema().has_field(VIEWCAT_VIEW_NAME_FIELD)
                        && actual_layout.schema().has_field(VIEWCAT_VIEW_DEF_FIELD)
                })
                .times(1)
                .returning(move |_, _, _| {
                    let table_scan = {
                        let mut table_scan = MockUpdateScan::new();
                        {
                            table_scan.expect_insert().times(1).returning(|| Ok(()));
                            table_scan
                                .expect_set_string()
                                .with(eq(VIEWCAT_VIEW_NAME_FIELD), eq("view1"))
                                .times(1)
                                .returning(|_, _| Ok(()));
                            table_scan
                                .expect_set_string()
                                .with(eq(VIEWCAT_VIEW_DEF_FIELD), eq("select * from table1"))
                                .times(1)
                                .returning(|_, _| Ok(()));
                        }
                        table_scan
                    };
                    Ok(Box::new(table_scan) as Box<dyn UpdateScan>)
                });
            table_scan_factory
        };

        let view_manager = ViewManagerImpl::new(&table_manager, Box::new(table_scan_factory));
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));

        view_manager
            .create_view("view1", "select * from table1", &tx)
            .unwrap();
    }

    #[test]
    fn test_get_view_def() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        // table manager が get_layout を呼び出すことを確認
        let table_manager = {
            let mut table_manager = MockTableManager::new();
            table_manager
                .expect_get_layout()
                .times(1)
                .returning(|_, _| {
                    let mut schema = Schema::new();
                    schema.add_field(
                        VIEWCAT_VIEW_NAME_FIELD,
                        FieldInfo::String(MAX_VIEW_NAME_LENGTH),
                    );
                    schema.add_field(
                        VIEWCAT_VIEW_DEF_FIELD,
                        FieldInfo::String(MAX_VIEWDEF_LENGTH),
                    );
                    let layout = Layout::new(schema).unwrap();
                    Ok(layout)
                });
            table_manager
        };

        // table scan の挙動を確認
        let table_scan_factory = {
            let mut table_scan_factory = MockTableScanFactory::new();
            // create_view で table_scan_factory が create を呼び出すことを確認
            table_scan_factory
                .expect_create()
                .withf(move |_, actual_table, actual_layout| {
                    actual_table == VIEWCAT_TABLE_NAME
                        && actual_layout.schema().has_field(VIEWCAT_VIEW_NAME_FIELD)
                        && actual_layout.schema().has_field(VIEWCAT_VIEW_DEF_FIELD)
                })
                .times(1)
                .returning(move |_, _, _| {
                    let table_scan = {
                        let mut table_scan = MockUpdateScan::new();
                        table_scan
                            .expect_move_next()
                            .times(1)
                            .returning(|| Ok(true));

                        // record の中身は view1, select * from table1 とする
                        table_scan
                            .expect_get_string()
                            .withf(move |field_name| field_name == VIEWCAT_VIEW_NAME_FIELD)
                            .times(1)
                            .returning(|_| Ok("view1".to_string()));
                        table_scan
                            .expect_get_string()
                            .withf(move |field_name| field_name == VIEWCAT_VIEW_DEF_FIELD)
                            .times(1)
                            .returning(|_| Ok("select * from table1".to_string()));
                        table_scan
                    };
                    Ok(Box::new(table_scan) as Box<dyn UpdateScan>)
                });
            table_scan_factory
        };

        let view_manager = ViewManagerImpl::new(&table_manager, Box::new(table_scan_factory));
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));

        let def = view_manager.get_view_def("view1", &tx).unwrap();
        assert_eq!(def, "select * from table1");
    }
}

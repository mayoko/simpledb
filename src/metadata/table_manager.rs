use std::{cell::RefCell, collections::HashMap, rc::Rc};

use mockall::automock;
use thiserror::Error;

use crate::{
    metadata::constants::{
        FCAT_FLDNAME_FIELD, FCAT_LENGTH_FIELD, FCAT_OFFSET_FIELD, FCAT_TBLNAME_FIELD,
        FCAT_TYPE_FIELD, FLDCAT_TABLE_NAME, MAX_TABLE_NAME_LENGTH, TBLCAT_SLOTSIZE_FIELD,
        TBLCAT_TABLE_NAME,
    },
    query::{read_scan::ReadScanError, update_scan::UpdateScanError},
    record::{
        layout::{Layout, LayoutError},
        schema::{FieldInfo, FieldType, Schema},
        table_scan_factory::{TableScanFactory, TableScanFactoryError, TableScanFactoryImpl},
    },
    tx::transaction::Transaction,
};

use super::constants::MAX_FIELD_NAME_LENGTH;

#[automock]
pub trait TableManager {
    /// table manager が table を管理するために必要なファイルがまだ作成されていない場合、作成する
    /// このメソッドは何回呼んでも問題ない
    fn setup_if_not_exists(&self, tx: Rc<RefCell<Transaction>>) -> Result<(), TableManagerError>;
    // 新しい table を作成する
    // Warning: すでに table が存在する場合、エラーを返すべきだが、その確認は特にしていない
    fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<(), TableManagerError>;
    fn get_layout(
        &self,
        table_name: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<Layout, TableManagerError>;
}

/**
 * table の作成及び table の定義情報の取得を行うためのクラス
 *
 * 内部的には tblcat という table に table の一覧を保存し、fldcat という table に各 table の field の情報を保存している
 */
pub struct TableManagerImpl {
    tcat_layout: Layout,
    fcat_layout: Layout,
    table_scan_factory: Box<dyn TableScanFactory>,
}

#[derive(Error, Debug)]
pub(crate) enum TableManagerError {
    #[error("layout error: {0}")]
    Layout(#[from] LayoutError),
    #[error("table scan factory error: {0}")]
    TableScanFactory(#[from] TableScanFactoryError),
    #[error("read scan error: {0}")]
    ReadScan(#[from] ReadScanError),
    #[error("update scan error: {0}")]
    UpdateScan(#[from] UpdateScanError),
    #[error("invalid call error: {0}")]
    InvalidCall(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl TableManager for TableManagerImpl {
    fn setup_if_not_exists(&self, tx: Rc<RefCell<Transaction>>) -> Result<(), TableManagerError> {
        // tblcat テーブルに tblcat 自身の情報が書いていなければ、初期化をしていなかったと判断して初期化を行う
        let mut tcat =
            self.table_scan_factory
                .create(tx.clone(), TBLCAT_TABLE_NAME, &self.tcat_layout)?;
        while tcat.move_next()? {
            if tcat.get_string(TBLCAT_TABLE_NAME)? == TBLCAT_TABLE_NAME {
                return Ok(());
            }
        }
        self.create_table(
            TBLCAT_TABLE_NAME,
            self.tcat_layout.schema().clone(),
            tx.clone(),
        )?;
        self.create_table(FLDCAT_TABLE_NAME, self.fcat_layout.schema().clone(), tx)?;
        Ok(())
    }

    /// 新しい table を作成する
    /// Warning: すでに table が存在する場合、エラーを返すべきだが、その確認は特にしていない
    fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<(), TableManagerError> {
        let layout = Layout::new(schema.clone())?;

        {
            let mut tcat =
                self.table_scan_factory
                    .create(tx.clone(), TBLCAT_TABLE_NAME, &self.tcat_layout)?;
            tcat.insert()?;
            tcat.set_string(TBLCAT_TABLE_NAME, table_name)?;
            tcat.set_int(TBLCAT_SLOTSIZE_FIELD, layout.slot_size() as i32)?;
        }

        {
            let mut fcat =
                self.table_scan_factory
                    .create(tx.clone(), FLDCAT_TABLE_NAME, &self.fcat_layout)?;
            for field in &schema.fields() {
                fcat.insert()?;
                fcat.set_string(FCAT_TBLNAME_FIELD, table_name)?;
                fcat.set_string(FCAT_FLDNAME_FIELD, field)?;
                match schema.info(field) {
                    Some(info) => {
                        fcat.set_int(FCAT_TYPE_FIELD, info.get_type() as i32)?;
                        fcat.set_int(
                            FCAT_OFFSET_FIELD,
                            layout
                                .offset(field)
                                .ok_or(TableManagerError::InvalidCall(format!(
                                    "field {} not found",
                                    field
                                )))? as i32,
                        )?;
                        match info {
                            FieldInfo::Integer => {
                                fcat.set_int(FCAT_LENGTH_FIELD, 0)?;
                            }
                            FieldInfo::String(length) => {
                                fcat.set_int(FCAT_LENGTH_FIELD, length as i32)?;
                            }
                        }
                    }
                    None => {
                        return Err(TableManagerError::InvalidCall(format!(
                            "field {} not found",
                            field
                        )))
                    }
                }
            }
        }
        Ok(())
    }

    // table の layout を取得する
    fn get_layout(
        &self,
        table_name: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<Layout, TableManagerError> {
        let slot_size = self.get_record_size(table_name, tx.clone())?;
        let (schema, offsets) = self.get_schema_and_offsets(table_name, tx)?;

        Ok(Layout::new_from_existing_settings(
            schema, offsets, slot_size,
        ))
    }
}

impl TableManagerImpl {
    pub fn new(table_scan_factory: Box<dyn TableScanFactory>) -> Result<Self, LayoutError> {
        let mut tcat_schema = Schema::new();
        tcat_schema.add_field(TBLCAT_TABLE_NAME, FieldInfo::String(MAX_TABLE_NAME_LENGTH));
        tcat_schema.add_field(TBLCAT_SLOTSIZE_FIELD, FieldInfo::Integer);
        let tcat_layout = Layout::new(tcat_schema)?;

        let mut fcat_schema = Schema::new();
        fcat_schema.add_field(FCAT_TBLNAME_FIELD, FieldInfo::String(MAX_TABLE_NAME_LENGTH));
        fcat_schema.add_field(FCAT_FLDNAME_FIELD, FieldInfo::String(MAX_FIELD_NAME_LENGTH));
        fcat_schema.add_field(FCAT_TYPE_FIELD, FieldInfo::Integer);
        fcat_schema.add_field(FCAT_LENGTH_FIELD, FieldInfo::Integer);
        fcat_schema.add_field(FCAT_OFFSET_FIELD, FieldInfo::Integer);
        let fcat_layout = Layout::new(fcat_schema)?;

        Ok(Self {
            tcat_layout,
            fcat_layout,
            table_scan_factory,
        })
    }

    fn get_record_size(
        &self,
        table_name: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<usize, TableManagerError> {
        let mut tcat = self
            .table_scan_factory
            .create(tx, TBLCAT_TABLE_NAME, &self.tcat_layout)?;
        while tcat.move_next()? {
            if tcat.get_string(TBLCAT_TABLE_NAME)? == table_name {
                return Ok(tcat.get_int(TBLCAT_SLOTSIZE_FIELD)? as usize);
            }
        }
        Err(TableManagerError::InvalidCall(format!(
            "table {} not found",
            table_name
        )))
    }

    fn get_schema_and_offsets(
        &self,
        table_name: &str,
        tx: Rc<RefCell<Transaction>>,
    ) -> Result<(Schema, HashMap<String, usize>), TableManagerError> {
        let mut schema = Schema::new();
        let mut offsets = HashMap::new();
        let mut fcat = self
            .table_scan_factory
            .create(tx, FLDCAT_TABLE_NAME, &self.fcat_layout)?;
        while fcat.move_next()? {
            if fcat.get_string(FCAT_TBLNAME_FIELD)? == table_name {
                let field_name = fcat.get_string(FCAT_FLDNAME_FIELD)?;
                let field_type_i32 = fcat.get_int(FCAT_TYPE_FIELD)?;
                let field_type = FieldType::from_i32(field_type_i32).map_err(|e| {
                    TableManagerError::Internal(format!(
                        "unexpected field type value: {}. error: {}",
                        field_type_i32, e
                    ))
                })?;
                let field_length = fcat.get_int(FCAT_LENGTH_FIELD)? as usize;
                let field_offset = fcat.get_int(FCAT_OFFSET_FIELD)? as usize;
                schema.add_field(
                    &field_name,
                    match field_type {
                        FieldType::Integer => FieldInfo::Integer,
                        FieldType::String => FieldInfo::String(field_length),
                    },
                );
                offsets.insert(field_name, field_offset);
            }
        }
        Ok((schema, offsets))
    }
}

#[cfg(test)]
mod table_manager_test {
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

    fn setup_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_field("A", FieldInfo::Integer);
        schema.add_field("B", FieldInfo::String(9));

        Layout::new(schema).unwrap()
    }

    #[test]
    fn test_setup() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));
        let table_scan_factory = Box::new(TableScanFactoryImpl::new());

        let table_manager = TableManagerImpl::new(table_scan_factory).unwrap();
        table_manager.setup_if_not_exists(tx.clone()).unwrap();
        // table 管理用に作成した tblcat, fldcat が存在する
        assert!(table_manager
            .get_layout(TBLCAT_TABLE_NAME, tx.clone())
            .is_ok());
        assert!(table_manager
            .get_layout(FLDCAT_TABLE_NAME, tx.clone())
            .is_ok());
        // 他の table は存在しない
        assert!(table_manager
            .get_layout("not_exist_table", tx.clone())
            .is_err());

        // 何回呼び出しても大丈夫
        table_manager.setup_if_not_exists(tx.clone()).unwrap();
        table_manager.setup_if_not_exists(tx.clone()).unwrap();

        tx.borrow_mut().commit().unwrap();
    }

    #[test]
    fn test_create_table() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);
        let tx = Rc::new(RefCell::new(factory.create().unwrap()));
        let table_scan_factory = Box::new(TableScanFactoryImpl::new());

        let table_manager = TableManagerImpl::new(table_scan_factory).unwrap();
        table_manager.setup_if_not_exists(tx.clone()).unwrap();

        let layout = setup_layout();
        table_manager
            .create_table("test_table", layout.schema().clone(), tx.clone())
            .unwrap();
        let layout_from_manager = table_manager.get_layout("test_table", tx.clone()).unwrap();
        assert_eq!(layout, layout_from_manager);

        tx.borrow_mut().commit().unwrap();
    }
}

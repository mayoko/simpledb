use std::{cell::RefCell, rc::Rc};

use thiserror::Error;

use crate::{
    record::{
        schema::{FieldInfo, Schema},
        table_scan::{TableScan, TableScanError},
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
    table_manager: &'a TableManager,
}

#[derive(Error, Debug)]
pub(crate) enum ViewManagerError {
    #[error("table manager error: {0}")]
    TableManager(#[from] TableManagerError),
    #[error("table scan error: {0}")]
    TableScan(#[from] TableScanError),
    #[error("invalid call error: {0}")]
    InvalidCall(String),
    #[error("internal error: {0}")]
    Internal(String),
}

impl<'a> ViewManager<'a> {
    pub fn new(table_manager: &'a TableManager) -> ViewManager<'a> {
        ViewManager { table_manager }
    }

    // view manager が view を管理するために必要なファイルがまだ作成されていない場合、作成する
    // このメソッドは何回呼んでも問題ない
    fn setup_if_not_exists(&self, tx: Rc<RefCell<Transaction>>) -> Result<(), ViewManagerError> {
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
        let mut ts = TableScan::new(tx, VIEWCAT_TABLE_NAME, &layout)?;
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
        let mut ts = TableScan::new(tx, VIEWCAT_TABLE_NAME, &layout)?;
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

use crate::file::blockid::BlockId;
use crate::tx::buffer_list::BufferListError;
use crate::tx::transaction::{Transaction, TransactionSizeError};

use super::layout::Layout;

use super::record_page::{RecordPage, RecordPageError};
use super::table_scan::{TableScan, TableScanImpl};
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;

/// table scan を作成するための factory
/// application 中に何個あっても問題ない
pub trait TableScanFactory {
    fn create(
        &self,
        tx: Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Box<dyn TableScan>, TableScanFactoryError>;
}

pub struct TableScanFactoryImpl;

#[derive(Error, Debug)]
pub(crate) enum TableScanFactoryError {
    #[error("transaction size error: {0}")]
    TransactionSize(#[from] TransactionSizeError),
    #[error("record page error: {0}")]
    RecordPage(#[from] RecordPageError),
}

impl TableScanFactoryImpl {
    pub fn new() -> TableScanFactoryImpl {
        TableScanFactoryImpl {}
    }
}

impl TableScanFactory for TableScanFactoryImpl {
    /// table scan を作成する
    /// table が存在しない場合は新しく作成される
    fn create(
        &self,
        tx: Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Box<dyn TableScan>, TableScanFactoryError> {
        let filename = format!("{}.tbl", tblname);
        let record_page = if tx.borrow_mut().size(&filename)? == 0 {
            let block = tx.borrow_mut().append(&filename)?;
            let record_page = RecordPage::new(tx.clone(), &block, layout);
            record_page.format()?;
            record_page
        } else {
            let block = BlockId::new(&filename, 0);

            RecordPage::new(tx.clone(), &block, layout)
        };
        Ok(Box::new(TableScanImpl {
            tx: tx.clone(),
            layout: layout.clone(),
            record_page,
            filename,
            current_slot: None,
        }))
    }
}

use crate::file::blockid::BlockId;
use crate::query::scan::{ReadScan, UpdateScan};
use crate::tx::transaction::{Transaction, TransactionSizeError};

use super::layout::Layout;

use super::record_page::{RecordPage, RecordPageError};
use super::table_scan::TableScanImpl;
use mockall::automock;
use std::{cell::RefCell, rc::Rc};
use thiserror::Error;

/// table scan を作成するための factory
/// application 中に何個あっても問題ない
#[automock]
pub trait TableScanFactory {
    /// table scan を作成する
    fn create(
        &self,
        tx: &Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Box<dyn UpdateScan>, TableScanFactoryError>;
    /// read-only な table scan を作成する
    /// update が必要ない場合にはこちらを使う
    fn create_read_only(
        &self,
        tx: &Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Box<dyn ReadScan>, TableScanFactoryError>;
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
    fn create(
        &self,
        tx: &Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Box<dyn UpdateScan>, TableScanFactoryError> {
        Ok(Box::new(self.create_internal(tx, tblname, layout)?))
    }
    fn create_read_only(
        &self,
        tx: &Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Box<dyn ReadScan>, TableScanFactoryError> {
        Ok(Box::new(self.create_internal(tx, tblname, layout)?))
    }
}

impl TableScanFactoryImpl {
    /// TableScanImpl を作成する
    fn create_internal(
        &self,
        tx: &Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<TableScanImpl, TableScanFactoryError> {
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
        Ok(TableScanImpl {
            tx: tx.clone(),
            layout: layout.clone(),
            record_page,
            filename,
            current_slot: None,
        })
    }
}

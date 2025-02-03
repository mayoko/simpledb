use crate::{
    file::blockid::BlockId,
    query::constant::Constant,
    tx::buffer_list::BufferListError,
    tx::transaction::{Transaction, TransactionSizeError},
};

use super::{
    layout::Layout,
    record_page::{RecordPage, RecordPageError},
    rid::Rid,
    schema::FieldInfo,
};

use thiserror::Error;

use std::{cell::RefCell, rc::Rc};

/**
 * table の record を取得・操作するための構造体
 *
 * move_next など、record の指し示す位置 (cursor) を移動させるメソッドを呼んだあとに get_val, insert などのメソッドを呼ぶことで table の record を操作できる
 *
 * フィールドの長さは固定長で、Unspanned (page をまたいで record を保存することがない) と仮定している
 */
pub struct TableScan {
    tx: Rc<RefCell<Transaction>>,
    layout: Layout,
    // 現在の record が格納されている RecordPage
    record_page: RecordPage,
    filename: String,
    current_slot: Option<usize>,
}

#[derive(Error, Debug)]
pub(crate) enum TableScanError {
    #[error("invalid call error: {0}")]
    InvalidCall(String),
    #[error("buffer list error: {0}")]
    BufferList(#[from] BufferListError),
    #[error("transaction size error: {0}")]
    TransactionSize(#[from] TransactionSizeError),
    #[error("record page error: {0}")]
    RecordPage(#[from] RecordPageError),
}

impl TableScan {
    // table scan を作成する
    // table が存在しない場合は新しく作成される
    pub fn new(
        tx: Rc<RefCell<Transaction>>,
        tblname: &str,
        layout: &Layout,
    ) -> Result<Self, TableScanError> {
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
        Ok(TableScan {
            tx: tx.clone(),
            layout: layout.clone(),
            record_page,
            filename,
            current_slot: None,
        })
    }

    // record の存在する、次の slot に移動する。record が存在しない場合は false を返す
    pub fn move_next(&mut self) -> Result<bool, TableScanError> {
        self.current_slot = self.record_page.next_after(self.current_slot)?;
        while self.current_slot.is_none() {
            if self.is_at_last_block()? {
                return Ok(false);
            }
            let next_block_num = self.record_page.block().number() + 1;
            let block = BlockId::new(&self.filename, next_block_num);
            self.move_to_block(&block)?;
            self.current_slot = self.record_page.next_after(None)?;
        }
        Ok(true)
    }

    // 今いる slot に対して、指定した field の値を取得する
    pub fn get_val(&self, field_name: &str) -> Result<Constant, TableScanError> {
        let slot = match self.current_slot {
            None => {
                return Err(TableScanError::InvalidCall(
                    "no record is specified. you need to call before_first (and optionally move_next) first".to_string(),
                ))
            }
            Some(slot) => slot,
        };
        match self.layout.schema().info(field_name) {
            None => Err(TableScanError::InvalidCall("field not found".to_string())),
            Some(FieldInfo::Integer) => {
                let val = self.record_page.get_int(slot, field_name)?;
                Ok(Constant::Int(val))
            }
            Some(FieldInfo::String(_)) => {
                let val = self.record_page.get_string(slot, field_name)?;
                Ok(Constant::String(val))
            }
        }
    }

    pub fn get_int(&self, field_name: &str) -> Result<i32, TableScanError> {
        match self.get_val(field_name)? {
            Constant::Int(val) => Ok(val),
            _ => Err(TableScanError::InvalidCall(
                "field type mismatch".to_string(),
            )),
        }
    }

    pub fn get_string(&self, field_name: &str) -> Result<String, TableScanError> {
        match self.get_val(field_name)? {
            Constant::String(val) => Ok(val),
            _ => Err(TableScanError::InvalidCall(
                "field type mismatch".to_string(),
            )),
        }
    }

    pub fn set_val(&self, field_name: &str, val: &Constant) -> Result<(), TableScanError> {
        let slot = match self.current_slot {
            None => {
                return Err(TableScanError::InvalidCall(
                    "no record is specified. you need to call before_first/insert first"
                        .to_string(),
                ))
            }
            Some(slot) => slot,
        };
        match self.layout.schema().info(field_name) {
            None => Err(TableScanError::InvalidCall("field not found".to_string())),
            Some(FieldInfo::Integer) => {
                let val = match val {
                    Constant::Int(val) => *val,
                    _ => {
                        return Err(TableScanError::InvalidCall(
                            "field type mismatch".to_string(),
                        ))
                    }
                };
                self.record_page.set_int(slot, field_name, val)?;
                Ok(())
            }
            Some(FieldInfo::String(_)) => {
                let val = match val {
                    Constant::String(val) => val,
                    _ => {
                        return Err(TableScanError::InvalidCall(
                            "field type mismatch".to_string(),
                        ))
                    }
                };
                self.record_page.set_string(slot, field_name, val)?;
                Ok(())
            }
        }
    }

    pub fn set_int(&self, field_name: &str, val: i32) -> Result<(), TableScanError> {
        self.set_val(field_name, &Constant::Int(val))
    }

    pub fn set_string(&self, field_name: &str, val: &str) -> Result<(), TableScanError> {
        self.set_val(field_name, &Constant::String(val.to_string()))
    }

    // 新しい record を挿入するために、現在の slot 位置から移動を行う
    pub fn insert(&mut self) -> Result<(), TableScanError> {
        self.current_slot = self.record_page.insert_after(self.current_slot)?;
        while self.current_slot.is_none() {
            if self.is_at_last_block()? {
                self.move_to_new_block()?;
            } else {
                let next_block_num = self.record_page.block().number() + 1;
                let block = BlockId::new(&self.filename, next_block_num);
                self.move_to_block(&block)?;
            }
            self.current_slot = self.record_page.insert_after(None)?;
        }
        Ok(())
    }

    // 現在 cursor が指している record を削除する
    pub fn delete(&mut self) -> Result<(), TableScanError> {
        match self.current_slot {
            None => Err(TableScanError::InvalidCall(
                "no record is specified. you need to call before_first (and optionally move_next) first".to_string(),
            )),
            Some(slot) => {
                self.record_page.delete(slot)?;
                Ok(())
            }
        }
    }

    pub fn move_to_rid(&mut self, rid: &Rid) -> Result<(), TableScanError> {
        let block = BlockId::new(&self.filename, rid.block_number());
        self.record_page = RecordPage::new(self.tx.clone(), &block, &self.layout);
        self.current_slot = rid.slot();
        Ok(())
    }

    pub fn get_rid(&self) -> Rid {
        Rid::new(self.record_page.block().number(), self.current_slot)
    }

    pub fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }

    // table scan の cursor を先頭に移動する
    pub fn before_first(&mut self) -> Result<(), TableScanError> {
        let block = BlockId::new(&self.filename, 0);
        self.move_to_block(&block)?;
        Ok(())
    }

    fn move_to_block(&mut self, block: &BlockId) -> Result<(), TableScanError> {
        self.record_page = RecordPage::new(self.tx.clone(), &block, &self.layout);
        self.current_slot = None;
        Ok(())
    }

    fn move_to_new_block(&mut self) -> Result<(), TableScanError> {
        let block = self.tx.borrow_mut().append(&self.filename)?;
        self.move_to_block(&block)?;
        self.record_page.format()?;
        Ok(())
    }

    // table を走査していき、すでに最後の block まで到達していれば true を返す
    fn is_at_last_block(&self) -> Result<bool, TableScanError> {
        let block_num = self.record_page.block().number();
        Ok(block_num == self.tx.borrow_mut().size(&self.filename)? - 1)
    }
}

#[cfg(test)]
mod table_scan_test {
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use crate::query::constant::Constant;
    use crate::record::layout::Layout;
    use crate::record::table_scan::TableScan;
    use crate::tx::concurrency::lock_table::LockTable;
    use crate::tx::transaction::TransactionFactory;
    use crate::{
        buffer::buffer_manager::BufferManager,
        record::schema::{FieldInfo, Schema},
    };

    use std::cell::RefCell;
    use std::rc::Rc;
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
    fn test_scan_test() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        let tx = Rc::new(RefCell::new(factory.create().unwrap()));

        {
            let layout = setup_layout();
            let mut table_scan = TableScan::new(tx.clone(), "testtbl", &layout).unwrap();

            // 50 個の record を insert する
            table_scan.before_first().unwrap();
            for i in 0..50 {
                table_scan.insert().unwrap();
                table_scan.set_val("A", &Constant::Int(i)).unwrap();
                table_scan
                    .set_val("B", &Constant::String(format!("test{}", i)))
                    .unwrap();
            }

            // 偶数の整数値を持った record を削除する
            table_scan.before_first().unwrap();
            for i in 0..50 {
                table_scan.move_next().unwrap();
                let a = table_scan.get_val("A").unwrap();
                assert_eq!(a, Constant::Int(i));
                assert_eq!(
                    table_scan.get_val("B").unwrap(),
                    Constant::String(format!("test{}", i))
                );

                let a = a.as_int().unwrap();
                if a % 2 == 0 {
                    table_scan.delete().unwrap();
                }
            }

            // 奇数の整数値を持った record だけが残っていることを確認する
            table_scan.before_first().unwrap();
            let mut count = 0;
            while table_scan.move_next().unwrap() {
                let a = table_scan.get_val("A").unwrap().as_int().unwrap();
                // 上の操作で偶数値を持つ record は消えているはずなので、奇数値のみが残っている
                assert_eq!(a % 2, 1);

                count += 1;
            }
            assert_eq!(count, 25);
        }

        tx.borrow_mut().commit().unwrap();
    }
}

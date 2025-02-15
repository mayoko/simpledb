use crate::{
    file::blockid::BlockId,
    query::{
        constant::Constant,
        scan::{ReadScan, ReadScanError, UpdateScan, UpdateScanError},
    },
    tx::{
        buffer_list::BufferListError,
        transaction::{Transaction, TransactionSizeError},
    },
};

use super::{
    layout::Layout,
    record_page::{RecordPage, RecordPageError},
    rid::Rid,
    schema::FieldInfo,
};

use anyhow::{anyhow, Result as AnyhowResult};
use mockall::mock;
use thiserror::Error;

use std::{cell::RefCell, rc::Rc};

pub trait TableScan: ReadScan + UpdateScan {}

/**
 * table の record を取得・操作するための構造体
 *
 * move_next など、record の指し示す位置 (cursor) を移動させるメソッドを呼んだあとに get_val, insert などのメソッドを呼ぶことで table の record を操作できる
 *
 * フィールドの長さは固定長で、Unspanned (page をまたいで record を保存することがない) と仮定している
 */
pub struct TableScanImpl {
    // TableScanFactory に見せるために pub(crate) にしている
    pub(crate) tx: Rc<RefCell<Transaction>>,
    pub(crate) layout: Layout,
    // 現在の record が格納されている RecordPage
    pub(crate) record_page: RecordPage,
    pub(crate) filename: String,
    pub(crate) current_slot: Option<usize>,
}

#[derive(Error, Debug)]
pub(crate) enum TableScanError {
    #[error("buffer list error: {0}")]
    BufferList(#[from] BufferListError),
    #[error("transaction size error: {0}")]
    TransactionSize(#[from] TransactionSizeError),
    #[error("record page error: {0}")]
    RecordPage(#[from] RecordPageError),
}

impl TableScan for TableScanImpl {}

impl ReadScan for TableScanImpl {
    /// table scan の cursor を先頭に移動する
    fn before_first(&mut self) -> AnyhowResult<()> {
        let block = BlockId::new(&self.filename, 0);
        self.move_to_block(&block);
        Ok(())
    }

    /// record の存在する、次の slot に移動する。record が存在しない場合は false を返す
    fn move_next(&mut self) -> AnyhowResult<bool> {
        self.current_slot = self.record_page.next_after(self.current_slot)?;
        while self.current_slot.is_none() {
            if self.is_at_last_block()? {
                return Ok(false);
            }
            let next_block_num = self.record_page.block().number() + 1;
            let block = BlockId::new(&self.filename, next_block_num);
            self.move_to_block(&block);
            self.current_slot = self.record_page.next_after(None)?;
        }
        Ok(true)
    }

    // 今いる slot に対して、指定した field の値を取得する
    fn get_val(&self, field_name: &str) -> AnyhowResult<Constant> {
        let slot = match self.current_slot {
            None => {
                return Err(anyhow!(ReadScanError::InvalidCall(
                    "no record is specified for the table scan. you need to call before_first (and optionally move_next) first".to_string(),
                )))
            }
            Some(slot) => slot,
        };
        match self.layout.schema().info(field_name) {
            None => Err(anyhow!(ReadScanError::InvalidCall(
                "field not found for the table scan".to_string(),
            ))),
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

    fn has_field(&self, field_name: &str) -> bool {
        self.layout.schema().has_field(field_name)
    }
}

impl UpdateScan for TableScanImpl {
    fn set_val(&self, field_name: &str, val: &Constant) -> AnyhowResult<()> {
        let slot = match self.current_slot {
            None => Err(UpdateScanError::InvalidCall(
                "no record is specified for the table scan. you need to call before_first/insert first".to_string(),
            )),
            Some(slot) => Ok(slot),
        }?;
        Ok(match self.layout.schema().info(field_name) {
            None => Err(UpdateScanError::InvalidCall(format!(
                "field {} not found for the table scan",
                field_name
            ))),
            Some(FieldInfo::Integer) => {
                let val = match val {
                    Constant::Int(val) => Ok(*val),
                    _ => Err(UpdateScanError::InvalidCall(format!(
                        "field type mismatch (expected int): {}.",
                        field_name
                    ))),
                }?;
                self.record_page.set_int(slot, field_name, val)?;
                Ok(())
            }
            Some(FieldInfo::String(_)) => {
                let val = match val {
                    Constant::String(val) => Ok(val),
                    _ => Err(UpdateScanError::InvalidCall(format!(
                        "field type mismatch (expected string): {}.",
                        field_name
                    ))),
                }?;
                self.record_page.set_string(slot, field_name, val)?;
                Ok(())
            }
        }?)
    }

    // 新しい record を挿入するために、現在の slot 位置から移動を行う
    fn insert(&mut self) -> AnyhowResult<()> {
        self.current_slot = self.record_page.insert_after(self.current_slot)?;
        while self.current_slot.is_none() {
            if self.is_at_last_block()? {
                self.move_to_new_block()?;
            } else {
                let next_block_num = self.record_page.block().number() + 1;
                let block = BlockId::new(&self.filename, next_block_num);
                self.move_to_block(&block);
            }
            self.current_slot = self.record_page.insert_after(None)?;
        }
        Ok(())
    }

    // 現在 cursor が指している record を削除する
    fn delete(&mut self) -> AnyhowResult<()> {
        Ok(match self.current_slot {
            None => Err(UpdateScanError::InvalidCall(
                "no record is specified for the table scan. you need to call before_first (and optionally move_next) first".to_string(),
            )),
            Some(slot) => {
                self.record_page.delete(slot)?;
                Ok(())
            }
        }?)
    }

    fn move_to_rid(&mut self, rid: &Rid) -> AnyhowResult<()> {
        let block = BlockId::new(&self.filename, rid.block_number());
        self.record_page = RecordPage::new(self.tx.clone(), &block, &self.layout);
        self.current_slot = rid.slot();
        Ok(())
    }

    fn get_rid(&self) -> AnyhowResult<Rid> {
        Ok(Rid::new(
            self.record_page.block().number(),
            self.current_slot,
        ))
    }
}

impl TableScanImpl {
    fn move_to_block(&mut self, block: &BlockId) {
        self.record_page = RecordPage::new(self.tx.clone(), &block, &self.layout);
        self.current_slot = None;
    }

    fn move_to_new_block(&mut self) -> AnyhowResult<(), TableScanError> {
        let block = self.tx.borrow_mut().append(&self.filename)?;
        self.move_to_block(&block);
        self.record_page.format()?;
        Ok(())
    }

    // table を走査していき、すでに最後の block まで到達していれば true を返す
    fn is_at_last_block(&self) -> AnyhowResult<bool, TableScanError> {
        let block_num = self.record_page.block().number();
        Ok(block_num == self.tx.borrow_mut().size(&self.filename)? - 1)
    }
}

impl ReadScan for Box<TableScanImpl> {
    fn before_first(&mut self) -> AnyhowResult<()> {
        self.as_mut().before_first()
    }

    fn move_next(&mut self) -> AnyhowResult<bool> {
        self.as_mut().move_next()
    }

    fn get_val(&self, field_name: &str) -> AnyhowResult<Constant> {
        self.as_ref().get_val(field_name)
    }

    fn has_field(&self, field_name: &str) -> bool {
        self.as_ref().has_field(field_name)
    }
}

mock! {
    pub TableScan {}
    impl ReadScan for TableScan {
        fn before_first(&mut self) -> AnyhowResult<()>;
        fn move_next(&mut self) -> AnyhowResult<bool>;
        fn get_val(&self, field_name: &str) -> AnyhowResult<Constant>;
        fn get_int(&self, field_name: &str) -> AnyhowResult<i32>;
        fn get_string(&self, field_name: &str) -> AnyhowResult<String>;
        fn has_field(&self, field_name: &str) -> bool;
    }
    impl UpdateScan for TableScan {
        fn set_val(&self, field_name: &str, val: &Constant) -> AnyhowResult<()>;
        fn set_int(&self, field_name: &str, val: i32) -> AnyhowResult<()>;
        fn set_string(&self, field_name: &str, val: &str) -> AnyhowResult<()>;
        fn insert(&mut self) -> AnyhowResult<()>;
        fn delete(&mut self) -> AnyhowResult<()>;
        fn move_to_rid(&mut self, rid: &Rid) -> AnyhowResult<()>;
        fn get_rid(&self) -> AnyhowResult<Rid>;
    }
    impl TableScan for TableScan {}
}

#[cfg(test)]
mod table_scan_test {
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use crate::record::table_scan_factory::{TableScanFactory, TableScanFactoryImpl};
    use crate::tx::concurrency::lock_table::LockTable;
    use crate::tx::transaction::TransactionFactory;
    use crate::{
        buffer::buffer_manager::BufferManager,
        record::schema::{FieldInfo, Schema},
    };

    use super::*;

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
            let table_scan_factory = TableScanFactoryImpl::new();
            let mut table_scan = table_scan_factory
                .create(tx.clone(), "testtbl", &layout)
                .unwrap();

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

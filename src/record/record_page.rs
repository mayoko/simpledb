use crate::{
    file::blockid::BlockId,
    tx::transaction::{Transaction, TransactionGetError, TransactionSetError},
};

use thiserror::Error;

use super::layout::Layout;

use std::{cell::RefCell, rc::Rc};

/**
 * ある block の中で、layout に従った record を取得・操作するための構造体
 *
 * フィールドの長さは固定長で、Unspanned (page をまたいで record を保存することがない) と仮定している
 */
pub struct RecordPage {
    // record を取得する主体となっている transaction
    tx: Rc<RefCell<Transaction>>,
    // 参照している block
    block: BlockId,
    layout: Layout,
}

#[derive(Eq, PartialEq, Debug, Clone, Copy)]
pub enum RecordPageFlag {
    // この slot が使用されていないことを示す
    Empty = 0,
    // この slot が使用中であることを示す
    Used = 1,
}

#[derive(Error, Debug)]
pub(crate) enum RecordPageError {
    #[error("invalid call error: {0}")]
    InvalidCallError(String),
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("transaction get error: {0}")]
    TransactionGetError(#[from] TransactionGetError),
    #[error("transaction set error: {0}")]
    TransactionSetError(#[from] TransactionSetError),
}

impl Drop for RecordPage {
    fn drop(&mut self) {
        // new で pin した block を unpin する
        self.tx.borrow_mut().unpin(&self.block).unwrap();
    }
}

impl RecordPage {
    pub fn new(tx: Rc<RefCell<Transaction>>, block: &BlockId, layout: &Layout) -> Self {
        let record_page = RecordPage {
            tx,
            block: block.clone(),
            layout: layout.clone(),
        };
        record_page.tx.borrow_mut().pin(&block).unwrap();
        record_page
    }

    pub fn get_int(&self, slot: usize, field_name: &str) -> Result<i32, RecordPageError> {
        let offset = self.offset(slot, field_name)?;
        Ok(self.tx.borrow_mut().get_int(&self.block, offset)?)
    }

    pub fn get_string(&self, slot: usize, field_name: &str) -> Result<String, RecordPageError> {
        let offset = self.offset(slot, field_name)?;
        Ok(self.tx.borrow_mut().get_string(&self.block, offset)?)
    }

    pub fn set_int(&self, slot: usize, field_name: &str, val: i32) -> Result<(), RecordPageError> {
        let offset = self.offset(slot, field_name)?;
        self.tx
            .borrow_mut()
            .set_int(&self.block, offset, val, true)?;
        Ok(())
    }

    pub fn set_string(
        &self,
        slot: usize,
        field_name: &str,
        val: &str,
    ) -> Result<(), RecordPageError> {
        let offset = self.offset(slot, field_name)?;
        self.tx
            .borrow_mut()
            .set_string(&self.block, offset, val, true)?;
        Ok(())
    }

    pub fn delete(&mut self, slot: usize) -> Result<(), RecordPageError> {
        self.set_flag(slot, RecordPageFlag::Empty)?;
        Ok(())
    }

    // block の状態を初期化する。ここで施した変更は log には保存しない
    pub fn format(&self) -> Result<(), RecordPageError> {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            self.tx.borrow_mut().set_int(
                &self.block,
                self.root_offset(slot),
                RecordPageFlag::Empty as i32,
                true,
            )?;
            let schema = self.layout.schema();
            for field in schema.fields() {
                let offset = self.offset(slot, &field)?;
                match schema.info(&field) {
                    Some(crate::record::schema::FieldInfo::Integer) => {
                        self.tx.borrow_mut().set_int(&self.block, offset, 0, false)?;
                    }
                    Some(crate::record::schema::FieldInfo::String(_)) => {
                        self.tx.borrow_mut().set_string(&self.block, offset, "", false)?;
                    }
                    None => return Err(RecordPageError::InvalidCallError(
                        "field not found. It might be because the layout configuration was not correct."
                            .to_string(),
                    )),
                }
            }
            slot += 1;
        }
        Ok(())
    }

    // 現在いる block の中で、ちゃんとした値が入っている次の slot を探す (入力に与えた slot は含まない)
    // ファイルの一番最初から探したい場合、slot に None を与える
    pub fn next_after(&mut self, slot: Option<usize>) -> Result<Option<usize>, RecordPageError> {
        self.search_after(slot, RecordPageFlag::Used)
    }

    // 現在いる block の中で、空いていて insert に使うことのできる次の slot を探す (入力に与えた slot は含まない)
    // ファイルの一番最初から探したい場合、slot に None を与える
    // 見つかった場合、その slot を Used に変更して slot 番号を返す
    pub fn insert_after(&mut self, slot: Option<usize>) -> Result<Option<usize>, RecordPageError> {
        let slot = self.search_after(slot, RecordPageFlag::Empty)?;
        match slot {
            Some(slot) => {
                self.set_flag(slot, RecordPageFlag::Used)?;
                Ok(Some(slot))
            }
            None => Ok(None),
        }
    }

    pub fn block(&self) -> &BlockId {
        &self.block
    }

    fn search_after(
        &mut self,
        slot: Option<usize>,
        target_flag: RecordPageFlag,
    ) -> Result<Option<usize>, RecordPageError> {
        let mut next_slot = match slot {
            Some(slot) => slot + 1,
            None => 0,
        };
        while self.is_valid_slot(next_slot) {
            let flag = self
                .tx
                .borrow_mut()
                .get_int(&self.block, self.root_offset(next_slot))?;
            let flag = RecordPageFlag::from_i32(flag).ok_or(RecordPageError::InternalError(
                format!("invalid flag found. slot: {}, flag: {}", next_slot, flag),
            ))?;
            if flag == target_flag {
                return Ok(Some(next_slot));
            }
            next_slot += 1;
        }
        Ok(None)
    }

    fn set_flag(&mut self, slot: usize, flag: RecordPageFlag) -> Result<(), RecordPageError> {
        let offset = slot * self.layout.slot_size();
        self.tx
            .borrow_mut()
            .set_int(&self.block, offset, flag as i32, true)?;
        Ok(())
    }

    fn offset(&self, slot: usize, field_name: &str) -> Result<usize, RecordPageError> {
        Ok(slot * self.layout.slot_size()
            + self
                .layout
                .offset(field_name)
                .ok_or(RecordPageError::InvalidCallError(
                    "field not found".to_string(),
                ))?)
    }

    fn root_offset(&self, slot: usize) -> usize {
        slot * self.layout.slot_size()
    }

    fn is_valid_slot(&self, slot: usize) -> bool {
        let block_size = self.tx.borrow_mut().block_size();
        return self.root_offset(slot + 1) < block_size;
    }
}

impl RecordPageFlag {
    pub fn from_i32(n: i32) -> Option<RecordPageFlag> {
        match n {
            0 => Some(RecordPageFlag::Empty),
            1 => Some(RecordPageFlag::Used),
            _ => None,
        }
    }
}

#[cfg(test)]
mod record_page_test {
    use crate::file::blockid::BlockId;
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use crate::tx::concurrency::lock_table::LockTable;
    use crate::tx::transaction::TransactionFactory;
    use crate::{
        buffer::buffer_manager::BufferManager,
        record::record_page::Layout,
        record::schema::{FieldInfo, Schema},
    };

    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::Arc;

    use tempfile::{tempdir, TempDir};

    use super::RecordPage;

    fn setup_factory(dir: &TempDir) -> TransactionFactory {
        let file_manager = Arc::new(FileManager::new(dir.path(), 800));
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
    fn test_record_page() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        let tx = Rc::new(RefCell::new(factory.create().unwrap()));
        let block = BlockId::new("testfile", 0);

        {
            let layout = setup_layout();
            let mut record_page = RecordPage::new(tx.clone(), &block, &layout);

            // format する
            assert!(record_page.format().is_ok());

            // insert していく
            let mut slot = record_page.insert_after(None).unwrap();
            while let Some(s) = slot {
                assert!(record_page.set_int(s, "A", s as i32).is_ok());
                assert!(record_page.set_string(s, "B", &format!("rec{}", s)).is_ok());
                slot = record_page.insert_after(slot).unwrap();
            }
            // record を順に読んでいき、10 以下の値を取る record は削除する。ついでに、insert した reocord の数を数えておく
            let mut count = 0;
            let mut slot = record_page.next_after(None).unwrap();
            while let Some(s) = slot {
                count += 1;
                let a = record_page.get_int(s, "A").unwrap();
                let b = record_page.get_string(s, "B").unwrap();
                if a <= 10 {
                    assert!(record_page.delete(s).is_ok());
                } else {
                    assert_eq!(a, s as i32);
                    assert_eq!(b, format!("rec{}", s));
                }
                slot = record_page.next_after(slot).unwrap();
            }
            // slot size = 48 (= 4 (flag) + 4 (integer) + (4 + 4 * 9) (string)) なので、block_size = 800 のもとでは 800/48 = 16 までしか record を保存しない
            assert_eq!(count, 16);
            let mut slot = record_page.next_after(None).unwrap();
            let mut count = 0;
            while let Some(_) = slot {
                count += 1;
                slot = record_page.next_after(slot).unwrap();
            }
            // 11 ~ 15 の 5 つ
            assert_eq!(count, 5);
        }

        // drop で unpin されているので、再び unpin しようとすると error になる
        assert!(tx.borrow_mut().unpin(&block).is_err());
        tx.borrow_mut().commit().unwrap();
    }
}

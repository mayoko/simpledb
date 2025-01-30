use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use thiserror::Error;

use super::buffer_list::{self, BufferList, BufferListError};
use super::concurrency::lock_table::{LockTable, LockTableError};
use super::log::log_record_iterator::{LogRecordIterator, LogRecordReverseIterator};
use super::log::record::log_record::{LogRecord, LogRecordError, LogReplayError};
use crate::buffer::buffer_manager::{BufferManager, BufferManagerError};
use crate::file::file_manager::FileManagerError;
use crate::file::{blockid::BlockId, file_manager::FileManager};
use crate::log::log_manager::{LogError, LogManager};
use crate::tx::concurrency::concurrency_manager::ConcurrencyManager;
use crate::tx::log::log_record_writer::LogRecordWriter;

/**
 * db を操作するひとまとまりの処理単位である transaction を表すクラス
 *
 * このクラスを利用する場合、まず pin で変更したい block を保持し、その後保持した block に対して操作を行った後に、commit または rollback を行う
 * write ahead logging (WAL) を実現するため、このクラスは log_manager と buffer manager の呼び出し順を注意深く制御している
 *
 * このクラスのインスタンスはプログラム中に何個あっても良い
 */
pub struct Transaction {
    concurrency_manager: ConcurrencyManager,
    log_record_writer: LogRecordWriter,
    log_manager: Arc<LogManager>,
    buffer_manager: Arc<BufferManager>,
    file_manager: Arc<FileManager>,
    txnum: u32,
    buffer_list: BufferList,
}

/**
 * Transaction の生成を行うクラス
 *
 * Transaction は必ずこのクラスを通して作成する
 * このクラスのインスタンスはプログラム中に一つだけある想定 (next_txnum の管理をする必要があるため)
 */
pub struct TransactionFactory {
    // トランザクションの ID を生成するためのシーケンス
    next_txnum: Mutex<u32>,
    file_manager: Arc<FileManager>,
    log_manager: Arc<LogManager>,
    buffer_manager: Arc<BufferManager>,
    lock_table: Arc<LockTable>,
}

#[derive(Error, Debug)]
pub enum TransactionCommitError {
    #[error("Lock table error: {0}")]
    LockTableError(#[from] LockTableError),
    #[error("Log record error: {0}")]
    LogRecordError(#[from] LogRecordError),
    #[error("Buffer list error: {0}")]
    BufferListError(#[from] BufferListError),
}

#[derive(Error, Debug)]
pub enum TransactionRollbackError {
    #[error("Lock table error: {0}")]
    LockTableError(#[from] LockTableError),
    #[error("Log record error: {0}")]
    LogRecordError(#[from] LogRecordError),
    #[error("Buffer list error: {0}")]
    BufferListError(#[from] BufferListError),
    #[error("Log error: {0}")]
    LogError(#[from] LogError),
    #[error("log replay error: {0}")]
    LogReplayError(#[from] LogReplayError),
}

#[derive(Error, Debug)]
pub enum TransactionRecoverError {
    #[error("Lock table error: {0}")]
    LockTableError(#[from] LockTableError),
    #[error("Log record error: {0}")]
    LogRecordError(#[from] LogRecordError),
    #[error("Log error: {0}")]
    LogError(#[from] LogError),
    #[error("log replay error: {0}")]
    LogReplayError(#[from] LogReplayError),
    #[error("buffer manager error: {0}")]
    BufferManagerError(#[from] BufferManagerError),
    #[error("file manager error: {0}")]
    FileManagerError(#[from] FileManagerError),
}

#[derive(Error, Debug)]
pub enum TransactionGetError {
    #[error("Lock table error: {0}")]
    LockTableError(#[from] LockTableError),
    #[error("Buffer list error: {0}")]
    BufferListError(#[from] BufferListError),
    #[error("lock error: {0}")]
    LockError(String),
    #[error("invalid method call error: {0}")]
    InvalidMethodCallError(String),
    #[error("from utf8 error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
}

#[derive(Error, Debug)]
pub enum TransactionSetError {
    #[error("Lock table error: {0}")]
    LockTableError(#[from] LockTableError),
    #[error("Log record error: {0}")]
    LogRecordError(#[from] LogRecordError),
    #[error("lock error: {0}")]
    LockError(String),
    #[error("invalid method call error: {0}")]
    InvalidMethodCallError(String),
}

#[derive(Error, Debug)]
pub enum TransactionSizeError {
    #[error("Lock table error: {0}")]
    LockTableError(#[from] LockTableError),
    #[error("file manager error: {0}")]
    FileManagerError(#[from] FileManagerError),
}

impl Transaction {
    // WAL のルールに則って transaction の内容を commit する
    pub fn commit(&mut self) -> Result<(), TransactionCommitError> {
        self.log_record_writer.log_commit(self.txnum)?;
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all()?;

        Ok(())
    }

    // WAL のルールに則って transaction の内容を rollback する
    pub fn rollback(&mut self) -> Result<(), TransactionRollbackError> {
        self.log_record_writer.log_rollback(self.txnum)?;
        self.do_rollback()?;
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all()?;

        Ok(())
    }

    // 現在までの log の内容をもとに、database の状態を復元する
    // Note: このメソッドを呼び出す場合、他の transaction は走っていないことが前提とされている。db の立ち上げのときなどに呼び出すのが良い
    pub fn recover(&mut self) -> Result<(), TransactionRecoverError> {
        self.do_recover()?;
        self.concurrency_manager.release()?;
        // recover では log に書き込む前に buffer manager を flush する
        self.buffer_manager.flush_all()?;
        let lsn = self.log_record_writer.log_check_point()?;
        self.log_manager.flush(lsn)?;
        Ok(())
    }

    // block の読み書きをするために必要な準備である、pin を行う
    pub fn pin(&mut self, block: &BlockId) -> Result<(), BufferListError> {
        self.buffer_list.pin(block)?;
        Ok(())
    }

    // block の読み書き終了後、不要になった block の pin を解除する
    // Note: pin することと lock を取ることは独立に行えるので、lock を取っている状態であっても pin を解除することができる
    //       unpin して内容が flush されたとしても、lock を取り続けていれば uncommitted read は起きないし、
    //       flush された内容が log に書き出されても commit log が入っていない限りは recovery で元に戻される
    // Note: pin を呼び出した回数分だけ unpin する必要がある
    pub fn unpin(&mut self, block: &BlockId) -> Result<(), BufferListError> {
        self.buffer_list.unpin(block)?;
        Ok(())
    }

    pub fn get_int(&mut self, block: &BlockId, offset: usize) -> Result<i32, TransactionGetError> {
        self.concurrency_manager.slock(block)?;
        let buffer = self.buffer_list.get_buffer(block).ok_or_else(|| {
            TransactionGetError::InvalidMethodCallError(
                "buffer must be pinned first to read the value".to_string(),
            )
        })?;
        let buffer = buffer.lock().or_else(|_| {
            Err(TransactionGetError::LockError(
                "Failed to lock buffer".to_string(),
            ))
        })?;
        let page = buffer.contents();
        Ok(page.get_int(offset))
    }

    pub fn get_string(
        &mut self,
        block: &BlockId,
        offset: usize,
    ) -> Result<String, TransactionGetError> {
        self.concurrency_manager.slock(block)?;
        let buffer = self.buffer_list.get_buffer(block).ok_or_else(|| {
            TransactionGetError::InvalidMethodCallError(
                "buffer must be pinned first to read the value".to_string(),
            )
        })?;
        let buffer = buffer.lock().or_else(|_| {
            Err(TransactionGetError::LockError(
                "Failed to lock buffer".to_string(),
            ))
        })?;
        let page = buffer.contents();
        Ok(page.get_string(offset)?)
    }

    pub fn set_int(
        &mut self,
        block: &BlockId,
        offset: usize,
        val: i32,
        is_ok_to_log: bool,
    ) -> Result<(), TransactionSetError> {
        self.concurrency_manager.xlock(block)?;
        let buffer = self.buffer_list.get_buffer(block).ok_or_else(|| {
            TransactionSetError::InvalidMethodCallError(
                "buffer must be pinned first to set the value".to_string(),
            )
        })?;
        let mut buffer = buffer.lock().or_else(|_| {
            Err(TransactionSetError::LockError(
                "Failed to lock buffer".to_string(),
            ))
        })?;
        let lsn = if is_ok_to_log {
            let lsn = self
                .log_record_writer
                .log_set_int(self.txnum, &buffer, offset, val)?;
            Some(lsn)
        } else {
            None
        };

        let page = buffer.contents_mut();
        page.set_int(offset, val);
        buffer.set_modified(self.txnum as u64, lsn);

        Ok(())
    }

    pub fn set_string(
        &mut self,
        block: &BlockId,
        offset: usize,
        val: &str,
        is_ok_to_log: bool,
    ) -> Result<(), TransactionSetError> {
        self.concurrency_manager.xlock(block)?;
        let buffer = self.buffer_list.get_buffer(block).ok_or_else(|| {
            TransactionSetError::InvalidMethodCallError(
                "buffer must be pinned first to set the value".to_string(),
            )
        })?;
        let mut buffer = buffer.lock().or_else(|_| {
            Err(TransactionSetError::LockError(
                "Failed to lock buffer".to_string(),
            ))
        })?;
        let lsn = if is_ok_to_log {
            let lsn = self
                .log_record_writer
                .log_set_string(self.txnum, &buffer, offset, val)?;
            Some(lsn)
        } else {
            None
        };

        let page = buffer.contents_mut();
        page.set_string(offset, val);
        buffer.set_modified(self.txnum as u64, lsn);

        Ok(())
    }

    pub fn size(&mut self, filename: &str) -> Result<usize, TransactionSizeError> {
        let block = BlockId::new_end_of_file(filename);
        self.concurrency_manager.slock(&block)?;
        Ok(self.file_manager.length(filename)?)
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId, TransactionSizeError> {
        let block = BlockId::new_end_of_file(filename);
        self.concurrency_manager.xlock(&block)?;
        let new_block = self.file_manager.append(filename)?;
        Ok(new_block)
    }

    pub fn block_size(&self) -> usize {
        self.file_manager.block_size()
    }

    pub fn available_buffers(&self) -> Result<usize, BufferManagerError> {
        self.buffer_manager.available()
    }

    fn do_rollback(&mut self) -> Result<(), TransactionRollbackError> {
        // commit 済のトランザクションのリスト
        let mut iter = LogRecordIterator::new(self.log_manager.clone())?;
        while let Some(log_record) = iter.next() {
            match log_record {
                LogRecord::Start(inner) => {
                    if inner.tx_num() == self.txnum {
                        break;
                    }
                }
                LogRecord::SetStringRecord(record) => {
                    if record.tx_num() == self.txnum {
                        record.undo(self)?;
                    }
                }
                LogRecord::SetIntRecord(record) => {
                    if record.tx_num() == self.txnum {
                        record.undo(self)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /**
     * undo-redo recovery を行う
     */
    fn do_recover(&mut self) -> Result<(), TransactionRecoverError> {
        // undo stage

        // commit 済のトランザクションのリスト
        let mut committed_txs: HashSet<u32> = HashSet::new();
        let mut iter = LogRecordIterator::new(self.log_manager.clone())?;
        while let Some(log_record) = iter.next() {
            match log_record {
                LogRecord::CheckPoint() => {
                    // redo stage へ移行
                    break;
                }
                LogRecord::SetStringRecord(record) => {
                    if !committed_txs.contains(&record.tx_num()) {
                        record.undo(self)?;
                    }
                }
                LogRecord::SetIntRecord(record) => {
                    if !committed_txs.contains(&record.tx_num()) {
                        record.undo(self)?;
                    }
                }
                LogRecord::Commit(inner) => {
                    committed_txs.insert(inner.tx_num());
                }
                _ => {}
            }
        }

        // redo stage
        let mut rev_iter = LogRecordReverseIterator::new(&iter)?;
        while let Some(log_record) = rev_iter.next() {
            // commit された変更を再適用する
            match log_record {
                LogRecord::SetStringRecord(record) => {
                    if committed_txs.contains(&record.tx_num()) {
                        record.redo(self)?;
                    }
                }
                LogRecord::SetIntRecord(record) => {
                    if committed_txs.contains(&record.tx_num()) {
                        record.redo(self)?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

impl TransactionFactory {
    pub fn new(
        file_manager: Arc<FileManager>,
        log_manager: Arc<LogManager>,
        buffer_manager: Arc<BufferManager>,
        lock_table: Arc<LockTable>,
    ) -> TransactionFactory {
        TransactionFactory {
            file_manager,
            log_manager,
            buffer_manager,
            lock_table,
            next_txnum: Mutex::new(0),
        }
    }

    pub fn create(&self) -> Result<Transaction, LogRecordError> {
        let mut txnum = self.next_txnum.lock().unwrap();
        *txnum += 1;
        let log_record_writer = LogRecordWriter::new(self.log_manager.clone());
        log_record_writer.log_start(*txnum)?;
        Ok(Transaction {
            concurrency_manager: ConcurrencyManager::new(self.lock_table.clone()),
            log_record_writer,
            buffer_list: buffer_list::BufferList::new(self.buffer_manager.clone()),
            log_manager: self.log_manager.clone(),
            buffer_manager: self.buffer_manager.clone(),
            file_manager: self.file_manager.clone(),
            txnum: *txnum,
        })
    }
}

#[cfg(test)]
mod transaction_test {
    use std::sync::Arc;
    use tempfile::{tempdir, TempDir};

    use super::*;

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
    fn test_transaction_in_general() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        // tx1: block に 80: 1, 40: "one" と書き込み
        let mut tx1 = factory.create().unwrap();
        let block = BlockId::new("testfile", 0);
        tx1.pin(&block).unwrap();

        tx1.set_int(&block, 80, 1, false).unwrap();
        tx1.set_string(&block, 40, "one", false).unwrap();
        tx1.commit().unwrap();

        // tx2: block の値を読み込んだあと、その値を変更し commit
        let mut tx2 = factory.create().unwrap();
        tx2.pin(&block).unwrap();
        let ival = tx2.get_int(&block, 80).unwrap();
        let sval = tx2.get_string(&block, 40).unwrap();
        assert_eq!(ival, 1);
        assert_eq!(sval, "one");
        tx2.set_int(&block, 80, ival + 1, true).unwrap();
        tx2.set_string(&block, 40, &format!("{}!", sval), true)
            .unwrap();
        tx2.commit().unwrap();

        // tx3: block の値を読み込んだあと、値を変更し rollback
        let mut tx3 = factory.create().unwrap();
        tx3.pin(&block).unwrap();
        let ival = tx3.get_int(&block, 80).unwrap();
        let sval = tx3.get_string(&block, 40).unwrap();
        assert_eq!(ival, 2);
        assert_eq!(sval, "one!");
        tx3.set_int(&block, 80, 9999, true).unwrap();
        tx3.rollback().unwrap();

        // tx4: block の値を読み込み、rollback の値が反映されていないことを確認
        let mut tx4 = factory.create().unwrap();
        tx4.pin(&block).unwrap();
        let ival = tx4.get_int(&block, 80).unwrap();
        let sval = tx4.get_string(&block, 40).unwrap();
        assert_eq!(ival, 2);
        assert_eq!(sval, "one!");
        tx4.commit().unwrap();
    }

    #[test]
    fn test_lock_behavior() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        let mut tx1 = factory.create().unwrap();
        let mut tx2 = factory.create().unwrap();
        let block = BlockId::new("testfile", 0);

        tx1.pin(&block).unwrap();
        // pin 自体はすべての transaction で可能
        assert!(tx2.pin(&block).is_ok());

        tx1.set_int(&block, 80, 1, true).unwrap();
        // tx1 が xlock しているので、tx2 は slock も xlock もできない
        assert!(tx2.get_int(&block, 80).is_err());
        assert!(tx2.set_int(&block, 80, 2, true).is_err());

        tx1.unpin(&block).unwrap();
        // unpin しても lock は残るので、tx2 はやはり slock も xlock もできない
        assert!(tx2.get_int(&block, 80).is_err());
        assert!(tx2.set_int(&block, 80, 2, true).is_err());

        tx1.commit().unwrap();
        // tx1 が commit したので、lock が解放され、tx2 は slock ができるようになる
        assert!(tx2.get_int(&block, 80).is_ok());
        assert!(tx2.set_int(&block, 80, 2, true).is_ok());
        tx2.commit().unwrap();
    }

    #[test]
    fn test_recover() {
        let dir = tempdir().unwrap();
        let factory = setup_factory(&dir);

        // arrange: まず tx1 で commit, tx2 で rollback, tx3 で commit も rollback もされていない変更を作成
        let mut tx1 = factory.create().unwrap();
        let block = BlockId::new("testfile", 0);

        tx1.pin(&block).unwrap();
        tx1.set_int(&block, 80, 1, true).unwrap();
        tx1.set_string(&block, 40, "one", true).unwrap();
        tx1.commit().unwrap();

        let mut tx2 = factory.create().unwrap();
        tx2.pin(&block).unwrap();
        tx2.set_int(&block, 80, 2, true).unwrap();
        tx2.set_string(&block, 40, "two", true).unwrap();
        tx2.rollback().unwrap();

        let mut tx3 = factory.create().unwrap();
        tx3.pin(&block).unwrap();
        tx3.set_int(&block, 80, 3, true).unwrap();
        tx3.set_string(&block, 40, "three", true).unwrap();
        // tx3 の途中で crash した状況を再現するため、lock 解放 -> unpin を自前で行う
        tx3.concurrency_manager.release().unwrap();
        tx3.buffer_list.unpin_all().unwrap();

        // act: recover を行う
        let mut tx4 = factory.create().unwrap();
        tx4.recover().unwrap();

        // assert: tx1, tx2 は commit された変更が復元されている
        let mut tx5 = factory.create().unwrap();
        tx5.pin(&block).unwrap();
        assert_eq!(tx5.get_int(&block, 80).unwrap(), 1);
        assert_eq!(tx5.get_string(&block, 40).unwrap(), "one");
    }
}

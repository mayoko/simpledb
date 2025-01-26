use crate::file::blockid;
use crate::file::file_manager;
use crate::file::page;

use crate::log::log_iterator;
use std::{
    io,
    sync::{Arc, Mutex},
};
use thiserror::Error;

/**
 * recovery や rollback を使う際に用いる、db の log record を書き込むためのクラス
 * このクラスでは、それぞれの log は単なる byte 列として扱われる
 *
 * このクラスのインスタンスはプログラム中に一つだけ存在する
 */
pub struct LogManager {
    fm: Arc<file_manager::FileManager>,
    logfile: String,
    log_page: Mutex<page::Page>,
    current_block: Mutex<blockid::BlockId>,
    latest_lsn: Mutex<u64>, // LSN = log sequence number
    last_saved_lsn: Mutex<u64>,
}

#[derive(Error, Debug)]
pub enum LogError {
    #[error("Failed to acquire write lock")]
    LockError,
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    #[error("Error from file manager: {0}")]
    FileManagerError(#[from] file_manager::FileManagerError),
}

impl LogManager {
    pub fn new(fm: Arc<file_manager::FileManager>, logfile: &str) -> Result<LogManager, LogError> {
        let block_size = fm.block_size();
        let mut log_page = page::Page::new_from_size(block_size);

        let log_size = fm.length(logfile)?;
        let current_block = if log_size == 0 {
            append_new_block(&fm, &mut log_page, logfile)?
        } else {
            let current_block = blockid::BlockId::new(logfile, log_size - 1);
            fm.read(&current_block, &mut log_page)?;
            current_block
        };

        let latest_lsn = 0;
        let last_saved_lsn = 0;
        Ok(LogManager {
            fm: fm,
            logfile: logfile.to_string(),
            log_page: Mutex::new(log_page),
            current_block: Mutex::new(current_block),
            latest_lsn: Mutex::new(latest_lsn),
            last_saved_lsn: Mutex::new(last_saved_lsn),
        })
    }

    /**
     * byte 列としての log record を追加する。追加に成功した場合、追加された log record の log sequential number を返す
     *
     * この method では log record が block に書き込まれることは保証されない。書き込みまでを保証したい場合は、flush もしくは flush_all を呼ぶ必要がある
     */
    pub fn append(&self, logrec: &[u8]) -> Result<u64, LogError> {
        // boundary 取得
        let mut boundary = {
            let log_page = self.log_page.lock().map_err(|_| LogError::LockError)?;
            log_page.get_int(0) as usize
        };

        // 今の block に書き込めなさそうなら新しい block を作る
        let integer_bytes = 4;
        let bytes_needed = logrec.len() + integer_bytes;
        if boundary < integer_bytes + bytes_needed {
            self.flush_all()?;
            let mut log_page = self.log_page.lock().map_err(|_| LogError::LockError)?;
            let mut current_block = self.current_block.lock().map_err(|_| LogError::LockError)?;
            *current_block = append_new_block(&self.fm, &mut log_page, &self.logfile)?;
            boundary = log_page.get_int(0) as usize;
        }

        // logrec を書き込む
        let rec_pos = boundary - bytes_needed;
        let mut log_page = self.log_page.lock().map_err(|_| LogError::LockError)?;
        log_page.set_bytes(rec_pos, logrec);
        log_page.set_int(0, rec_pos as i32);

        // lsn の更新
        let mut latest_lsn = self.latest_lsn.lock().map_err(|_| LogError::LockError)?;
        *latest_lsn += 1;

        Ok(*latest_lsn)
    }

    /**
     * log record を最新順から読むための iterator を返す
     */
    pub fn iterator(&self) -> Result<log_iterator::LogIterator, LogError> {
        self.flush_all()?;
        let current_block = self.current_block.lock().map_err(|_| LogError::LockError)?;
        Ok(log_iterator::LogIterator::new(
            self.fm.clone(),
            &current_block,
        )?)
    }

    /**
     * 少なくとも lsn までの log record を block に書き込んで、永続性を保証する
     */
    pub fn flush(&self, lsn: u64) -> Result<(), LogError> {
        // flush_all でも last_saved_lsn が参照されるので、ここで scope を閉じて drop する必要がある
        {
            let last_saved_lsn = self
                .last_saved_lsn
                .lock()
                .map_err(|_| LogError::LockError)?;
            if lsn <= *last_saved_lsn {
                return Ok(());
            }
        }
        self.flush_all()?;
        Ok(())
    }

    /**
     * すべての log record を block に書き込んで、永続性を保証する
     */
    fn flush_all(&self) -> Result<(), LogError> {
        let mut log_page = self.log_page.lock().map_err(|_| LogError::LockError)?;
        let current_block = self.current_block.lock().map_err(|_| LogError::LockError)?;
        self.fm.write(&current_block, &mut log_page)?;

        let mut last_saved_lsn = self
            .last_saved_lsn
            .lock()
            .map_err(|_| LogError::LockError)?;
        *last_saved_lsn = self
            .latest_lsn
            .lock()
            .map_err(|_| LogError::LockError)?
            .clone();
        Ok(())
    }
}

fn append_new_block(
    fm: &file_manager::FileManager,
    page: &mut page::Page,
    logfile: &str,
) -> Result<blockid::BlockId, LogError> {
    let new_block = fm.append(logfile)?;

    let block_size = fm.block_size();
    page.set_int(0, block_size as i32);
    fm.write(&new_block, page)?;

    Ok(new_block)
}

#[cfg(test)]
mod test_log_manager {
    use super::*;

    #[test]
    fn test_log_manager() {
        let dir = tempfile::tempdir().unwrap();
        let fm = file_manager::FileManager::new(dir.path(), 400);
        let log_manager = LogManager::new(Arc::new(fm), "log_file").unwrap();

        // log_record がない場合
        let mut log_iter = log_manager.iterator().unwrap();
        assert_eq!(log_iter.next(), None);

        // 1 つ目の log_record を追加
        let log_record = b"test log record";
        let lsn = log_manager.append(log_record).unwrap();
        assert_eq!(lsn, 1);

        let mut log_iter = log_manager.iterator().unwrap();
        assert_eq!(log_iter.next(), Some(log_record.to_vec()));

        // 2 つ目の log_record を追加: 最新のものから順に読む
        let next_log_record = b"next log record";
        let lsn = log_manager.append(next_log_record).unwrap();
        assert_eq!(lsn, 2);

        let mut log_iter = log_manager.iterator().unwrap();
        assert_eq!(log_iter.next(), Some(next_log_record.to_vec()));
        assert_eq!(log_iter.next(), Some(log_record.to_vec()));

        let mut log_rev_iter = log_iterator::LogReverseIterator::new(&log_iter).unwrap();
        assert_eq!(log_rev_iter.next(), Some(log_record.to_vec()));
        assert_eq!(log_rev_iter.next(), Some(next_log_record.to_vec()));
    }

    #[test]
    fn test_many_logs() {
        let dir = tempfile::tempdir().unwrap();
        let fm = file_manager::FileManager::new(dir.path(), 400);
        let log_manager = LogManager::new(Arc::new(fm), "log_file").unwrap();

        for i in 0..100 {
            let log_record_str = format!("test log record {}", i);
            let log_record = log_record_str.as_bytes();
            let lsn = log_manager.append(log_record).unwrap();
            assert_eq!(lsn, i as u64 + 1);
        }

        let mut log_iter = log_manager.iterator().unwrap();
        for i in (0..100).rev() {
            let log_record_str = format!("test log record {}", i);
            let log_record = log_record_str.as_bytes();
            assert_eq!(log_iter.next(), Some(log_record.to_vec()));
        }

        let mut log_rev_iter = log_iterator::LogReverseIterator::new(&log_iter).unwrap();
        for i in 0..100 {
            let log_record_str = format!("test log record {}", i);
            let log_record = log_record_str.as_bytes();
            assert_eq!(log_rev_iter.next(), Some(log_record.to_vec()));
        }
    }
}

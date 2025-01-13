use crate::file::blockid;
use crate::file::file_manager;
use crate::file::page;

use crate::log::log_iterator;
use std::io;

pub struct LogManager<'a> {
    fm: &'a file_manager::FileManager,
    logfile: String,
    log_page: page::Page,
    current_block: blockid::BlockId,
    latest_lsn: u64, // LSN = log sequence number
    last_saved_lsn: u64,
}

impl<'a> LogManager<'a> {
    pub fn new(
        fm: &'a file_manager::FileManager,
        logfile: &str,
    ) -> Result<LogManager<'a>, io::Error> {
        let block_size = fm.block_size();
        let mut log_page = page::Page::new_from_size(block_size);

        let log_size = fm.length(logfile)?;
        let current_block = if log_size == 0 {
            append_new_block(fm, &mut log_page, logfile)?
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
            log_page: log_page,
            current_block: current_block,
            latest_lsn: latest_lsn,
            last_saved_lsn: last_saved_lsn,
        })
    }

    pub fn append(&mut self, logrec: &[u8]) -> Result<u64, io::Error> {
        let mut boundary = self.log_page.get_int(0) as usize;
        let integer_bytes = 4;
        let bytes_needed = logrec.len() + integer_bytes;
        if boundary < integer_bytes + bytes_needed {
            self.flush_all()?;
            self.current_block = append_new_block(self.fm, &mut self.log_page, &self.logfile)?;
            boundary = self.log_page.get_int(0) as usize;
        }
        let rec_pos = boundary - bytes_needed;
        self.log_page.set_bytes(rec_pos, logrec);
        self.log_page.set_int(0, rec_pos as i32);
        self.latest_lsn += 1;

        Ok(self.latest_lsn)
    }

    pub fn iterator(&mut self) -> Result<log_iterator::LogIterator, io::Error> {
        self.flush_all()?;
        log_iterator::LogIterator::new(self.fm, &self.current_block)
    }

    pub fn flush(&mut self, lsn: u64) -> Result<(), io::Error> {
        if lsn > self.last_saved_lsn {
            self.flush_all()?;
        }
        Ok(())
    }

    fn flush_all(&mut self) -> Result<(), io::Error> {
        self.fm.write(&self.current_block, &mut self.log_page)?;
        self.last_saved_lsn = self.latest_lsn;
        Ok(())
    }
}

fn append_new_block(
    fm: &file_manager::FileManager,
    page: &mut page::Page,
    logfile: &str,
) -> Result<blockid::BlockId, io::Error> {
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
        let mut fm = file_manager::FileManager::new(dir.path(), 400);
        let mut log_manager = LogManager::new(&mut fm, "log_file").unwrap();

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
    }

    #[test]
    fn test_many_logs() {
        let dir = tempfile::tempdir().unwrap();
        let mut fm = file_manager::FileManager::new(dir.path(), 400);
        let mut log_manager = LogManager::new(&mut fm, "log_file").unwrap();

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
    }
}

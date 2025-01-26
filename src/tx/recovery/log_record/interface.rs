use thiserror::Error;

use crate::file::page::Page;
use crate::log::log_manager;
use crate::tx::buffer_list::BufferListError;
use crate::tx::transaction::TransactionSetError;

use super::commit_record::CommitRecord;
use super::rollback_record::RollbackRecord;
use super::set_int_record::SetIntRecord;
use super::set_string_record::SetStringRecord;
use super::start_record::StartRecord;

#[derive(Debug, Eq, PartialEq)]
pub enum LogRecord {
    CheckPoint(),
    Start(StartRecord),
    Commit(CommitRecord),
    Rollback(RollbackRecord),
    SetIntRecord(SetIntRecord),
    SetStringRecord(SetStringRecord),
}

#[derive(Debug, Eq, PartialEq)]
pub enum LogOp {
    CheckPoint = 0,
    Start = 1,
    Commit = 2,
    Rollback = 3,
    SetInt = 4,
    SetString = 5,
}

#[derive(Error, Debug)]
pub enum LogRecordError {
    #[error("Log manager error: {0}")]
    LogErrorError(#[from] log_manager::LogError),
    #[error("FromUtf8Error: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("Log record error: {0}")]
    GeneralError(#[from] anyhow::Error),
}

#[derive(Error, Debug)]
pub enum LogReplayError {
    #[error("Lock table error: {0}")]
    BufferListError(#[from] BufferListError),
    #[error("file manager error: {0}")]
    TransactionSetError(#[from] TransactionSetError),
}

impl LogRecord {
    pub fn op(&self) -> LogOp {
        match self {
            LogRecord::CheckPoint() => LogOp::CheckPoint,
            LogRecord::Start(_) => LogOp::Start,
            LogRecord::Commit(_) => LogOp::Commit,
            LogRecord::Rollback(_) => LogOp::Rollback,
            LogRecord::SetIntRecord(_) => LogOp::SetInt,
            LogRecord::SetStringRecord(_) => LogOp::SetString,
        }
    }

    /**
     * byte 列から LogRecord を作成する
     */
    pub fn new(bytes: &[u8]) -> Result<LogRecord, LogRecordError> {
        let page = Page::new_from_vec(bytes);
        let op = LogOp::from_i32(page.get_int(0)).ok_or_else(|| {
            LogRecordError::GeneralError(anyhow::anyhow!("Unknown log record operation"))
        })?;
        match op {
            LogOp::CheckPoint => Ok(LogRecord::CheckPoint()),
            LogOp::Start => {
                let inner = StartRecord::new(bytes);
                Ok(LogRecord::Start(inner))
            }
            LogOp::Commit => {
                let inner = CommitRecord::new(bytes);
                Ok(LogRecord::Commit(inner))
            }
            LogOp::Rollback => {
                let inner = RollbackRecord::new(bytes);
                Ok(LogRecord::Rollback(inner))
            }
            LogOp::SetInt => {
                let inner = SetIntRecord::new(bytes)?;
                Ok(LogRecord::SetIntRecord(inner))
            }
            LogOp::SetString => {
                let inner = SetStringRecord::new(bytes)?;
                Ok(LogRecord::SetStringRecord(inner))
            }
        }
    }
}

impl LogOp {
    pub fn from_i32(n: i32) -> Option<LogOp> {
        match n {
            0 => Some(LogOp::CheckPoint),
            1 => Some(LogOp::Start),
            2 => Some(LogOp::Commit),
            3 => Some(LogOp::Rollback),
            4 => Some(LogOp::SetInt),
            5 => Some(LogOp::SetString),
            _ => None,
        }
    }
}

#[cfg(test)]
mod log_record_test {
    use crate::file::blockid::BlockId;
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;
    use crate::tx::recovery::log_record::check_point_record::CheckPointRecord;

    use std::sync::Arc;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_all_log_record() {
        let dir = tempdir().unwrap();
        let fm = FileManager::new(dir.path(), 400);
        let lm = LogManager::new(Arc::new(fm), "test.log").unwrap();

        // checkpoint -> tx1 start -> tx1 set_int -> tx1 rollback -> tx2 start -> tx2 set_string -> tx2 commit
        let lsn = CheckPointRecord::write_to_log(&lm).unwrap();
        assert_eq!(lsn, 1);
        let lsn = StartRecord::write_to_log(&lm, 5).unwrap();
        assert_eq!(lsn, 2);
        let lsn =
            SetIntRecord::write_to_log(&lm, 6, &BlockId::new("testfile", 1), 100, 50, 80).unwrap();
        assert_eq!(lsn, 3);
        let lsn = RollbackRecord::write_to_log(&lm, 5).unwrap();
        assert_eq!(lsn, 4);
        let lsn = StartRecord::write_to_log(&lm, 6).unwrap();
        assert_eq!(lsn, 5);
        let lsn =
            SetStringRecord::write_to_log(&lm, 7, &BlockId::new("testfile", 2), 200, "old", "new")
                .unwrap();
        assert_eq!(lsn, 6);
        let lsn = CommitRecord::write_to_log(&lm, 6).unwrap();
        assert_eq!(lsn, 7);

        // 最新のものから順に取り出す
        let mut log_iter = lm.iterator().unwrap();

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::Commit);

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::SetString);

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::Start);

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::Rollback);

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::SetInt);

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::Start);

        let record = LogRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.op(), LogOp::CheckPoint);
    }
}

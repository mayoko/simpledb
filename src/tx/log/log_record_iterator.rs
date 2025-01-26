use super::record::log_record::LogRecord;
use crate::file::file_manager::FileManagerError;
use crate::log::log_iterator::{LogIterator, LogReverseIterator};
use crate::log::log_manager::{self, LogError};

use std::sync::Arc;

/**
 * recovery や rollback を使う際に用いる、db の log record を最新のものから順に読み込むためのクラス
 * log_manager では読み書き対象が bytes であり内容についてはなんの知識も持たなかったが、このクラスでは log record の内容について知識を持つ
 *
 * log を読みたくなったタイミングで new でインスタンスを生成し、その後 next を呼び出すことで最新の log record から順に読み込むことができる
 *
 * このクラスのインスタンスはプログラム中に何個あっても良い
 */
pub struct LogRecordIterator {
    log_iter: LogIterator,
}

/**
 * recovery や rollback を使う際に用いる、db の log record を古いものから順に読み込むためのクラス
 * log_manager では読み書き対象が bytes であり内容についてはなんの知識も持たなかったが、このクラスでは log record の内容について知識を持つ
 *
 * log を読みたくなったタイミングで LogRecordIterator でインスタンスを生成し、その後 next を呼び出すことで、LogRecordIterator で読み込んだ log record を逆順に辿って最新の log record まで読み込むことができる
 *
 * このクラスのインスタンスはプログラム中に何個あっても良い
 */
pub struct LogRecordReverseIterator {
    log_iter: LogReverseIterator,
}

impl LogRecordIterator {
    pub fn new(lm: Arc<log_manager::LogManager>) -> Result<Self, LogError> {
        let log_iter = lm.iterator()?;
        Ok(LogRecordIterator { log_iter })
    }
}

impl LogRecordReverseIterator {
    pub fn new(iter: &LogRecordIterator) -> Result<LogRecordReverseIterator, FileManagerError> {
        Ok(LogRecordReverseIterator {
            log_iter: LogReverseIterator::new(&iter.log_iter)?,
        })
    }
}

impl Iterator for LogRecordIterator {
    type Item = LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self.log_iter.next() {
            Some(bytes) => match LogRecord::new(&bytes) {
                Ok(log_record) => Some(log_record),
                Err(_) => {
                    eprintln!("failed to parse log record: {:?}", bytes);
                    None
                }
            },
            None => None,
        }
    }
}

impl Iterator for LogRecordReverseIterator {
    type Item = LogRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match self.log_iter.next() {
            Some(bytes) => match LogRecord::new(&bytes) {
                Ok(log_record) => Some(log_record),
                Err(_) => {
                    eprintln!("failed to parse log record: {:?}", bytes);
                    None
                }
            },
            None => None,
        }
    }
}

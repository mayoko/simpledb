use super::record::commit_record::CommitRecord;
use super::record::rollback_record::RollbackRecord;
use super::record::{
    check_point_record::CheckPointRecord, log_record::LogRecordError, set_int_record::SetIntRecord,
    set_string_record::SetStringRecord, start_record::StartRecord,
};
use crate::buffer::buffer;
use crate::log::log_manager;

use std::sync::Arc;

use anyhow::Context;

/**
 * recovery や rollback を使う際に用いる、db の log record を書き込むためのクラス
 * log_manager では読み書き対象が bytes であり内容についてはなんの知識も持たなかったが、このクラスでは log record の内容について知識を持つ
 *
 * このクラスのインスタンスはプログラム中に何個あっても良い
 */
pub struct LogRecordWriter {
    lm: Arc<log_manager::LogManager>,
}

impl LogRecordWriter {
    pub fn new(lm: Arc<log_manager::LogManager>) -> LogRecordWriter {
        LogRecordWriter { lm }
    }

    pub fn log_check_point(&self) -> Result<u64, LogRecordError> {
        let lsn = CheckPointRecord::write_to_log(&self.lm)?;
        self.lm.flush(lsn)?;
        Ok(lsn)
    }

    pub fn log_start(&self, txnum: u32) -> Result<u64, LogRecordError> {
        let lsn = StartRecord::write_to_log(&self.lm, txnum)?;
        Ok(lsn)
    }

    pub fn log_commit(&self, txnum: u32) -> Result<u64, LogRecordError> {
        let lsn = CommitRecord::write_to_log(&self.lm, txnum)?;
        // 永続性のため、log は即座に反映する必要がある
        self.lm.flush(lsn)?;
        Ok(lsn)
    }

    pub fn log_rollback(&self, txnum: u32) -> Result<u64, LogRecordError> {
        let lsn = RollbackRecord::write_to_log(&self.lm, txnum)?;
        // 永続性のため、log は即座に反映する必要がある
        self.lm.flush(lsn)?;
        Ok(lsn)
    }

    pub fn log_set_string(
        &self,
        txnum: u32,
        buff: &buffer::Buffer,
        offset: usize,
        new_val: &str,
    ) -> Result<u64, LogRecordError> {
        let block = buff
            .block()
            .context("buffer block must be set before logging")?;
        let old_val = buff.contents().get_string(offset)?;

        let lsn = SetStringRecord::write_to_log(&self.lm, txnum, block, offset, &old_val, new_val)?;
        Ok(lsn)
    }

    pub fn log_set_int(
        &self,
        txnum: u32,
        buff: &buffer::Buffer,
        offset: usize,
        new_val: i32,
    ) -> Result<u64, LogRecordError> {
        let block = buff
            .block()
            .context("buffer block must be set before logging")?;
        let old_val = buff.contents().get_int(offset);

        let lsn = SetIntRecord::write_to_log(&self.lm, txnum, block, offset, old_val, new_val)?;
        Ok(lsn)
    }
}

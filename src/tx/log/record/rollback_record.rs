use super::log_record::LogOp;
use crate::constants::INTEGER_BYTE_LEN;
use crate::file::page::Page;
use crate::log::log_manager::{LogError, LogManager};

/**
 * transaction が正常に完了せず、変更を戻したことを示す log record
 */
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct RollbackRecord {
    txnum: u32,
}

impl RollbackRecord {
    /**
     * byte 列から CommitRecord を再現する
     */
    pub fn new(bytes: &[u8]) -> Self {
        let p = Page::new_from_vec(bytes);
        let txnum = p.get_int(INTEGER_BYTE_LEN) as u32;

        RollbackRecord { txnum }
    }
    /**
     * transaction が正常に完了せず、変更を戻したことを log に書き込む関数
     *
     * 成功した場合、書き込まれた log sequence number を返す
     */
    pub fn write_to_log(lm: &LogManager, txnum: u32) -> Result<u64, LogError> {
        let record_len = INTEGER_BYTE_LEN * 2;
        let mut p = Page::new_from_size(record_len);
        p.set_int(0, LogOp::Rollback as i32);
        p.set_int(INTEGER_BYTE_LEN, txnum as i32);

        let lsn = lm.append(p.contents())?;
        Ok(lsn)
    }
}

#[cfg(test)]
mod rollback_record_test {
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;

    use std::sync::Arc;
    use tempfile::tempdir;

    use super::RollbackRecord;

    #[test]
    fn test_rollback_record_log() {
        let dir = tempdir().unwrap();
        let fm = FileManager::new(dir.path(), 400);
        let lm = LogManager::new(Arc::new(fm), "test.log").unwrap();

        RollbackRecord::write_to_log(&lm, 5).unwrap();

        let mut log_iter = lm.iterator().unwrap();
        let record = RollbackRecord::new(&log_iter.next().unwrap());
        assert_eq!(record.txnum, 5);
    }
}

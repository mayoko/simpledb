use super::interface::LogOp;
use crate::constants::INTEGER_BYTE_LEN;
use crate::file::page::Page;
use crate::log::log_manager::{LogError, LogManager};

/**
 * recovery が完了し、これより前の record はすべて完了した transaction として block に書き込まれたことを示す log record
 */
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct CheckPointRecord {}

impl CheckPointRecord {
    /**
     * check point record の内容を log として書き込むための関数
     *
     * 成功した場合、書き込まれた log sequence number を返す
     */
    pub fn write_to_log(lm: &LogManager) -> Result<u64, LogError> {
        let mut p = Page::new_from_size(INTEGER_BYTE_LEN);
        p.set_int(0, LogOp::CheckPoint as i32);

        let lsn = lm.append(p.contents())?;
        Ok(lsn)
    }
}

#[cfg(test)]
mod check_point_record_test {
    use crate::file::file_manager::FileManager;
    use crate::file::page::Page;
    use crate::log::log_manager::LogManager;
    use crate::tx::recovery::log_record::interface::LogOp;

    use std::sync::Arc;
    use tempfile::tempdir;

    use super::CheckPointRecord;

    #[test]
    fn test_write_to_log() {
        let dir = tempdir().unwrap();
        let fm = FileManager::new(dir.path(), 400);
        let lm = LogManager::new(Arc::new(fm), "test.log").unwrap();

        CheckPointRecord::write_to_log(&lm).unwrap();

        let mut log_iter = lm.iterator().unwrap();
        let page = Page::new_from_vec(&log_iter.next().unwrap());
        assert_eq!(page.get_int(0), LogOp::CheckPoint as i32);
    }
}

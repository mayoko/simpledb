use std::string::FromUtf8Error;

use super::log_record::{LogOp, LogReplayError};
use crate::constants::INTEGER_BYTE_LEN;
use crate::file::{blockid, page};
use crate::log::log_manager;
use crate::tx::transaction::Transaction;

/**
 * 文字列を変更したことを示す log record で保持する情報
 */
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct SetStringRecord {
    txnum: u32,
    block: blockid::BlockId,
    offset: usize,
    old_value: String,
    new_value: String,
}

impl SetStringRecord {
    /**
     * byte 列から SetStringRecordInner を再現する
     */
    pub fn new(bytes: &[u8]) -> Result<Self, FromUtf8Error> {
        let p = page::Page::new_from_vec(bytes);
        let tpos = INTEGER_BYTE_LEN;
        let txnum = p.get_int(tpos) as u32;

        let fpos = tpos + INTEGER_BYTE_LEN;
        let filename = p.get_string(fpos)?;
        let bpos = fpos + filename.len() + INTEGER_BYTE_LEN;
        let blknum = p.get_int(bpos) as usize;
        let block = blockid::BlockId::new(&filename, blknum);

        let opos = bpos + INTEGER_BYTE_LEN;
        let offset = p.get_int(opos) as usize;

        let ovpos = opos + INTEGER_BYTE_LEN;
        let old_value = p.get_string(ovpos)?;

        let nvpos = ovpos + old_value.len() + INTEGER_BYTE_LEN;
        let new_value = p.get_string(nvpos)?;

        Ok(SetStringRecord {
            txnum,
            block,
            offset,
            old_value,
            new_value,
        })
    }

    /**
     * transaction 番号を取得する
     */
    pub fn tx_num(&self) -> u32 {
        self.txnum
    }

    /**
     * log record の内容を元に、指定された transaction のもとで undo を実行する
     * rollback や recovery で利用される
     */
    pub fn undo(&self, tx: &mut Transaction) -> Result<(), LogReplayError> {
        tx.pin(&self.block)?;
        tx.set_string(&self.block, self.offset, &self.old_value, false)?;
        Ok(())
    }

    /**
     * log record の内容を元に、指定された transaction のもとで redo を実行する
     * recovery で利用される
     */
    pub fn redo(&self, tx: &mut Transaction) -> Result<(), LogReplayError> {
        tx.pin(&self.block)?;
        tx.set_string(&self.block, self.offset, &self.new_value, false)?;
        Ok(())
    }

    /**
     * SetString log record の内容を log として書き込むための関数
     *
     * 成功した場合、書き込まれた log sequence number を返す
     */
    pub fn write_to_log(
        lm: &log_manager::LogManager,
        txnum: u32,
        block: &blockid::BlockId,
        offset: usize,
        old_val: &str,
        new_val: &str,
    ) -> Result<u64, log_manager::LogError> {
        let tpos = INTEGER_BYTE_LEN;
        let fpos = tpos + INTEGER_BYTE_LEN;
        let bpos = fpos + block.file_name().len() + INTEGER_BYTE_LEN;
        let opos = bpos + INTEGER_BYTE_LEN;
        let ovpos = opos + INTEGER_BYTE_LEN;
        let nvpos = ovpos + old_val.len() + INTEGER_BYTE_LEN;
        let record_len = nvpos + new_val.len() + INTEGER_BYTE_LEN;

        let mut p = page::Page::new_from_size(record_len);
        p.set_int(0, LogOp::SetString as i32);
        p.set_int(tpos, txnum as i32);
        p.set_string(fpos, block.file_name());
        p.set_int(bpos, block.number() as i32);
        p.set_int(opos, offset as i32);
        p.set_string(ovpos, old_val);
        p.set_string(nvpos, new_val);

        let lsn = lm.append(p.contents())?;

        Ok(lsn)
    }
}

#[cfg(test)]
mod set_string_record_test {
    use crate::file::blockid::BlockId;
    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;

    use std::sync::Arc;
    use tempfile::tempdir;

    use super::SetStringRecord;

    #[test]
    fn test_set_int_record_log() {
        let dir = tempdir().unwrap();
        let fm = FileManager::new(dir.path(), 400);
        let lm = LogManager::new(Arc::new(fm), "test.log").unwrap();

        SetStringRecord::write_to_log(&lm, 5, &BlockId::new("testfile", 0), 80, "old", "new")
            .unwrap();

        let mut log_iter = lm.iterator().unwrap();
        let record = SetStringRecord::new(&log_iter.next().unwrap()).unwrap();
        assert_eq!(record.txnum, 5);
        assert_eq!(record.block, BlockId::new("testfile", 0));
        assert_eq!(record.offset, 80);
        assert_eq!(record.old_value, "old");
        assert_eq!(record.new_value, "new");
    }
}

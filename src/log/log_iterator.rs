use std::sync::Arc;

use crate::constants::INTEGER_BYTE_LEN;
use crate::file::blockid;
use crate::file::file_manager;
use crate::file::page;

/**
 * 最新のログから順番に読んでいくための iterator
 */
pub struct LogIterator {
    fm: Arc<file_manager::FileManager>,
    block: blockid::BlockId,
    page: page::Page,
    current_pos: usize, // block 内部での位置
}

/**
 * ログを逆順に読むための iterator
 */
pub struct LogReverseIterator {
    fm: Arc<file_manager::FileManager>,
    block: blockid::BlockId,
    page: page::Page,
    rec_pos_list: Vec<usize>,   // log record の開始地点のリスト
    current_idx: Option<usize>, // rec_pos_list の index. 指すべきものがない場合 (rec_pos_list が空の場合) は None
}

impl LogIterator {
    pub fn new(
        fm: Arc<file_manager::FileManager>,
        block: &blockid::BlockId,
    ) -> Result<LogIterator, file_manager::FileManagerError> {
        let block_size = fm.block_size();
        let mut log_iterator = LogIterator {
            fm: fm,
            block: block.clone(),
            page: page::Page::new_from_size(block_size),
            current_pos: 0,
        };
        log_iterator.move_to_block(&block)?;

        Ok(log_iterator)
    }

    fn move_to_block(
        &mut self,
        block: &blockid::BlockId,
    ) -> Result<(), file_manager::FileManagerError> {
        self.block = block.clone();
        self.fm.read(&self.block, &mut self.page)?;
        let boundary = self.page.get_int(0) as usize;
        self.current_pos = boundary;
        Ok(())
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let block_size = self.fm.block_size();
        let block_number = self.block.number();
        // block の最後まで読んだ && ログの最初の block まで読んだ
        if self.current_pos == block_size && block_number == 0 {
            return None;
        }
        // 今の block の最後まで読んでいたら、前の block に移動する
        if self.current_pos == block_size {
            let prev_block = blockid::BlockId::new(self.block.file_name(), block_number - 1);
            match self.move_to_block(&prev_block) {
                Ok(_) => {}
                Err(_) => return None,
            }
        }
        let log_rec = self.page.get_bytes(self.current_pos);
        self.current_pos += INTEGER_BYTE_LEN + log_rec.len();
        Some(log_rec)
    }
}

impl LogReverseIterator {
    /**
     * LogIterator から逆順の iterator を作成する
     */
    pub fn new(iter: &LogIterator) -> Result<Self, file_manager::FileManagerError> {
        let rec_pos_list = Self::construct_rec_pos_list(iter.current_pos, &iter.block, &iter.fm)?;
        let current_idx = if rec_pos_list.is_empty() {
            None
        } else {
            Some(rec_pos_list.len() - 1)
        };
        Ok(LogReverseIterator {
            fm: iter.fm.clone(),
            block: iter.block.clone(),
            page: page::Page::new_from_vec(iter.page.contents()),
            rec_pos_list,
            current_idx,
        })
    }

    fn construct_rec_pos_list(
        pos: usize,
        block: &blockid::BlockId,
        fm: &file_manager::FileManager,
    ) -> Result<Vec<usize>, file_manager::FileManagerError> {
        let mut rec_pos_list = Vec::new();
        let mut page = page::Page::new_from_size(fm.block_size());
        fm.read(block, &mut page)?;
        let mut current_pos = page.get_int(0) as usize;

        while current_pos < pos {
            rec_pos_list.push(current_pos);
            let log_rec = page.get_bytes(current_pos);
            current_pos += INTEGER_BYTE_LEN + log_rec.len();
        }
        Ok(rec_pos_list)
    }

    fn move_to_block(
        &mut self,
        block: &blockid::BlockId,
    ) -> Result<(), file_manager::FileManagerError> {
        self.block = block.clone();
        self.fm.read(&self.block, &mut self.page)?;
        self.rec_pos_list = Self::construct_rec_pos_list(self.fm.block_size(), &block, &self.fm)?;
        self.current_idx = if self.rec_pos_list.is_empty() {
            None
        } else {
            Some(self.rec_pos_list.len() - 1)
        };
        Ok(())
    }
}

impl Iterator for LogReverseIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        let block_number = self.block.number();

        let block_length = self.fm.length(self.block.file_name());
        if block_length.is_err() || self.current_idx.is_none() {
            return None;
        }
        // 上で none かどうか確認しているので unwrap して OK
        let idx = self.current_idx.unwrap();
        let block_length = block_length.unwrap();

        let log_rec = self.page.get_bytes(self.rec_pos_list[idx]);
        // current_idx, (rec_pos_list) の更新
        if idx == 0 {
            if block_number == block_length - 1 {
                // すべての block を読み終わった
                self.current_idx = None;
            } else {
                // move_to_block で両者が更新される
                let next_block = blockid::BlockId::new(self.block.file_name(), block_number + 1);
                let _ = self.into_iter().move_to_block(&next_block);
            }
        } else {
            self.current_idx = Some(idx - 1);
        }
        Some(log_rec)
    }
}

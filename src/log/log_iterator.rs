use crate::file::blockid;
use crate::file::file_manager;
use crate::file::page;
use std::io;

/**
 * 最新のログから順番に読んでいくための iterator
 */
pub struct LogIterator<'a> {
    fm: &'a file_manager::FileManager,
    block: blockid::BlockId,
    page: page::Page,
    current_pos: usize, // block 内部での位置
}

impl<'a> LogIterator<'a> {
    pub fn new(
        fm: &'a file_manager::FileManager,
        block: &blockid::BlockId,
    ) -> Result<LogIterator<'a>, io::Error> {
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

    fn move_to_block(&mut self, block: &blockid::BlockId) -> Result<(), io::Error> {
        self.block = block.clone();
        self.fm.read(&self.block, &mut self.page)?;
        let boundary = self.page.get_int(0) as usize;
        self.current_pos = boundary;
        Ok(())
    }
}

impl Iterator for LogIterator<'_> {
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
        // 4 = integer length
        self.current_pos += 4 + log_rec.len();
        Some(log_rec)
    }
}

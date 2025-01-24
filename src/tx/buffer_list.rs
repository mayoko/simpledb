use std::collections::{hash_map::Entry, HashMap};
use std::sync::{Arc, Mutex};

use thiserror::Error;

use crate::buffer::buffer_manager::BufferManagerError;
use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    file::blockid::BlockId,
};

/**
 * transaction で使うために pin している buffer のリストを管理するクラス
 *
 * 書き込みするための lock を取得するなど、並行実行性の担保は transaction 側で行うので、ここでは考えなくて良い
 * transaction は一つの thread 内でしか動かないので、thread 間での競合は考慮しなくて良い
 */
pub(crate) struct BufferList<'a> {
    // 保持している buffer のリスト
    buffers: HashMap<BlockId, Arc<Mutex<Buffer<'a>>>>,
    // pin している block のリスト
    pins: Vec<BlockId>,
    buffer_manager: &'a BufferManager<'a>,
}

#[derive(Error, Debug)]
pub enum BufferListError {
    #[error("buffer manager error")]
    BufferManagerError(#[from] BufferManagerError),
    #[error("buffer list error caused by invalid method call: {0}")]
    InvalidMethodCallError(String),
    #[error("buffer list error caused by invalid state. it is likely because state management in this class is not appropriate: {0}")]
    InvalidStateError(String),
}

impl<'a> BufferList<'a> {
    pub fn new(buffer_manager: &'a BufferManager<'a>) -> BufferList<'a> {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    pub fn get_buffer(&mut self, block: &BlockId) -> Option<Arc<Mutex<Buffer<'a>>>> {
        self.buffers.get(block).cloned()
    }

    /**
     * 指定された block を pin する
     *
     * すでに pin されていた block であっても、再度 pin するような挙動をするので、unpin では必ず pin した回数分だけ unpin する必要がある
     */
    pub fn pin(&mut self, block: &BlockId) -> Result<Arc<Mutex<Buffer<'a>>>, BufferManagerError> {
        let buffer = self.buffer_manager.pin(block)?;
        self.buffers
            .entry(block.clone())
            .or_insert_with(|| buffer.clone());
        self.pins.push(block.clone());

        Ok(buffer)
    }

    /**
     * 指定された block を unpin する
     *
     * pin されていない block を unpin しようとした場合はエラーを返す
     */
    pub fn unpin(&mut self, block: &BlockId) -> Result<(), BufferListError> {
        let entry = self.buffers.entry(block.clone());
        match entry {
            Entry::Occupied(occupied) => {
                let buffer = occupied.get();
                self.buffer_manager.unpin(buffer.clone())?;

                match self.pins.iter().position(|b| b == block) {
                    Some(pos) => {
                        self.pins.remove(pos);
                        if !self.pins.contains(block) {
                            occupied.remove();
                        }
                        Ok(())
                    }
                    None => Err(BufferListError::InvalidMethodCallError(format!(
                        "block {} is not pinned",
                        block
                    ))),
                }
            }
            Entry::Vacant(_) => Err(BufferListError::InvalidMethodCallError(format!(
                "block {} is not pinned",
                block
            ))),
        }
    }

    pub fn unpin_all(&mut self) -> Result<(), BufferListError> {
        for block in &self.pins {
            match self.buffers.get(block) {
                Some(buffer) => {
                    self.buffer_manager.unpin(buffer.clone())?;
                }
                None => {
                    return Err(BufferListError::InvalidStateError(format!(
                        "block {} is not pinned",
                        block
                    )));
                }
            }
        }

        self.buffers.clear();
        self.pins.clear();

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tempfile::tempdir;

    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;

    #[test]
    fn test_pin_and_unpin() {
        let dir = tempdir().unwrap();
        let file_manager = FileManager::new(dir.path(), 400);
        let log_manager = LogManager::new(&file_manager, "test.log").unwrap();
        let buffer_manager = BufferManager::new(&file_manager, &log_manager, 3, Some(10));
        let mut buffer_list = BufferList::new(&buffer_manager);

        let block = BlockId::new("testfile", 0);

        buffer_list.pin(&block).unwrap();
        buffer_list.pin(&block).unwrap();

        // 2 回 pin しているので 2 回は unpin できる
        assert!(buffer_list.unpin(&block).is_ok());
        assert!(buffer_list.unpin(&block).is_ok());
        assert!(buffer_list.unpin(&block).is_err());
    }

    #[test]
    fn test_unpin_all() {
        let dir = tempdir().unwrap();
        let file_manager = FileManager::new(dir.path(), 400);
        let log_manager = LogManager::new(&file_manager, "test.log").unwrap();
        let buffer_manager = BufferManager::new(&file_manager, &log_manager, 3, Some(10));
        let mut buffer_list = BufferList::new(&buffer_manager);

        let block = BlockId::new("testfile", 0);

        buffer_list.pin(&block).unwrap();
        buffer_list.pin(&block).unwrap();

        assert!(buffer_list.unpin_all().is_ok());
    }
}

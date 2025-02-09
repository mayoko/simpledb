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
pub(crate) struct BufferList {
    // 保持している buffer のリスト
    buffers: HashMap<BlockId, Arc<Mutex<Buffer>>>,
    // pin している block のリスト
    pins: Vec<BlockId>,
    buffer_manager: Arc<BufferManager>,
}

#[derive(Error, Debug)]
pub enum BufferListError {
    #[error("buffer manager error")]
    BufferManager(#[from] BufferManagerError),
    #[error("buffer list error caused by invalid method call: {0}")]
    InvalidMethodCall(String),
    #[error("buffer list error caused by invalid state. it is likely because state management in this class is not appropriate: {0}")]
    InvalidState(String),
}

impl BufferList {
    pub fn new(buffer_manager: Arc<BufferManager>) -> BufferList {
        BufferList {
            buffers: HashMap::new(),
            pins: Vec::new(),
            buffer_manager,
        }
    }

    pub fn get_buffer(&mut self, block: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.buffers.get(block).cloned()
    }

    /**
     * 指定された block を pin する
     *
     * Note: すでに pin されていた block であっても、再度 pin するような挙動をするので、unpin では必ず pin した回数分だけ unpin する必要がある
     */
    pub fn pin(&mut self, block: &BlockId) -> Result<Arc<Mutex<Buffer>>, BufferManagerError> {
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
                    None => Err(BufferListError::InvalidMethodCall(format!(
                        "block {} is not pinned",
                        block
                    ))),
                }
            }
            Entry::Vacant(_) => Err(BufferListError::InvalidMethodCall(format!(
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
                    return Err(BufferListError::InvalidState(format!(
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
mod buffer_list_test {
    use std::path;

    use super::*;
    use tempfile::tempdir;

    use crate::file::file_manager::FileManager;
    use crate::log::log_manager::LogManager;

    fn setup_buffer_list(dir_path: &path::Path) -> BufferList {
        let file_manager = Arc::new(FileManager::new(dir_path, 400));
        let log_manager = Arc::new(LogManager::new(file_manager.clone(), "test.log").unwrap());
        let buffer_manager = Arc::new(BufferManager::new(file_manager, log_manager, 3, Some(10)));
        BufferList::new(buffer_manager)
    }

    #[test]
    fn test_pin_and_unpin() {
        let dir = tempdir().unwrap();
        let mut buffer_list = setup_buffer_list(dir.path());
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
        let mut buffer_list = setup_buffer_list(dir.path());
        let block = BlockId::new("testfile", 0);

        buffer_list.pin(&block).unwrap();
        buffer_list.pin(&block).unwrap();

        assert!(buffer_list.unpin_all().is_ok());
    }
}

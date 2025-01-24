use std::sync::{Arc, Condvar, Mutex};
use std::time;
use thiserror::Error;

use crate::buffer::buffer;
use crate::file::{blockid, file_manager};
use crate::log::log_manager;

const MAX_PIN_WAIT_TIME_MS: u64 = 10_000; // 10 seconds

/**
 * block (disk 上のデータ) を page を用いて操作できる Buffer を、pool として管理するクラス
 *
 * クライアントは、pin をすることで、参照したい block を Buffer を通して操作し、操作が終わったら unpin を行って不要になったことを通知する.
 * また、flush_all を呼ぶことで、buffer pool に書き込まれた内容を block に書き込み、永続性を保証することができる.
 *
 * プログラム全体で一つしかない想定
 */
pub struct BufferManager<'a> {
    buffer_pool: Vec<Arc<Mutex<buffer::Buffer<'a>>>>,
    num_available: Arc<(Mutex<usize>, Condvar)>,
    max_pin_wait_time_ms: u64,
}

#[derive(Error, Debug)]
pub enum BufferManagerError {
    #[error("Error from buffer: {0}")]
    BufferError(#[from] buffer::BufferError),
    #[error("Failed to acquire lock")]
    LockError,
    #[error("Failed to pin buffer")]
    PinError,
    #[error("Log error: {0}")]
    LogError(#[from] log_manager::LogError),
}

impl<'a> BufferManager<'a> {
    pub fn new(
        fm: &'a file_manager::FileManager,
        lm: &'a log_manager::LogManager<'a>,
        num_buffs: usize,
        max_pin_wait_time_ms: Option<u64>,
    ) -> BufferManager<'a> {
        let mut buffer_pool = Vec::new();
        buffer_pool.reserve(num_buffs);
        for _ in 0..num_buffs {
            buffer_pool.push(Arc::new(Mutex::new(buffer::Buffer::new(fm, lm))));
        }
        BufferManager {
            buffer_pool: buffer_pool,
            num_available: Arc::new((Mutex::new(num_buffs), Condvar::new())),
            max_pin_wait_time_ms: match max_pin_wait_time_ms {
                Some(ms) => ms,
                None => MAX_PIN_WAIT_TIME_MS,
            },
        }
    }

    // Buffer にある空きの buffer の数を返す
    pub fn available(&self) -> Result<usize, BufferManagerError> {
        let (value, _) = &*self.num_available;
        Ok(value
            .lock()
            .map_err(|_| BufferManagerError::LockError)?
            .clone())
    }

    // buffer pool に書き込まれた内容を block い書き込み、永続性を保証する
    pub fn flush_all(&self) -> Result<(), BufferManagerError> {
        for buf_lock in &self.buffer_pool {
            let mut buf = buf_lock.lock().map_err(|_| BufferManagerError::LockError)?;
            if buf.block().is_some() {
                buf.flush()?;
            }
        }
        Ok(())
    }

    // 不要になった buffer を pin から外す
    pub fn unpin(&self, buf: Arc<Mutex<buffer::Buffer>>) -> Result<(), BufferManagerError> {
        let mut buf = buf.lock().map_err(|_| BufferManagerError::LockError)?;
        buf.unpin();
        if !buf.is_pinned() {
            let (value, cond) = &*self.num_available;
            let mut num_available = value.lock().map_err(|_| BufferManagerError::LockError)?;
            *num_available += 1;
            cond.notify_all();
        }
        Ok(())
    }

    // 必要になる buffer を pin する.
    // max_pin_wait_time_ms まで buffer が確保できない場合、エラーを返す
    pub fn pin(
        &'a self,
        blk: &blockid::BlockId,
    ) -> Result<Arc<Mutex<buffer::Buffer<'a>>>, BufferManagerError> {
        let start = time::Instant::now();
        let mut buff = self.try_to_pin(blk)?;
        while buff.is_none() && get_waiting_time(start) < self.max_pin_wait_time_ms {
            // buffer が確保できなかった場合、max_pin_wait_time_ms まで待つ
            let (num_available_lock, cond) = &*self.num_available;
            let num_available = num_available_lock
                .lock()
                .map_err(|_| BufferManagerError::LockError)?;
            let (_num_available, _) = cond
                .wait_timeout(
                    num_available,
                    time::Duration::from_millis(self.max_pin_wait_time_ms),
                )
                .map_err(|_| BufferManagerError::PinError)?;
            // buffer が空いた通知が来たので、再度 buffer 確保を試みる
            buff = self.try_to_pin(blk)?;
        }
        match buff {
            Some(b) => Ok(b),
            None => Err(BufferManagerError::PinError),
        }
    }

    // buffer pool に block を割り当てを試みる
    // 割り当てられなかった場合、None を返す
    fn try_to_pin(
        &'a self,
        blk: &blockid::BlockId,
    ) -> Result<Option<Arc<Mutex<buffer::Buffer<'a>>>>, BufferManagerError> {
        let maybe_buf_lock = self.find_existing_buffer(blk)?;
        let maybe_buf_lock = match maybe_buf_lock {
            Some(buf_lock) => Some(buf_lock),
            None => {
                // buffer pool に block を参照している buffer が存在しない場合、pin されていない buffer から確保を試みる
                let maybe_buf_lock = self.choose_unpinned_buffer()?;
                match maybe_buf_lock {
                    None => None,
                    Some(buf_lock) => {
                        // pin できる buffer が見つかった場合、その buffer に block を割り当てる
                        let mut buf = buf_lock.lock().map_err(|_| BufferManagerError::LockError)?;
                        buf.assign_to_block(blk)?;
                        Some(buf_lock.clone())
                    }
                }
            }
        };
        match maybe_buf_lock {
            Some(buf_lock) => {
                let mut buf = buf_lock.lock().map_err(|_| BufferManagerError::LockError)?;
                if !buf.is_pinned() {
                    // pin する予定の buffer がこれ以前に pin されていない場合、この pin により available な buffer が一つ減ったことを意味する
                    let (value, _) = &*self.num_available;
                    let mut num_available =
                        value.lock().map_err(|_| BufferManagerError::LockError)?;
                    *num_available -= 1;
                }

                buf.pin();
                Ok(Some(buf_lock.clone()))
            }
            None => Ok(None),
        }
    }

    // すでに buffer で保持している block の pin を要求された場合、その buffer を返す
    fn find_existing_buffer(
        &'a self,
        blk: &blockid::BlockId,
    ) -> Result<Option<Arc<Mutex<buffer::Buffer<'a>>>>, BufferManagerError> {
        for buf_lock in &self.buffer_pool {
            let buf = buf_lock.lock().map_err(|_| BufferManagerError::LockError)?;
            if let Some(b) = buf.block() {
                if b == blk {
                    return Ok(Some(buf_lock.clone()));
                }
            }
        }
        Ok(None)
    }

    // buffer pool から pin されていない buffer を選択する
    // pin されていない buffer が存在しない場合は None を返す
    fn choose_unpinned_buffer(
        &'a self,
    ) -> Result<Option<Arc<Mutex<buffer::Buffer<'a>>>>, BufferManagerError> {
        for buf_lock in &self.buffer_pool {
            let buf = buf_lock.lock().map_err(|_| BufferManagerError::LockError)?;
            if !buf.is_pinned() {
                return Ok(Some(buf_lock.clone()));
            }
        }
        Ok(None)
    }
}

fn get_waiting_time(start: time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

#[cfg(test)]
mod test_buffer_manager {
    use crate::file::page;

    use super::*;
    use tempfile;

    #[test]
    fn test_pin_result() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();

        let file_manager = file_manager::FileManager::new(&path, 400);
        let log_manager = log_manager::LogManager::new(&file_manager, "testlog").unwrap();
        // max_pin_wait_time_ms を 100 に設定することで、早めにエラーを返すようにする
        let buffer_manager = BufferManager::new(&file_manager, &log_manager, 3, Some(100));

        // この 3 つの buffer は確保することができる
        let buf0 = buffer_manager.pin(&blockid::BlockId::new("testfile", 0));
        assert!(buf0.is_ok());
        assert_eq!(buffer_manager.available().unwrap(), 2);

        let buf1 = buffer_manager.pin(&blockid::BlockId::new("testfile", 1));
        assert!(buf1.is_ok());
        assert_eq!(buffer_manager.available().unwrap(), 1);

        let buf2 = buffer_manager.pin(&blockid::BlockId::new("testfile", 2));
        assert!(buf2.is_ok());
        assert_eq!(buffer_manager.available().unwrap(), 0);

        // buffer_manager の num_buffs が 3 に設定されているため、これ以上 buffer を確保することはできない
        let buf3 = buffer_manager.pin(&blockid::BlockId::new("testfile", 3));
        assert!(buf3.is_err());
        assert!(matches!(buf3.err().unwrap(), BufferManagerError::PinError));

        // buffer が解放されると、新しい buffer を確保することができる
        buffer_manager.unpin(buf0.unwrap()).unwrap();
        let buf3 = buffer_manager.pin(&blockid::BlockId::new("testfile", 3));
        assert!(buf3.is_ok());
    }

    #[test]
    fn test_buffer_read_and_write() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();

        let file_manager = file_manager::FileManager::new(&path, 400);
        let log_manager = log_manager::LogManager::new(&file_manager, "testlog").unwrap();
        let buffer_manager = BufferManager::new(&file_manager, &log_manager, 3, Some(100));

        let buf_lock = buffer_manager
            .pin(&blockid::BlockId::new("testfile", 0))
            .unwrap();
        {
            let mut buf = buf_lock.lock().unwrap();
            let page = buf.contents_mut();
            page.set_int(0, 123);
            buf.set_modified(1, Some(0));
        }
        buffer_manager.unpin(buf_lock).unwrap();

        let pinned_buf_lock = buffer_manager
            .pin(&blockid::BlockId::new("testfile", 0))
            .unwrap();
        let pinned_buf = pinned_buf_lock.lock().unwrap();
        let pinned_page = pinned_buf.contents();
        assert_eq!(pinned_page.get_int(0), 123);
    }

    #[test]
    fn test_it_writes_to_block_when_the_buffer_is_overwritten() {
        // unpin で追い出されると file に書き込まれていることを、実際に file を読むことで確認する
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();

        let file_manager = file_manager::FileManager::new(&path, 400);
        let log_manager = log_manager::LogManager::new(&file_manager, "testlog").unwrap();
        // num_buffs を 1 に設定することで、即座に buffer が追い出されるようにする
        let buffer_manager = BufferManager::new(&file_manager, &log_manager, 1, Some(100));

        let buf0 = buffer_manager
            .pin(&blockid::BlockId::new("testfile", 0))
            .unwrap();
        // buffer に書き込む
        {
            let mut buf = buf0.lock().unwrap();
            let page = buf.contents_mut();
            page.set_int(0, 123);
            buf.set_modified(1, Some(0));
        }

        {
            let mut page = page::Page::new_from_size(400);
            file_manager
                .read(&blockid::BlockId::new("testfile", 0), &mut page)
                .unwrap();
            // まだ書き込まれていないはず
            assert_ne!(page.get_int(0), 123);
        }

        // unpin して新しい buffer を確保することで、buffer が追い出される
        buffer_manager.unpin(buf0).unwrap();
        buffer_manager
            .pin(&blockid::BlockId::new("testfile", 1))
            .unwrap();

        {
            let mut page = page::Page::new_from_size(400);
            file_manager
                .read(&blockid::BlockId::new("testfile", 0), &mut page)
                .unwrap();
            // 書き込まれているはず
            assert_eq!(page.get_int(0), 123);
        }
    }

    #[test]
    fn test_it_writes_to_block_if_flush_all_is_called() {
        // flush_all を呼ぶと file に書き込まれていることを、実際に file を読むことで確認する
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();

        let file_manager = file_manager::FileManager::new(&path, 400);
        let log_manager = log_manager::LogManager::new(&file_manager, "testlog").unwrap();
        let buffer_manager = BufferManager::new(&file_manager, &log_manager, 1, Some(100));

        let buf0 = buffer_manager
            .pin(&blockid::BlockId::new("testfile", 0))
            .unwrap();
        // buffer に書き込む
        {
            let mut buf = buf0.lock().unwrap();
            let page = buf.contents_mut();
            page.set_int(0, 123);
            buf.set_modified(1, Some(0));
        }

        {
            let mut page = page::Page::new_from_size(400);
            file_manager
                .read(&blockid::BlockId::new("testfile", 0), &mut page)
                .unwrap();
            // まだ書き込まれていないはず
            assert_ne!(page.get_int(0), 123);
        }

        buffer_manager.flush_all().unwrap();

        {
            let mut page = page::Page::new_from_size(400);
            file_manager
                .read(&blockid::BlockId::new("testfile", 0), &mut page)
                .unwrap();
            // 書き込まれているはず
            assert_eq!(page.get_int(0), 123);
        }
    }
}

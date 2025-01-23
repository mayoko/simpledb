use std::{
    collections::HashMap,
    fs,
    io::{self, Read, Seek, Write},
    os::unix::fs::OpenOptionsExt,
    path,
    sync::Mutex,
};
use thiserror::Error;

use super::blockid::BlockId;
use super::page::Page;

/**
 * simpledb では、block の中身は page を通して読み書きされる。
 * その読み書きの直接的な interface を提供するクラス
 *
 * file の中に block を連続して配置することで block を扱っているため、名前を FileManager としている
 */
pub struct FileManager {
    db_directory: path::PathBuf,
    blocksize: usize,
    is_new: bool,
    open_files: Mutex<HashMap<String, fs::File>>,
}

#[derive(Error, Debug)]
pub enum FileManagerError {
    #[error("Failed to acquire write lock")]
    LockError,
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
}

impl FileManager {
    pub fn new(db_directory: &path::Path, blocksize: usize) -> FileManager {
        let is_new = !db_directory.exists();
        if is_new {
            fs::create_dir_all(db_directory).unwrap();
        }
        let file_paths = fs::read_dir(db_directory).unwrap();
        // temp から始まるファイルは削除
        for file_path in file_paths {
            match file_path {
                Ok(file) => {
                    if file.path().starts_with("temp") {
                        fs::remove_file(file.path()).unwrap_or_else(|err| eprintln!("{err}"));
                    }
                }
                Err(error) => {
                    eprintln!("{error}");
                }
            }
        }

        FileManager {
            db_directory: path::PathBuf::from(db_directory),
            blocksize: blocksize,
            is_new: is_new,
            open_files: Mutex::new(HashMap::<String, fs::File>::new()),
        }
    }

    // ブロックの内容を page に読み込む
    pub fn read(&self, blk: &BlockId, p: &mut Page) -> Result<(), FileManagerError> {
        let blocksize = self.blocksize;

        self.cache_file(blk.file_name())?;
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| FileManagerError::LockError)?;
        let file = open_files.get_mut(blk.file_name());

        match file {
            Some(file) => {
                file.seek(io::SeekFrom::Start(blk.number() as u64 * blocksize as u64))?;
                file.read(p.contents_mut())?;
                Ok(())
            }
            None => Err(file_not_found_error()),
        }
    }

    // page の内容を block に書き込む
    pub fn write(&self, blk: &BlockId, p: &Page) -> Result<(), FileManagerError> {
        let blocksize = self.blocksize;
        self.cache_file(blk.file_name())?;
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| FileManagerError::LockError)?;
        let file = open_files.get_mut(blk.file_name());

        match file {
            Some(file) => {
                file.seek(std::io::SeekFrom::Start(
                    blk.number() as u64 * blocksize as u64,
                ))?;
                file.write(p.contents())?;
                Ok(())
            }
            None => Err(file_not_found_error()),
        }
    }

    // ファイルの末尾に新しいブロックを追加する
    pub fn append(&self, filename: &str) -> Result<BlockId, FileManagerError> {
        let blknum = self.length(filename)?;
        let block = BlockId::new(filename, blknum);
        let blocksize = self.blocksize;

        self.cache_file(filename)?;
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| FileManagerError::LockError)?;
        let file = open_files.get_mut(filename);

        match file {
            Some(file) => {
                file.seek(std::io::SeekFrom::Start((blknum * blocksize) as u64))?;

                let bytes = vec![0u8; blocksize];
                file.write(&bytes)?;

                Ok(block)
            }
            None => Err(file_not_found_error()),
        }
    }

    pub fn length(&self, filename: &str) -> Result<usize, FileManagerError> {
        self.cache_file(filename)?;
        let open_files = self
            .open_files
            .lock()
            .map_err(|_| FileManagerError::LockError)?;
        let file = open_files.get(filename);
        match file {
            Some(file) => {
                let metadata = file.metadata()?;
                Ok((metadata.len() / self.blocksize as u64) as usize)
            }
            None => Err(file_not_found_error()),
        }
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> usize {
        self.blocksize
    }

    fn cache_file(&self, filename: &str) -> Result<(), FileManagerError> {
        let mut open_files = self
            .open_files
            .lock()
            .map_err(|_| FileManagerError::LockError)?;
        let contains_key = open_files.contains_key(filename);
        if !contains_key {
            let file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .custom_flags(libc::O_SYNC)
                .open(self.db_directory.join(filename))?;
            open_files.insert(filename.to_string(), file);
        }
        Ok(())
    }
}

fn file_not_found_error() -> FileManagerError {
    FileManagerError::IoError(io::Error::new(io::ErrorKind::NotFound, "File not found"))
}

#[cfg(test)]
mod test_file_manager {
    use super::*;
    use tempfile;

    #[test]
    fn test_is_new() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_owned();

        let file_manager = FileManager::new(&path, 400);
        assert_eq!(file_manager.is_new(), false);

        // これでフォルダが削除される
        drop(dir);

        let file_manager = FileManager::new(&path, 400);
        assert_eq!(file_manager.is_new(), true);
    }

    #[test]
    fn test_block_size() {
        let dir = tempfile::tempdir().unwrap();

        let file_manager = FileManager::new(dir.path(), 400);
        assert_eq!(file_manager.block_size(), 400);
    }

    #[test]
    fn test_read_and_write() {
        let dir = tempfile::tempdir().unwrap();

        let file_manager = FileManager::new(dir.path(), 400);
        let block = BlockId::new("test_file", 0);
        let mut page = Page::new_from_size(400);

        page.set_int(0, 123);
        file_manager.write(&block, &mut page).unwrap();

        let mut read_page = Page::new_from_size(400);
        file_manager.read(&block, &mut read_page).unwrap();
        assert_eq!(read_page.get_int(0), 123);
    }

    #[test]
    fn test_append() {
        let dir = tempfile::tempdir().unwrap();

        let file_manager = FileManager::new(dir.path(), 400);
        assert_eq!(file_manager.length("test_file").unwrap(), 0);

        let block = file_manager.append("test_file").unwrap();
        assert_eq!(block.number(), 0);
        assert_eq!(file_manager.length("test_file").unwrap(), 1);

        let block = file_manager.append("test_file").unwrap();
        assert_eq!(block.number(), 1);
        assert_eq!(file_manager.length("test_file").unwrap(), 2);
    }
}

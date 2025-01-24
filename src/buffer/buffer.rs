use crate::file::{blockid, file_manager, page};
use crate::log::log_manager;

use std::io;
use std::sync::Arc;
use thiserror::Error;

/**
 * block (disk 上のデータ) を page を用いて適切に管理するためのクラス
 *
 * 以下のような機能を持つ:
 * - block の内容を page を通して読み書きする
 * - block が変更されたかどうかの追跡 (log sequence number と transaction number を用いる)
 * - いくつのクライアントがこの buffer を pin しているかの追跡
 */
pub struct Buffer {
    fm: Arc<file_manager::FileManager>,
    lm: Arc<log_manager::LogManager>,
    contents: page::Page,
    block: Option<blockid::BlockId>, // None なら buffer は空
    pins: usize,                     // この buffer を pin してほしいといったクライアントの数
    txnum: Option<u64>,              // transaction の番号。None なら transaction は走っていない
    lsn: Option<u64>,                // この buffer が最後に書き込まれた log sequence number
}

#[derive(Error, Debug)]
pub(crate) enum BufferError {
    #[error("I/O error: {0}")]
    IoError(#[from] io::Error),
    #[error("Error from log manager: {0}")]
    LogError(#[from] log_manager::LogError),
    #[error("Error from file manager: {0}")]
    FileManagerError(#[from] file_manager::FileManagerError),
}

impl Buffer {
    pub fn new(fm: Arc<file_manager::FileManager>, lm: Arc<log_manager::LogManager>) -> Buffer {
        let block_size = fm.block_size();
        Buffer {
            fm: fm,
            lm: lm,
            block: None,
            contents: page::Page::new_from_size(block_size),
            pins: 0,
            txnum: None,
            lsn: None,
        }
    }

    pub fn contents_mut(&mut self) -> &mut page::Page {
        &mut self.contents
    }

    pub fn contents(&self) -> &page::Page {
        &self.contents
    }

    pub fn block(&self) -> Option<&blockid::BlockId> {
        match self.block {
            Some(ref block) => Some(block),
            None => None,
        }
    }

    // 更新を行ったことを記録する
    // update に対して log record を書き込まない場合は lsn が None になる
    pub fn set_modified(&mut self, txnum: u64, lsn: Option<u64>) {
        self.txnum = Some(txnum);
        self.lsn = lsn;
    }

    // buffer を通して block の読み書きをしているクライアントの数を追加する
    pub fn pin(&mut self) {
        self.pins += 1;
    }

    // buffer を通して block の読み書きをしているクライアントの数を減らす
    pub fn unpin(&mut self) {
        self.pins -= 1;
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> Option<u64> {
        self.txnum
    }

    // buffer が参照する block を変更する
    pub(crate) fn assign_to_block(&mut self, block: &blockid::BlockId) -> Result<(), BufferError> {
        self.flush()?;
        self.block = Some(block.clone());
        self.fm.read(&block, &mut self.contents)?;
        self.pins = 0;
        Ok(())
    }

    // buffer が参照する block に対して行われた変更を書き込み、永続性を保証する
    pub(crate) fn flush(&mut self) -> Result<(), log_manager::LogError> {
        if self.block.is_some() && self.txnum.is_some() {
            let lsn = self.lsn.unwrap_or(0);
            self.lm.flush(lsn)?;
            self.fm
                .write(self.block.as_ref().unwrap(), &self.contents)?;
            self.txnum = None;
        }
        Ok(())
    }
}

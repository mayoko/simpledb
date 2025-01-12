use crate::file::{blockid, file_manager, page};
use crate::log::log_manager;

pub struct Buffer<'a> {
    fm: &'a mut file_manager::FileManager,
    lm: &'a mut log_manager::LogManager<'a>,
    contents: page::Page,
    block: Option<blockid::BlockId>, // None なら buffer は空
    pins: usize,                     // この buffer を pin してほしいといったクライアントの数
    txnum: Option<u64>,              // transaction の番号。None なら transaction は走っていない
    lsn: Option<u64>,                // この buffer が最後に書き込まれた log sequence number
}

impl<'a> Buffer<'a> {
    pub fn new(
        fm: &'a mut file_manager::FileManager,
        lm: &'a mut log_manager::LogManager<'a>,
    ) -> Buffer<'a> {
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

    pub fn contents(&mut self) -> &mut page::Page {
        &mut self.contents
    }

    pub fn block(&self) -> Option<&blockid::BlockId> {
        match self.block {
            Some(ref block) => Some(block),
            None => None,
        }
    }

    // update に対して log record を書き込まない場合は lsn が None になる
    pub fn set_modified(&mut self, txnum: u64, lsn: Option<u64>) {
        self.txnum = Some(txnum);
        self.lsn = lsn;
    }

    pub fn pin(&mut self) {
        self.pins += 1;
    }

    pub fn unpin(&mut self) {
        self.pins -= 1;
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> Option<u64> {
        self.txnum
    }

    pub(crate) fn assign_to_block(&mut self, block: &blockid::BlockId) {
        self.flush();
        self.block = Some(block.clone());
        self.fm.read(&block, &mut self.contents);
        self.pins = 0;
    }

    pub(crate) fn flush(&mut self) {
        if self.txnum.is_some() {
            let lsn = self.lsn.unwrap_or(0);
            self.lm.flush(lsn);
            self.txnum = None;
        }
    }
}

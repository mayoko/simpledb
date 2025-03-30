use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::sync::Arc;

use crate::file::blockid::BlockId;

use super::lock_table::{LockTable, LockTableError};

/**
 * 一つの transaction の中で扱われる lock の管理を行い、並行実行制御を行うクラス
 *
 * Transaction の中で管理すれば良いだけなので、LockTable とは異なり、thread 間での lock は考慮しなくて良い
 */
pub struct ConcurrencyManager {
    lock_table: Arc<LockTable>,
    locks: HashMap<BlockId, LockType>,
}

impl ConcurrencyManager {
    pub fn new(lock_table: Arc<LockTable>) -> ConcurrencyManager {
        ConcurrencyManager {
            lock_table,
            locks: HashMap::new(),
        }
    }

    // 共有ロックを取得
    pub fn slock(&mut self, block: &BlockId) -> Result<(), LockTableError> {
        match self.locks.get(block) {
            Some(_) => Ok(()),
            None => {
                // まだ lock を取っていなかったら lock を取って登録
                self.lock_table.slock(block)?;
                self.locks.insert(block.clone(), LockType::Shared);
                Ok(())
            }
        }
    }

    // 排他的ロックを取得
    pub fn xlock(&mut self, block: &BlockId) -> Result<(), LockTableError> {
        let entry = self.locks.entry(block.clone());
        match entry {
            Occupied(occupied) => {
                let value = occupied.into_mut();
                match value {
                    LockType::Shared => {
                        // すでに shared lock が取られていたら exclusive lock に変更
                        self.lock_table.promote_to_xlock(block)?;
                        *value = LockType::Exclusive;
                        Ok(())
                    }
                    LockType::Exclusive => {
                        // すでに exclusive lock が取られていたら何もしない
                        Ok(())
                    }
                }
            }
            Vacant(vacant) => {
                self.lock_table.xlock(block)?;
                vacant.insert(LockType::Exclusive);
                Ok(())
            }
        }
    }

    // 取得していたすべての lock を解放
    pub fn release(&mut self) -> Result<(), LockTableError> {
        for block in self.locks.keys() {
            self.lock_table.unlock(block)?;
        }
        self.locks.clear();
        Ok(())
    }
}

enum LockType {
    Shared,
    Exclusive,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::blockid::BlockId;

    #[test]
    fn test_concurrency_manager() {
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let mut cm1 = ConcurrencyManager::new(lock_table.clone());
        let mut cm2 = ConcurrencyManager::new(lock_table);

        let block = BlockId::new("testfile", 0);

        assert!(cm1.slock(&block).is_ok());
        // まだ cm1 以外に shared lock が取られていないので、cm1 は exclusive lock を取れる
        assert!(cm1.xlock(&block).is_ok());
        // 再び shared lock を取れる (内部としては exclusive lock が取られている)
        assert!(cm1.slock(&block).is_ok());
        assert!(cm1.release().is_ok());

        // release されたので cm2 は shared lock を取れる
        assert!(cm2.slock(&block).is_ok());
        // cm1 と cm2 は別の transaction として扱われるので、cm1 が exclusive lock を取ることはできない
        assert!(cm1.xlock(&block).is_err());
        // release したあとは cm1 が exclusive lock を取れる
        assert!(cm2.release().is_ok());
        assert!(cm1.slock(&block).is_ok());
        assert!(cm1.xlock(&block).is_ok());
    }
}

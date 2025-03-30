use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread::{self, park_timeout};
use std::time;

use dashmap::DashMap;
use thiserror::Error;

use crate::file::blockid::BlockId;

/**
 * ブロックごとの Lock を管理するクラス
 *
 * プログラム全体で一つしかない想定
 */
pub struct LockTable {
    // block ごとの Lock を管理するテーブル
    locks: DashMap<BlockId, Arc<Mutex<Lock>>>,
    // block ごとの、lock を待っている thread のリスト
    // lock の開放を待っている場合、自分の thread をここに入れてから park する
    queues: DashMap<BlockId, Arc<Mutex<VecDeque<thread::Thread>>>>,
    // ロックを取得する最大の時間 (ms)
    max_waiting_time_ms: u64,
}

#[derive(Error, Debug)]
pub enum LockTableError {
    #[error("Failed to acquire lock")]
    Lock(String),
    #[error("timeout error")]
    Timeout(String),
    #[error("lock table general error")]
    General(String),
}

impl LockTable {
    pub fn new(max_waiting_time_ms: Option<u64>) -> LockTable {
        LockTable {
            locks: DashMap::new(),
            queues: DashMap::new(),
            max_waiting_time_ms: match max_waiting_time_ms {
                Some(ms) => ms,
                None => MAX_WAITING_TIME_MS,
            },
        }
    }

    /**
     * 共有ロックを取得する
     */
    pub fn slock(&self, blk: &BlockId) -> Result<(), LockTableError> {
        let start = time::Instant::now();
        // timelimit まで lock 取得を試みる
        while get_waiting_time(start) < self.max_waiting_time_ms {
            // entry method で、特定 block の lock 情報に関する exclusive lock を獲得
            let lock_entry = self.locks.entry(blk.clone());
            let lock_entry_inner =
                lock_entry.or_insert_with(|| Arc::new(Mutex::new(Lock::Shared(0))));
            let mut lock = lock_entry_inner
                .value()
                .lock()
                .map_err(|_| LockTableError::Lock("failed to acquire lock".into()))?;
            match *lock {
                Lock::Shared(ref_count) => {
                    *lock = Lock::Shared(ref_count + 1);
                    return Ok(());
                }
                Lock::Exclusive => {
                    // 他のスレッドが排他ロックを取得している場合は待つ
                    let queue = self.get_or_create_queue(blk);
                    let mut queue = queue.lock().map_err(|_| {
                        LockTableError::Lock(
                            "failed to acquire the lock of waiting queue list".into(),
                        )
                    })?;
                    queue.push_back(thread::current());

                    // 他の thread が lock に触れるよう、dashmap の参照を解放 (これをやらないと unlock する側が値を読めない)
                    drop(queue);
                    drop(lock);
                    drop(lock_entry_inner);

                    // unpark が先に呼び出されても、仕様的に race condition は発生しないらしい
                    park_timeout(time::Duration::from_millis(self.max_waiting_time_ms));
                }
            }
        }
        Err(LockTableError::Timeout(
            "failed to acquire shared lock within the time limit".into(),
        ))
    }

    /**
     * 何も lock を持っていない状態から、占有ロックを取得する
     *
     * Note: 共有ロックを持っている場合は promote_to_xlock を使う。すでに slock を持っている状態でこのメソッドを呼び出すと deadlock する
     */
    pub fn xlock(&self, blk: &BlockId) -> Result<(), LockTableError> {
        let start = time::Instant::now();
        // timelimit まで lock 取得を試みる
        while get_waiting_time(start) < self.max_waiting_time_ms {
            // entry method で、特定 block の lock 情報に関する exclusive lock を獲得
            let lock_entry = self.locks.entry(blk.clone());
            match lock_entry {
                dashmap::mapref::entry::Entry::Occupied(_) => {
                    // 他のスレッドがロックを取得している場合は待つ
                    let queue = self.get_or_create_queue(blk);
                    let mut queue = queue.lock().map_err(|_| {
                        LockTableError::Lock(
                            "failed to acquire the lock of waiting queue list".into(),
                        )
                    })?;
                    queue.push_back(thread::current());

                    // 他の thread が lock に触れるよう、dashmap の参照を解放 (これをやらないと unlock する側が値を読めない)
                    drop(queue);
                    drop(lock_entry);

                    // unpark が先に呼び出されても、仕様的に race condition は発生しないらしい
                    park_timeout(time::Duration::from_millis(self.max_waiting_time_ms));
                }
                dashmap::mapref::entry::Entry::Vacant(_) => {
                    let lock = Arc::new(Mutex::new(Lock::Exclusive));
                    lock_entry.insert(lock);
                    return Ok(());
                }
            }
        }
        Err(LockTableError::Timeout(
            "failed to acquire exclusive lock within the time limit".into(),
        ))
    }

    /**
     * slock を持っていた状態から、xlock を取得する
     *
     * Warning: このメソッドでは、呼び出し元が本当に slock を持っていたのかについては確認していない。正しい状態で呼び出さないと lock の状態が破綻する
     */
    pub fn promote_to_xlock(&self, blk: &BlockId) -> Result<(), LockTableError> {
        let start = time::Instant::now();
        // timelimit まで lock 取得を試みる
        while get_waiting_time(start) < self.max_waiting_time_ms {
            let lock_entry = self.locks.entry(blk.clone());
            match lock_entry {
            dashmap::mapref::entry::Entry::Occupied(lock_entry) => {
                let mut lock = lock_entry.get().lock().map_err(|_| {
                    LockTableError::Lock(format!(
                        "failed to acquire the lock value for blk {:?}",
                        blk.clone()
                    ))
                })?;
                match *lock {
                    Lock::Shared(1) => {
                        *lock = Lock::Exclusive;
                        return Ok(());
                    }
                    Lock::Shared(_) | Lock::Exclusive => {
                        // 他のスレッドが排他ロックを取得している場合は待つ
                        let queue = self.get_or_create_queue(blk);
                        let mut queue = queue.lock().map_err(|_| {
                            LockTableError::Lock(
                                "failed to acquire the lock of waiting queue list".into(),
                            )
                        })?;
                        queue.push_back(thread::current());

                        // 他の thread が lock に触れるよう、dashmap の参照を解放 (これをやらないと unlock する側が値を読めない)
                        drop(queue);
                        drop(lock);

                        // unpark が先に呼び出されても、仕様的に race condition は発生しないらしい
                        park_timeout(time::Duration::from_millis(self.max_waiting_time_ms));
                    }
                }
            }
            dashmap::mapref::entry::Entry::Vacant(_) => return Err(LockTableError::General(
                "promote_to_xlock method must be called after the specified block is shared locked"
                    .into(),
            )),
        }
        }
        Err(LockTableError::Timeout(
            "failed to acquire exclusive lock within the time limit".into(),
        ))
    }

    /**
     * 取得していたロックを解放する
     *
     * 指定されたブロックに対するロックがなかった場合は Err を返す
     */
    pub fn unlock(&self, blk: &BlockId) -> Result<(), LockTableError> {
        let lock_entry = self.locks.entry(blk.clone());
        match lock_entry {
            dashmap::mapref::entry::Entry::Occupied(lock_entry) => {
                let mut lock = lock_entry.get().lock().map_err(|_| {
                    LockTableError::Lock(format!(
                        "failed to unlock the lock value for blk {:?}",
                        blk.clone()
                    ))
                })?;
                let mut should_remove = false;
                match *lock {
                    Lock::Shared(1) | Lock::Exclusive => {
                        should_remove = true;
                    }
                    Lock::Shared(ref_count) => {
                        *lock = Lock::Shared(ref_count - 1);
                    }
                };
                drop(lock);

                if should_remove {
                    lock_entry.remove();
                    let queue_entry = self.queues.entry(blk.clone());
                    match queue_entry {
                        dashmap::mapref::entry::Entry::Occupied(queue_entry) => {
                            let queue_arc = queue_entry.get();
                            let mut queue = queue_arc.lock().map_err(|_| {
                                LockTableError::Lock(
                                    "failed to acquire the lock of waiting queue list".into(),
                                )
                            })?;
                            while let Some(thread) = queue.pop_front() {
                                thread.unpark();
                            }

                            drop(queue);
                            queue_entry.remove();
                        }
                        dashmap::mapref::entry::Entry::Vacant(_) => {
                            // do nothing
                        }
                    }
                }
                Ok(())
            }
            dashmap::mapref::entry::Entry::Vacant(_) => Err(LockTableError::General(
                "unlock method must be called after the specified block is locked".into(),
            )),
        }
    }

    fn get_or_create_queue(&self, blk: &BlockId) -> Arc<Mutex<VecDeque<thread::Thread>>> {
        self.queues
            .entry(blk.clone())
            .or_insert_with(|| Arc::new(Mutex::new(VecDeque::new())))
            .clone()
    }
}

enum Lock {
    Shared(usize),
    Exclusive,
}

// lock を持つ最大の時間 (ms)
const MAX_WAITING_TIME_MS: u64 = 10_000;

fn get_waiting_time(start: time::Instant) -> u64 {
    start.elapsed().as_millis() as u64
}

#[cfg(test)]
mod lock_table_test {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_slock() {
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let blk = Arc::new(BlockId::new("test", 0));

        // thread での slock
        let mut handles = vec![];
        for _ in 0..10 {
            let lock_table_clone = lock_table.clone();
            let blk_clone = blk.clone();
            let handle = thread::spawn(move || {
                lock_table_clone.slock(&blk_clone).unwrap();
                thread::sleep(time::Duration::from_millis(100));
                lock_table_clone.unlock(&blk_clone).unwrap();
            });
            handles.push(handle);
        }

        lock_table.slock(&blk).unwrap();
        lock_table.unlock(&blk).unwrap();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_xlock() {
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let blk0 = Arc::new(BlockId::new("test", 0));
        let blk1 = Arc::new(BlockId::new("test", 1));

        lock_table.xlock(&blk0).unwrap();
        // 2 回目の xlock は失敗する
        assert!(lock_table.xlock(&blk0).is_err());
        // 別のブロックに対して xlock は成功する
        lock_table.xlock(&blk1).unwrap();

        // unlock すると、次の xlock が成功する
        lock_table.unlock(&blk0).unwrap();
        lock_table.xlock(&blk0).unwrap();
    }

    #[test]
    fn test_promote_to_xlock() {
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let blk = Arc::new(BlockId::new("test", 0));

        lock_table.slock(&blk).unwrap();
        // 普通に xlock しようとすると失敗する
        assert!(lock_table.xlock(&blk).is_err());
        // slock から xlock に昇格することはできる
        assert!(lock_table.promote_to_xlock(&blk).is_ok());
    }

    #[test]
    fn test_lock_combination() {
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let blk0 = Arc::new(BlockId::new("test", 0));
        let blk1 = Arc::new(BlockId::new("test", 1));

        // blk0: slock, blk1: xlock
        lock_table.slock(&blk0).unwrap();
        lock_table.xlock(&blk1).unwrap();

        assert!(lock_table.xlock(&blk0).is_err());
        assert!(lock_table.slock(&blk1).is_err());

        // unlock すると、次の xlock が成功する
        // for blk0
        lock_table.unlock(&blk0).unwrap();
        lock_table.xlock(&blk0).unwrap();

        // for blk1
        lock_table.unlock(&blk1).unwrap();
        lock_table.slock(&blk1).unwrap();
    }

    #[test]
    fn test_unlock_notification_for_xlock() {
        // 複数の thread 間で同じ block の lock を取り合った場合、park, unpark が正しく動作することを確認する
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let blk = Arc::new(BlockId::new("test", 0));

        let handle = {
            let lock_table_clone = lock_table.clone();
            let blk_clone = blk.clone();
            thread::spawn(move || {
                lock_table_clone.xlock(&blk_clone).unwrap();
                thread::sleep(time::Duration::from_millis(3));
                lock_table_clone.unlock(&blk_clone).unwrap();
            })
        };

        lock_table.xlock(&blk).unwrap();
        thread::sleep(time::Duration::from_millis(3));
        lock_table.unlock(&blk).unwrap();

        handle.join().unwrap();
    }

    #[test]
    fn test_unlock_notification_for_slock() {
        // 複数の thread 間で同じ block の lock を取り合った場合、park, unpark が正しく動作することを確認する
        let lock_table = Arc::new(LockTable::new(Some(10)));
        let blk = Arc::new(BlockId::new("test", 0));

        let mut handles = vec![];
        // slock する thread をたくさん用意する
        for _ in 0..10 {
            let handle = {
                let lock_table_clone = lock_table.clone();
                let blk_clone = blk.clone();
                thread::spawn(move || {
                    lock_table_clone.slock(&blk_clone).unwrap();
                    thread::sleep(time::Duration::from_millis(3));
                    lock_table_clone.unlock(&blk_clone).unwrap();
                })
            };
            handles.push(handle);
        }

        // main thread では xlock する
        lock_table.xlock(&blk).unwrap();
        thread::sleep(time::Duration::from_millis(3));
        lock_table.unlock(&blk).unwrap();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

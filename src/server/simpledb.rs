use std::cell::RefCell;
use std::sync::Arc;
use std::{path::Path, rc::Rc};

use anyhow::Result as AnyhowResult;

use crate::{
    buffer::buffer_manager::BufferManager,
    exec::executor::Executor,
    file::file_manager::FileManager,
    log::log_manager::LogManager,
    metadata::{
        metadata_manager::{MetadataManager, MetadataManagerImpl},
        table_manager::TableManagerImpl,
    },
    parse::parser_factory::ParserFactory,
    planner::basic_query_planner::BasicQueryPalanner,
    record::table_scan_factory::TableScanFactoryImpl,
    tx::{
        concurrency::lock_table::LockTable,
        transaction::{Transaction, TransactionFactory},
    },
};

pub struct SimpleDB {
    file_manager: Arc<FileManager>,
    log_manager: Arc<LogManager>,
    buffer_manager: Arc<BufferManager>,
    transaction_factory: TransactionFactory,
    metadata_manager: Arc<dyn MetadataManager>,
    executor: Executor,
}

impl SimpleDB {
    const BLOCK_SIZE: usize = 400;
    const BUFFER_SIZE: usize = 8;
    const LOG_FILE: &'static str = "simpledb.log";
    const LOCK_TABLE_MAX_WAITING_TIME_MS: u64 = 100;

    pub fn with_params(dir_name: &str, block_size: usize, buff_size: usize) -> AnyhowResult<Self> {
        let file_manager = Arc::new(FileManager::new(Path::new(dir_name), block_size));
        let log_manager = Arc::new(LogManager::new(file_manager.clone(), SimpleDB::LOG_FILE)?);
        let buffer_manager = Arc::new(BufferManager::new(
            file_manager.clone(),
            log_manager.clone(),
            buff_size,
            None,
        ));
        let table_manager = Arc::new(TableManagerImpl::new(
            Arc::new(TableScanFactoryImpl::new()),
        )?);
        let metadata_manager = Arc::new(MetadataManagerImpl::new(table_manager)?);
        let lock_table = Arc::new(LockTable::new(Some(
            SimpleDB::LOCK_TABLE_MAX_WAITING_TIME_MS,
        )));
        let transaction_factory = TransactionFactory::new(
            file_manager.clone(),
            log_manager.clone(),
            buffer_manager.clone(),
            lock_table,
        );

        let query_planner = BasicQueryPalanner::new(metadata_manager.clone(), ParserFactory::new());
        let executor = Executor::new(
            Box::new(query_planner),
            ParserFactory::new(),
            metadata_manager.clone(),
        );

        Ok(Self {
            file_manager,
            log_manager,
            buffer_manager,
            metadata_manager,
            executor,
            transaction_factory,
        })
    }

    pub fn new(dir_name: &str) -> AnyhowResult<Self> {
        Self::with_params(dir_name, SimpleDB::BLOCK_SIZE, SimpleDB::BUFFER_SIZE)
    }

    pub fn new_tx(&self) -> AnyhowResult<Rc<RefCell<Transaction>>> {
        Ok(Rc::new(RefCell::new(self.transaction_factory.create()?)))
    }

    pub fn metadata_manager(&self) -> Arc<dyn MetadataManager> {
        self.metadata_manager.clone()
    }

    pub fn executor(&self) -> &Executor {
        &self.executor
    }

    pub fn log_manager(&self) -> Arc<LogManager> {
        self.log_manager.clone()
    }

    pub fn buffer_manager(&self) -> Arc<BufferManager> {
        self.buffer_manager.clone()
    }
}

#[cfg(test)]
mod simpledb_integration_test {
    use tempfile::tempdir;

    use super::SimpleDB;

    fn setup(db: &SimpleDB) {
        // table 定義用の transaction
        {
            let tx = db.new_tx().unwrap();
            let executor = db.executor();
            let create_student_table_cmd =
                "create table student (sid int, sname varchar(10), gradyear int, majorid int)";
            executor
                .exec_update_command(create_student_table_cmd, &tx)
                .unwrap();
            let create_department_table_cmd = "create table dept (did int, dname varchar(10))";
            executor
                .exec_update_command(create_department_table_cmd, &tx)
                .unwrap();
            tx.borrow_mut().commit().unwrap();
        }
        // student のデータを追加
        {
            let tx = db.new_tx().unwrap();
            let executor = db.executor();
            let insert_student_cmds = vec![
                "insert into student (sid, sname, gradyear, majorid) values (1, 'joe', 2021, 10)",
                "insert into student (sid, sname, gradyear, majorid) values (2, 'amy', 2020, 20)",
                "insert into student (sid, sname, gradyear, majorid) values (3, 'max', 2022, 10)",
                "insert into student (sid, sname, gradyear, majorid) values (4, 'sue', 2022, 20)",
                "insert into student (sid, sname, gradyear, majorid) values (5, 'bob', 2020, 30)",
                "insert into student (sid, sname, gradyear, majorid) values (6, 'kim', 2020, 20)",
                "insert into student (sid, sname, gradyear, majorid) values (7, 'art', 2021, 30)",
                "insert into student (sid, sname, gradyear, majorid) values (8, 'pat', 2019, 20)",
                "insert into student (sid, sname, gradyear, majorid) values (9, 'lee', 2021, 10)",
            ];
            for insert_student_cmd in insert_student_cmds {
                executor
                    .exec_update_command(insert_student_cmd, &tx)
                    .unwrap();
            }
            tx.borrow_mut().commit().unwrap();
        }
        // department のデータを追加
        {
            let tx = db.new_tx().unwrap();
            let executor = db.executor();
            let insert_student_cmds = vec![
                "insert into dept (did, dname) values (10, 'compsci')",
                "insert into dept (did, dname) values (20, 'math')",
                "insert into dept (did, dname) values (30, 'drama')",
            ];
            for insert_student_cmd in insert_student_cmds {
                executor
                    .exec_update_command(insert_student_cmd, &tx)
                    .unwrap();
            }
            tx.borrow_mut().commit().unwrap();
        }
    }

    #[test]
    fn test_fetching_all_student_data() {
        let dir = tempdir().unwrap();
        let dir_name = dir.path().to_str().unwrap();
        let db = super::SimpleDB::new(dir_name).unwrap();
        setup(&db);

        let tx = db.new_tx().unwrap();
        let executor = db.executor();
        let select_student_cmd = "select sid, sname from student";
        let mut scan = executor.exec_query(select_student_cmd, &tx).unwrap();
        let mut result = Vec::new();
        while scan.move_next().unwrap() {
            let sid: i32 = scan.get_int("sid").unwrap();
            let name: String = scan.get_string("sname").unwrap();
            result.push((sid, name));
        }
        assert_eq!(result.len(), 9);
        let expected_pairs = vec![
            (1, "joe".to_string()),
            (2, "amy".to_string()),
            (3, "max".to_string()),
            (4, "sue".to_string()),
            (5, "bob".to_string()),
            (6, "kim".to_string()),
            (7, "art".to_string()),
            (8, "pat".to_string()),
            (9, "lee".to_string()),
        ];
        for pair in &result {
            assert!(expected_pairs.contains(pair));
        }
        // tx.commit() したあとに scan を drop すると、scan の中で保持している block を unpin しようとして失敗する (commit ですでに unpin されているため)
        drop(scan);
        tx.borrow_mut().commit().unwrap();
    }
    #[test]
    fn test_fetching_student_data_with_condition() {
        let dir = tempdir().unwrap();
        let dir_name = dir.path().to_str().unwrap();
        let db = super::SimpleDB::new(dir_name).unwrap();
        setup(&db);

        let tx = db.new_tx().unwrap();
        let executor = db.executor();
        let select_student_cmd = "select sid, sname from student where gradyear = 2020";
        let mut scan = executor.exec_query(select_student_cmd, &tx).unwrap();
        let mut result = Vec::new();
        while scan.move_next().unwrap() {
            let sid: i32 = scan.get_int("sid").unwrap();
            let name: String = scan.get_string("sname").unwrap();
            result.push((sid, name));
        }
        assert_eq!(result.len(), 3);
        let expected_pairs = [
            (2, "amy".to_string()),
            (5, "bob".to_string()),
            (6, "kim".to_string()),
        ];
        for pair in &result {
            assert!(expected_pairs.contains(pair));
        }
        // tx.commit() したあとに scan を drop すると、scan の中で保持している block を unpin しようとして失敗する (commit ですでに unpin されているため)
        drop(scan);
        tx.borrow_mut().commit().unwrap();
    }

    #[test]
    fn test_fetching_data_joining_student_and_dept_table() {
        let dir = tempdir().unwrap();
        let dir_name = dir.path().to_str().unwrap();
        let db = super::SimpleDB::new(dir_name).unwrap();
        setup(&db);

        let tx = db.new_tx().unwrap();
        let executor = db.executor();
        let select_student_cmd =
            "select sid, sname, dname from student, dept where gradyear = 2020 and majorid = did";
        let mut scan = executor.exec_query(select_student_cmd, &tx).unwrap();
        let mut result = Vec::new();
        while scan.move_next().unwrap() {
            let sid: i32 = scan.get_int("sid").unwrap();
            let name: String = scan.get_string("sname").unwrap();
            let dept_name: String = scan.get_string("dname").unwrap();
            result.push((sid, name, dept_name));
        }
        assert_eq!(result.len(), 3);
        let expected_pairs = [
            (2, "amy".to_string(), "math".to_string()),
            (5, "bob".to_string(), "drama".to_string()),
            (6, "kim".to_string(), "math".to_string()),
        ];
        for pair in &result {
            assert!(expected_pairs.contains(pair));
        }
        // tx.commit() したあとに scan を drop すると、scan の中で保持している block を unpin しようとして失敗する (commit ですでに unpin されているため)
        drop(scan);
        tx.borrow_mut().commit().unwrap();
    }
}

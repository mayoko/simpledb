use std::{cell::RefCell, rc::Rc};

use anyhow::Result as AnyhowResult;

use crate::{
    metadata::metadata_manager::MetadataManager,
    parse::{
        content::{
            create_table_data::CreateTableData, create_view_data::CreateViewData,
            delete_data::DeleteData, insert_data::InsertData, update_data::UpdateData,
        },
        parser::UpdateCommand,
        parser_factory::ParserFactory,
    },
    plan::{plan::Plan, predicate::Predicate, select_plan::SelectPlan, table_plan::TablePlan},
    planner::query_planner::QueryPlanner,
    query::scan::ReadScan,
    tx::transaction::Transaction,
};

pub struct Executor {
    planner: Box<dyn QueryPlanner>,
    parser_factory: ParserFactory,
    metadata_manager: Box<dyn MetadataManager>,
}

impl Executor {
    pub fn new(
        planner: Box<dyn QueryPlanner>,
        parser_factory: ParserFactory,
        metadata_manager: Box<dyn MetadataManager>,
    ) -> Self {
        Self {
            planner,
            parser_factory,
            metadata_manager,
        }
    }
    /// select クエリを実行し、その scan を返す。scan 自体の操作は client が行う必要がある
    pub fn exec_query(
        &self,
        cmd: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<Box<dyn ReadScan>> {
        let mut parser = self.parser_factory.create(cmd.to_string())?;
        let query_data = parser.parse_query()?;
        let plan = self.planner.create_plan(&query_data, tx)?;
        plan.open_read_scan()
    }
    /// create, update, delete などのクエリを実行する。影響を受けたレコードの数を返り値として返す
    pub fn exec_update_command(
        &self,
        cmd: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<u64> {
        let mut parser = self.parser_factory.create(cmd.to_string())?;
        let update_data = parser.parse_update_command()?;
        match update_data {
            UpdateCommand::Insert(insert_data) => self.exec_insert(&insert_data, tx),
            UpdateCommand::Delete(delete_data) => self.exec_delete(&delete_data, tx),
            UpdateCommand::Update(update_data) => self.execute_update(&update_data, tx),
            UpdateCommand::CreateTable(create_table_data) => {
                self.exec_create_table(&create_table_data, tx)
            }
            UpdateCommand::CreateView(create_view_data) => {
                self.exec_create_view(&create_view_data, tx)
            }
            UpdateCommand::CreateIndex(create_index_data) => {
                unimplemented!("create index is not implemented yet")
            }
        }
    }
    fn exec_delete(&self, data: &DeleteData, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<u64> {
        let plan = {
            let plan = TablePlan::new(
                data.get_table().clone(),
                self.metadata_manager.as_ref(),
                tx.clone(),
            )?;
            let plan = SelectPlan::new(
                Box::new(plan),
                Box::new(Predicate::Product(data.get_predicate().clone())),
            );
            Box::new(plan)
        };

        let mut scan = plan.open_update_scan()?;
        let mut update_count = 0;
        while scan.move_next()? {
            scan.delete()?;
            update_count += 1;
        }
        Ok(update_count)
    }
    fn execute_update(
        &self,
        data: &UpdateData,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<u64> {
        let plan = {
            let plan = TablePlan::new(
                data.get_table().clone(),
                self.metadata_manager.as_ref(),
                tx.clone(),
            )?;
            let plan = SelectPlan::new(
                Box::new(plan),
                Box::new(Predicate::Product(data.get_predicate().clone())),
            );
            Box::new(plan)
        };
        let mut scan = plan.open_update_scan()?;
        scan.before_first()?;
        let mut update_count = 0;
        while scan.move_next()? {
            let val = data
                .get_new_value()
                .convert_for_scan()
                .eval(scan.as_ref())?;
            scan.set_val(data.get_field(), &val)?;
            update_count += 1;
        }
        Ok(update_count)
    }
    fn exec_insert(&self, data: &InsertData, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<u64> {
        let plan = TablePlan::new(
            data.get_table().clone(),
            self.metadata_manager.as_ref(),
            tx.clone(),
        )?;
        let mut scan = plan.open_update_scan()?;
        scan.insert()?;
        for (field, val) in data.get_fields().iter().zip(data.get_values().iter()) {
            scan.set_val(field, val)?;
        }
        Ok(1)
    }
    fn exec_create_table(
        &self,
        data: &CreateTableData,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<u64> {
        self.metadata_manager
            .create_table(data.get_table(), data.get_schema().clone(), tx)?;
        Ok(0)
    }
    fn exec_create_view(
        &self,
        data: &CreateViewData,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<u64> {
        self.metadata_manager
            .create_view(data.view_name(), &data.view_def().to_string(), tx)?;
        Ok(0)
    }
}

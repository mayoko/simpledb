use std::{cell::RefCell, collections::HashMap, rc::Rc};

use anyhow::Result as AnyhowResult;

use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
};

use super::{
    stat_info::StatInfo, stat_manager::StatManager, table_manager::TableManager,
    view_manager::ViewManager,
};

pub trait MetadataManager {
    fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<()>;
    fn get_layout(&self, table_name: &str, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<Layout>;

    fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<()>;
    fn get_view_def(&self, view_name: &str, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<String>;

    fn get_table_stat(
        &self,
        table_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<HashMap<String, StatInfo>>;
}

pub struct MetadataManagerImpl {
    table_manager: Box<dyn TableManager>,
    view_manager: Box<dyn ViewManager>,
    stat_manager: Box<dyn StatManager>,
}

impl MetadataManager for MetadataManagerImpl {
    fn create_table(
        &self,
        table_name: &str,
        schema: Schema,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<()> {
        Ok(self.table_manager.create_table(table_name, schema, tx)?)
    }

    fn get_layout(&self, table_name: &str, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<Layout> {
        Ok(self.table_manager.get_layout(table_name, tx)?)
    }

    fn create_view(
        &self,
        view_name: &str,
        view_def: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<()> {
        Ok(self.view_manager.create_view(view_name, view_def, tx)?)
    }

    fn get_view_def(&self, view_name: &str, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<String> {
        Ok(self.view_manager.get_view_def(view_name, tx)?)
    }

    fn get_table_stat(
        &self,
        table_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<HashMap<String, StatInfo>> {
        self.stat_manager.get_table_stat(table_name, tx)
    }
}

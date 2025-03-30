use std::{cell::RefCell, collections::HashMap, rc::Rc, sync::Arc};

use anyhow::Result as AnyhowResult;

use crate::{
    record::{layout::Layout, schema::Schema, table_scan_factory::TableScanFactoryImpl},
    tx::transaction::Transaction,
};

use super::{
    stat_info::StatInfo, stat_manager::StatManagerFactory, table_manager::TableManager,
    view_manager::ViewManagerFactory,
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
    table_manager: Arc<dyn TableManager>,
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
        let view_manager = ViewManagerFactory::create(
            self.table_manager.as_ref(),
            Box::new(TableScanFactoryImpl::new()),
        );
        Ok(view_manager.create_view(view_name, view_def, tx)?)
    }

    fn get_view_def(&self, view_name: &str, tx: &Rc<RefCell<Transaction>>) -> AnyhowResult<String> {
        let view_manager = ViewManagerFactory::create(
            self.table_manager.as_ref(),
            Box::new(TableScanFactoryImpl::new()),
        );
        Ok(view_manager.get_view_def(view_name, tx)?)
    }

    fn get_table_stat(
        &self,
        table_name: &str,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<HashMap<String, StatInfo>> {
        let stat_manager = StatManagerFactory::create(
            self.table_manager.as_ref(),
            Box::new(TableScanFactoryImpl::new()),
        );
        stat_manager.get_table_stat(table_name, tx)
    }
}

impl MetadataManagerImpl {
    pub fn new(table_manager: Arc<dyn TableManager>) -> AnyhowResult<Self> {
        Ok(Self { table_manager })
    }
}

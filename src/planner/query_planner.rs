use std::{cell::RefCell, rc::Rc};

use anyhow::Result as AnyhowResult;

use crate::{
    parse::content::query_data::QueryData, plan::plan::Plan, tx::transaction::Transaction,
};

pub trait QueryPlanner {
    fn create_plan(
        &self,
        data: &QueryData,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<Box<dyn Plan>>;
}

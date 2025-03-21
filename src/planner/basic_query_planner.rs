use std::{cell::RefCell, rc::Rc};

use anyhow::Result as AnyhowResult;

use crate::{
    metadata::metadata_manager::MetadataManager,
    parse::{content::query_data::QueryData, parser_factory::ParserFactory},
    plan::{
        plan::Plan, predicate::Predicate, product_plan::ProductPlan, project_plan::ProjectPlan,
        select_plan::SelectPlan, table_plan::TablePlan,
    },
    tx::transaction::Transaction,
};

use super::query_planner::QueryPlanner;

pub struct BasicQueryPalanner {
    mdm: Box<dyn MetadataManager>,
    parser_factory: ParserFactory,
}

impl QueryPlanner for BasicQueryPalanner {
    fn create_plan(
        &self,
        data: &QueryData,
        tx: &Rc<RefCell<Transaction>>,
    ) -> AnyhowResult<Box<dyn Plan>> {
        let mut plans = {
            // Step 1: product でまとめる前に table の集合として plan の集合を取得 (view がある場合は、それが一つのテーブルとみなされている)
            let mut plans = vec![];
            for table in data.get_tables() {
                if let Ok(view_def) = self.mdm.get_view_def(table, tx) {
                    let mut parser = self.parser_factory.create(view_def)?;
                    let view_data = parser.parse_query()?;
                    plans.push(self.create_plan(&view_data, tx)?);
                } else {
                    plans.push(Box::new(TablePlan::new(
                        table.clone(),
                        self.mdm.as_ref(),
                        tx.clone(),
                    )?) as Box<dyn Plan>);
                }
            }
            plans
        };
        // Step 2: product でまとめる
        let mut plan = {
            let mut plan = plans.remove(0);
            for p in plans {
                plan = Box::new(ProductPlan::new(plan, p)?) as Box<dyn Plan>;
            }
            plan
        };
        // Step 3: predicate を適用
        plan = Box::new(SelectPlan::new(
            plan,
            Box::new(Predicate::Product(data.get_predicate().clone())),
        )) as Box<dyn Plan>;
        // Step 4: projection を適用
        plan = Box::new(ProjectPlan::new(plan, data.get_fields().clone())?) as Box<dyn Plan>;

        Ok(plan)
    }
}

impl BasicQueryPalanner {
    pub fn new(mdm: Box<dyn MetadataManager>, parser_factory: ParserFactory) -> Self {
        BasicQueryPalanner {
            mdm,
            parser_factory,
        }
    }
}

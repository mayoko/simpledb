use crate::plan::predicate::ProductPredicate;

pub struct DeleteData {
    table: String,
    predicate: ProductPredicate,
}

impl DeleteData {
    pub fn new(table: String, predicate: ProductPredicate) -> Self {
        Self { table, predicate }
    }
    pub fn get_table(&self) -> &String {
        &self.table
    }
    pub fn get_predicate(&self) -> &ProductPredicate {
        &self.predicate
    }
}

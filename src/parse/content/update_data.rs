use crate::plan::{expression::Expression, predicate::ProductPredicate};

pub struct UpdateData {
    table: String,
    field: String,
    new_value: Expression,
    predicate: ProductPredicate,
}

impl UpdateData {
    pub fn new(
        table: String,
        field: String,
        new_value: Expression,
        predicate: ProductPredicate,
    ) -> Self {
        Self {
            table,
            field,
            new_value,
            predicate,
        }
    }
    pub fn get_table(&self) -> &String {
        &self.table
    }
    pub fn get_field(&self) -> &String {
        &self.field
    }
    pub fn get_new_value(&self) -> &Expression {
        &self.new_value
    }
    pub fn get_predicate(&self) -> &ProductPredicate {
        &self.predicate
    }
}

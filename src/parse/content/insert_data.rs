use crate::query::constant::Constant;

pub struct InsertData {
    table: String,
    fields: Vec<String>,
    values: Vec<Constant>,
}

impl InsertData {
    pub fn new(table: String, fields: Vec<String>, values: Vec<Constant>) -> Self {
        Self {
            table,
            fields,
            values,
        }
    }
    pub fn get_table(&self) -> &String {
        &self.table
    }
    pub fn get_fields(&self) -> &Vec<String> {
        &self.fields
    }
    pub fn get_values(&self) -> &Vec<Constant> {
        &self.values
    }
}

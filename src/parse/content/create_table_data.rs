use crate::record::schema::Schema;

pub struct CreateTableData {
    table: String,
    schema: Schema,
}

impl CreateTableData {
    pub fn new(table: String, schema: Schema) -> Self {
        Self { table, schema }
    }
    pub fn get_table(&self) -> &String {
        &self.table
    }
    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

pub struct CreateIndexData {
    index_name: String,
    table_name: String,
    field_name: String,
}

impl CreateIndexData {
    pub fn new(index_name: String, table_name: String, field_name: String) -> Self {
        CreateIndexData {
            index_name,
            table_name,
            field_name,
        }
    }

    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }
}

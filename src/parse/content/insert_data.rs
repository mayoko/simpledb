use crate::query::constant::Constant;

/**
 * insert 文の parse 結果を保持する構造体
 * この時点では fields と values の対応関係は気にしていない。特に、fields と values の数が異なる場合にもエラーとしていない。
 */
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

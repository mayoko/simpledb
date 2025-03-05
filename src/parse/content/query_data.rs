use std::fmt;

use crate::query::predicate::ProductPredicate;

pub struct QueryData {
    fields: Vec<String>,
    tables: Vec<String>,
    predicate: ProductPredicate,
}

impl QueryData {
    pub fn new(fields: Vec<String>, tables: Vec<String>, predicate: ProductPredicate) -> Self {
        Self {
            fields,
            tables,
            predicate,
        }
    }
    pub fn get_fields(&self) -> &Vec<String> {
        &self.fields
    }
    pub fn get_tables(&self) -> &Vec<String> {
        &self.tables
    }
    pub fn get_predicate(&self) -> &ProductPredicate {
        &self.predicate
    }
}

impl fmt::Display for QueryData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut query = "select ".to_string();
        for (i, field) in self.fields.iter().enumerate() {
            query += field;
            if i != self.fields.len() - 1 {
                query += ", ";
            }
        }
        query += " from ";
        for (i, table) in self.tables.iter().enumerate() {
            query += table;
            if i != self.tables.len() - 1 {
                query += ", ";
            }
        }
        let predicate_string = self.predicate.to_string();
        if !predicate_string.is_empty() {
            query += " where ";
            query += &predicate_string
        }
        write!(f, "{}", query)
    }
}

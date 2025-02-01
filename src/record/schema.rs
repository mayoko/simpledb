use std::collections::HashMap;

use thiserror::Error;

/**
 * table のそれぞれの record いどのようなデータ型を持っているかを示す構造体
 */
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

#[derive(Error, Debug)]
pub(crate) enum SchemaError {
    #[error("invalid call error: {0}")]
    InvalidCallError(String),
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    // schema に field を追加する
    pub fn add_field(&mut self, field_name: &str, field_info: FieldInfo) {
        self.fields.push(field_name.into());
        self.info.insert(field_name.into(), field_info);
    }

    // schema の特定の field を追加する
    pub fn add(&mut self, field_name: &str, schema: &Schema) -> Result<(), SchemaError> {
        match schema.info(field_name) {
            Some(info) => {
                self.add_field(field_name, info);
                Ok(())
            }
            None => Err(SchemaError::InvalidCallError(format!(
                "field {} not found",
                field_name
            ))),
        }
    }

    // schema に指定したものをすべて追加する
    pub fn add_all(&mut self, schema: &Schema) -> Result<(), SchemaError> {
        for field in &schema.fields {
            self.add(field, schema)?;
        }
        Ok(())
    }

    pub fn fields(&self) -> Vec<String> {
        self.fields.clone()
    }

    pub fn has_field(&self, field_name: &str) -> bool {
        self.info.contains_key(field_name)
    }

    pub fn info(&self, field_name: &str) -> Option<FieldInfo> {
        self.info.get(field_name).copied()
    }
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum FieldInfo {
    Integer,
    String(usize),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum FieldType {
    Integer = 0,
    String = 1,
}

#[derive(Error, Debug)]
pub(crate) enum FieldTypeError {
    #[error("invalid call error: {0}")]
    InvalidCall(String),
}

impl FieldInfo {
    pub fn get_type(&self) -> FieldType {
        match self {
            FieldInfo::Integer => FieldType::Integer,
            FieldInfo::String(_) => FieldType::String,
        }
    }
}

impl FieldType {
    pub fn from_i32(value: i32) -> Result<FieldType, FieldTypeError> {
        match value {
            0 => Ok(FieldType::Integer),
            1 => Ok(FieldType::String),
            _ => Err(FieldTypeError::InvalidCall(format!(
                "invalid value: {}",
                value
            ))),
        }
    }
}

#[cfg(test)]
mod schema_test {
    use super::*;

    #[test]
    fn test_schema() {
        let mut schema = Schema::new();
        schema.add_field("a", FieldInfo::Integer);
        schema.add_field("b", FieldInfo::String(10));

        let mut schema2 = Schema::new();
        schema2.add_field("c", FieldInfo::Integer);
        schema2.add_field("d", FieldInfo::String(20));

        schema.add_all(&schema2).unwrap();

        for field in ["a", "b", "c", "d"] {
            assert!(schema.has_field(field));
        }
        assert_eq!(schema.fields(), vec!["a", "b", "c", "d"]);
        assert_eq!(schema.info("a"), Some(FieldInfo::Integer));
        assert_eq!(schema.info("b"), Some(FieldInfo::String(10)));
        assert_eq!(schema.info("c"), Some(FieldInfo::Integer));
        assert_eq!(schema.info("d"), Some(FieldInfo::String(20)));
    }
}

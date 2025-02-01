use std::collections::HashMap;

use thiserror::Error;

use crate::{constants::INTEGER_BYTE_LEN, file::page::Page};

use super::schema::{FieldInfo, Schema};

/**
 * table のレコードがどのように保存されているのかを示す構造体
 */
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct Layout {
    schema: Schema,
    // 各 field が record 開始位置からどれだけ離れた位置からデータを保存し始めているかを示す
    offsets: HashMap<String, usize>,
    // 1 つの record が何バイトで保存されているかを示す
    slot_size: usize,
}

#[derive(Error, Debug)]
pub(crate) enum LayoutError {
    #[error("invalid call error: {0}")]
    InvalidCallError(String),
}

impl Layout {
    pub fn new(schema: Schema) -> Result<Layout, LayoutError> {
        let mut offsets = HashMap::new();
        let mut pos = INTEGER_BYTE_LEN;
        for field in &schema.fields() {
            offsets.insert(field.clone(), pos);
            match Self::length_in_bytes(&schema, field) {
                Some(len) => pos += len,
                None => {
                    return Err(LayoutError::InvalidCallError(format!(
                        "field {} not found",
                        field
                    )))
                }
            }
        }
        Ok(Layout {
            schema,
            offsets,
            slot_size: pos,
        })
    }

    pub fn new_from_existing_settings(
        schema: Schema,
        offsets: HashMap<String, usize>,
        slot_size: usize,
    ) -> Layout {
        Layout {
            schema,
            offsets,
            slot_size,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn offset(&self, field_name: &str) -> Option<usize> {
        self.offsets.get(field_name).copied()
    }

    pub fn slot_size(&self) -> usize {
        self.slot_size
    }

    fn length_in_bytes(schema: &Schema, field_name: &str) -> Option<usize> {
        match schema.info(field_name) {
            Some(FieldInfo::Integer) => Some(INTEGER_BYTE_LEN),
            Some(FieldInfo::String(size)) => Some(Page::max_length(size)),
            None => None,
        }
    }
}

#[cfg(test)]
mod layout_test {
    use super::*;

    #[test]
    fn test_layout() {
        let mut schema = Schema::new();
        schema.add_field("id", FieldInfo::Integer);
        schema.add_field("name", FieldInfo::String(10));

        let layout = Layout::new(schema).unwrap();
        assert_eq!(layout.slot_size(), 4 + 4 + 4 + 40);
        assert_eq!(layout.offset("id"), Some(4));
        assert_eq!(layout.offset("name"), Some(8));
    }
}

use crate::record::rid::Rid;

use super::constant::Constant;

use anyhow::Result as AnyhowResult;
use mockall::{automock, mock};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ReadScanError {
    #[error("[read scan] internal error : {0}")]
    Internal(String),
    #[error("[read scan] invalid call : {0}")]
    InvalidCall(String),
}

#[automock]
pub trait ReadScan {
    /// table scan の cursor を先頭に移動する
    fn before_first(&mut self) -> AnyhowResult<()>;

    /// record の存在する、次の slot に移動する。record が存在しない場合は false を返す
    fn move_next(&mut self) -> AnyhowResult<bool>;

    /// 今いる slot に対して、指定した field の値を取得する
    /// field が存在しない場合は error を返す
    fn get_val(&self, field_name: &str) -> AnyhowResult<Constant>;
    fn get_int(&self, field_name: &str) -> AnyhowResult<i32> {
        Ok(match self.get_val(field_name)? {
            Constant::Int(val) => Ok(val),
            _ => Err(ReadScanError::InvalidCall(format!(
                "field type mismatch: {}. expected int",
                field_name
            ))),
        }?)
    }
    fn get_string(&self, field_name: &str) -> AnyhowResult<String> {
        Ok(match self.get_val(field_name)? {
            Constant::String(val) => Ok(val),
            _ => Err(ReadScanError::InvalidCall(format!(
                "field type mismatch: {}. expected string",
                field_name
            ))),
        }?)
    }

    fn has_field(&self, field_name: &str) -> bool;
}

#[derive(Error, Debug)]
pub enum UpdateScanError {
    #[error("[update scan] internal error : {0}")]
    Internal(String),
    #[error("[update scan] internal error : {0}")]
    InvalidCall(String),
}

pub trait UpdateScan: ReadScan {
    fn set_val(&self, field_name: &str, val: &Constant) -> AnyhowResult<()>;
    fn set_int(&self, field_name: &str, val: i32) -> AnyhowResult<()> {
        self.set_val(field_name, &Constant::Int(val))
    }
    fn set_string(&self, field_name: &str, val: &str) -> AnyhowResult<()> {
        self.set_val(field_name, &Constant::String(val.to_string()))
    }
    /// 新しい record を挿入するために、現在の slot 位置から移動を行う
    fn insert(&mut self) -> AnyhowResult<()>;
    /// 現在 cursor が指している record を削除する
    fn delete(&mut self) -> AnyhowResult<()>;

    /// 指定の record id の示す箇所に cursor を移動する
    fn move_to_rid(&mut self, rid: &Rid) -> AnyhowResult<()>;
    /// cursor が指している record id を取得する
    fn get_rid(&self) -> AnyhowResult<Rid>;
}

pub enum Scan {
    ReadOnly(Box<dyn ReadScan>),
    Updatable(Box<dyn UpdateScan>),
}

mock! {
    pub UpdateScan {}
    impl ReadScan for UpdateScan {
        fn before_first(&mut self) -> AnyhowResult<()>;
        fn move_next(&mut self) -> AnyhowResult<bool>;
        fn get_val(&self, field_name: &str) -> AnyhowResult<Constant>;
        fn get_int(&self, field_name: &str) -> AnyhowResult<i32>;
        fn get_string(&self, field_name: &str) -> AnyhowResult<String>;
        fn has_field(&self, field_name: &str) -> bool;
    }
    impl UpdateScan for UpdateScan {
        fn set_val(&self, field_name: &str, val: &Constant) -> AnyhowResult<()>;
        fn set_int(&self, field_name: &str, val: i32) -> AnyhowResult<()>;
        fn set_string(&self, field_name: &str, val: &str) -> AnyhowResult<()>;
        fn insert(&mut self) -> AnyhowResult<()>;
        fn delete(&mut self) -> AnyhowResult<()>;
        fn move_to_rid(&mut self, rid: &Rid) -> AnyhowResult<()>;
        fn get_rid(&self) -> AnyhowResult<Rid>;
    }
}

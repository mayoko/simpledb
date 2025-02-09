use mockall::automock;

use super::constant::Constant;
use crate::record::rid::Rid;
use std::fmt;

#[derive(Debug)]
pub enum ErrorKind {
    Internal,
    InvalidCall,
}

#[derive(Debug)]
pub struct UpdateScanError {
    kind: ErrorKind,
    source: Box<dyn std::error::Error + Send + Sync>,
}

#[automock]
pub trait UpdateScan {
    fn set_val(&self, field_name: &str, val: &Constant) -> Result<(), UpdateScanError>;
    fn set_int(&self, field_name: &str, val: i32) -> Result<(), UpdateScanError>;
    fn set_string(&self, field_name: &str, val: &str) -> Result<(), UpdateScanError>;
    /// 新しい record を挿入するために、現在の slot 位置から移動を行う
    fn insert(&mut self) -> Result<(), UpdateScanError>;
    /// 現在 cursor が指している record を削除する
    fn delete(&mut self) -> Result<(), UpdateScanError>;

    /// 指定の record id の示す箇所に cursor を移動する
    fn move_to_rid(&mut self, rid: &Rid);
    /// cursor が指している record id を取得する
    fn get_rid(&self) -> Rid;
}

impl UpdateScanError {
    pub fn new(kind: ErrorKind, source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        UpdateScanError { kind, source }
    }
}

impl fmt::Display for UpdateScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UpdateScanError. kind: {:?}, source: {:?}",
            self.kind, self.source
        )
    }
}

impl std::error::Error for UpdateScanError {}

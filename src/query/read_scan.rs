use super::constant::Constant;
use std::fmt;

#[derive(Debug)]
pub enum ErrorKind {
    Internal,
    InvalidCall,
}

#[derive(Debug)]
pub struct ReadScanError {
    kind: ErrorKind,
    source: Box<dyn std::error::Error + Send + Sync>,
}

pub trait ReadScan {
    /// table scan の cursor を先頭に移動する
    fn before_first(&mut self) -> Result<(), ReadScanError>;

    /// record の存在する、次の slot に移動する。record が存在しない場合は false を返す
    fn move_next(&mut self) -> Result<bool, ReadScanError>;

    /// 今いる slot に対して、指定した field の値を取得する
    /// field が存在しない場合は error を返す
    fn get_val(&self, field_name: &str) -> Result<Constant, ReadScanError>;
    fn get_int(&self, field_name: &str) -> Result<i32, ReadScanError>;
    fn get_string(&self, field_name: &str) -> Result<String, ReadScanError>;

    fn has_field(&self, field_name: &str) -> bool;
}

impl ReadScanError {
    pub fn new(kind: ErrorKind, source: Box<dyn std::error::Error + Send + Sync>) -> Self {
        ReadScanError { kind, source }
    }
}

impl fmt::Display for ReadScanError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ReadScanError. kind: {:?}, source: {:?}",
            self.kind, self.source
        )
    }
}

impl std::error::Error for ReadScanError {}

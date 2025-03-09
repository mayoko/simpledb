use anyhow::Result as AnyhowResult;
use mockall::automock;
use thiserror::Error;

use crate::{
    query::scan::{ReadScan, UpdateScan},
    record::schema::Schema,
};

#[derive(Error, Debug)]
pub enum PlanError {
    #[error("[plan] internal error : {0}")]
    Internal(String),
    #[error("[plan] invalid call : {0}")]
    InvalidCall(String),
}

/**
 * SQL の query tree の cost を計算するオブジェクトが実装する trait
 * Scan と対応関係を持つので、Scan の実装により cost が変わった場合には、こちらの cost 見積もりも変更する必要がある可能性がある
 */
#[automock]
pub trait Plan {
    /// Plan から ReadScan オブジェクトを作成する
    fn open_read_scan(&self) -> AnyhowResult<Box<dyn ReadScan>>;
    /// Plan から UpdateScan オブジェクトを作成する
    /// Update できない Plan について作成しようとした場合は InvalidCall error が返される
    fn open_update_scan(&self) -> AnyhowResult<Box<dyn UpdateScan>>;
    /// block にアクセスする回数の見積もりを返す
    fn get_block_access_cost(&self) -> AnyhowResult<u64>;
    /// record の数の見積もりを返す
    fn get_record_access_cost(&self) -> AnyhowResult<u64>;
    /// field の distinct value の見積もりを返す
    fn get_distinct_value_estimation(&self, field_name: &str) -> AnyhowResult<u64>;
    /// Plan が持つ schema を返す
    fn get_schema(&self) -> &Schema;
}

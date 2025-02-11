/**
 * Table のそれぞれのカラムに対する統計情報を保持するための構造体
 *
 * 実装の都合上、必ずしも正確な値が返されるわけではないことに注意
 */
#[derive(Debug, Clone, Copy)]
pub struct StatInfo {
    num_blocks: u64,
    num_records: u64,
    num_distinct_values: u64,
}

impl StatInfo {
    pub fn new(num_blocks: u64, num_records: u64, num_distinct_values: u64) -> Self {
        Self {
            num_blocks,
            num_records,
            num_distinct_values,
        }
    }

    /// table の保持する block 数を返す
    pub fn get_num_blocks(&self) -> u64 {
        self.num_blocks
    }

    /// table の保持する record 数を返す
    pub fn get_num_records(&self) -> u64 {
        self.num_records
    }

    /// カラムのユニークな値の数を返す
    pub fn get_num_distinct_values(&self) -> u64 {
        self.num_distinct_values
    }
}

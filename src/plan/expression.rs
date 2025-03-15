use crate::query::constant::Constant;

/**
 * Select の where 句で用いられる条件で、A=B などの比較における A または B を表す
 * 同じ名前の struct が query 以下のパッケージにも存在するが、こちらは実行計画を立てるうえで使うことを意図されている
 */
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum Expression {
    Constant(Constant),
    Field(String),
}

use crate::plan::plan::Plan;

use super::reduction_factor::ReductionFactor;

use anyhow::Result as AnyhowResult;
use mockall::automock;

#[automock]
pub trait Plannable {
    // この predicate が満たされるときに、どれだけ scan の結果が絞られるかの推定値を返す
    fn reduction_factor(&self, plan: &dyn Plan) -> AnyhowResult<ReductionFactor>;
}

use std::ops::{Mul, MulAssign};

/**
 * 条件 (Term や Predicate) が満たされるときに、どれだけ scan の結果が絞られるかを返す
 * 値が大きいほど scan の結果が絞られる
 */
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ReductionFactor {
    // この term が満たされるときに、scan の結果が 1/n に絞られる
    Constant(f64),
    // この term が満たされる scan の結果が存在しない
    Infinity(),
}

impl Mul for ReductionFactor {
    type Output = Self;

    fn mul(self, rhs: Self) -> Self {
        match (self, rhs) {
            (ReductionFactor::Constant(lhs), ReductionFactor::Constant(rhs)) => {
                ReductionFactor::Constant(lhs * rhs)
            }
            _ => ReductionFactor::Infinity(),
        }
    }
}

impl MulAssign for ReductionFactor {
    fn mul_assign(&mut self, rhs: Self) {
        *self = *self * rhs;
    }
}

impl PartialOrd for ReductionFactor {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (ReductionFactor::Constant(lhs), ReductionFactor::Constant(rhs)) => {
                lhs.partial_cmp(rhs)
            }
            (ReductionFactor::Infinity(), ReductionFactor::Infinity()) => {
                Some(std::cmp::Ordering::Equal)
            }
            (ReductionFactor::Infinity(), _) => Some(std::cmp::Ordering::Greater),
            (_, ReductionFactor::Infinity()) => Some(std::cmp::Ordering::Less),
        }
    }
}

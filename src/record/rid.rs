/**
 * Record の識別子を表す構造体
 */
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rid {
    blk_num: usize,
    slot: Option<usize>,
}

impl Rid {
    pub fn new(blk_num: usize, slot: Option<usize>) -> Self {
        Rid { blk_num, slot }
    }

    pub fn block_number(&self) -> usize {
        self.blk_num
    }

    pub fn slot(&self) -> Option<usize> {
        self.slot
    }
}

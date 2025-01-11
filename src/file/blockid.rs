use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BlockId {
    filename: String,
    blknum: usize,
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.blknum)
    }
}

impl BlockId {
    pub fn new(filename: &str, blknum: usize) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            blknum: blknum,
        }
    }

    pub fn file_name(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> usize {
        self.blknum
    }
}

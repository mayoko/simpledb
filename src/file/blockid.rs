use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BlockId {
    filename: String,
    blknum: BlockNumber,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum BlockNumber {
    Number(usize),
    EndOfFile,
}

impl Display for BlockId {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let blknum = match self.blknum {
            BlockNumber::Number(n) => n.to_string(),
            BlockNumber::EndOfFile => "end of file".to_string(),
        };
        write!(f, "[file {}, block {}]", self.filename, blknum)
    }
}

impl BlockId {
    pub fn new(filename: &str, blknum: usize) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            blknum: BlockNumber::Number(blknum),
        }
    }

    pub fn new_end_of_file(filename: &str) -> BlockId {
        BlockId {
            filename: filename.to_string(),
            blknum: BlockNumber::EndOfFile,
        }
    }

    pub fn file_name(&self) -> &str {
        &self.filename
    }

    pub fn number(&self) -> usize {
        match self.blknum {
            BlockNumber::Number(n) => n,
            BlockNumber::EndOfFile => {
                eprintln!("Warning: BlockId's number method is not expected to be called on end of file block. return 0");
                0
            }
        }
    }
}

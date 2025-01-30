#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl Constant {
    pub fn as_int(&self) -> Option<i32> {
        match self {
            Constant::Int(val) => Some(*val),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            Constant::String(val) => Some(val),
            _ => None,
        }
    }
}

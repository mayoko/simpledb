use std::fmt;

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

impl fmt::Display for Constant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Constant::Int(val) => write!(f, "{}", val),
            Constant::String(val) => write!(f, "'{}'", val),
        }
    }
}

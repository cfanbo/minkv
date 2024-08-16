use std::fmt;

#[derive(Debug)]
pub enum OpError {
    KeyNotFound,
    ReadSizeNotMatch,
    ValueInvalid,
    LockFailed
}

impl fmt::Display for OpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

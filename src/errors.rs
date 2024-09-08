/// Error types for Redis implementation.

#[derive(Debug)]
pub(crate) enum RedisError {
    RespParseError(RespError),
    IOError(std::io::Error),
    UnknownRequest(String),
    UnexpectedNumberOfArgs(String),
    UnexpectedArgumentType(String),
}

#[derive(Debug)]
pub(crate) enum RespError {
    UnexpectedEnd,
    UnknownStartingByte(u8),
    BadBulkStringSize(i64),
    BadArraySize(i64),
    IOError(std::io::Error),
    IntParseFailure(std::num::ParseIntError),
    StringParseFailure(std::str::Utf8Error),
}

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisError::RespParseError(inner) => write!(f, "RESP parsing error {:?}", inner),
            RedisError::IOError(inner) => write!(f, "IOError {:?}", inner),
            RedisError::UnknownRequest(val) => write!(f, "Unknown request: {:?}", val),
            RedisError::UnexpectedNumberOfArgs(val) => {
                write!(f, "Unexpected number of arguments: {}", val)
            }
            RedisError::UnexpectedArgumentType(val) => {
                write!(f, "Unexpected argument type: {}", val)
            }
        }
    }
}

impl std::error::Error for RedisError {}

impl From<std::io::Error> for RedisError {
    fn from(from: std::io::Error) -> Self {
        RedisError::IOError(from)
    }
}

impl From<RespError> for RedisError {
    fn from(from: RespError) -> Self {
        RedisError::RespParseError(from)
    }
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::UnexpectedEnd => write!(f, "Unexpected end of input stream"),
            RespError::UnknownStartingByte(byte) => write!(f, "Unexpected starting byte {}", byte),
            RespError::IOError(io_err) => io_err.fmt(f),
            RespError::IntParseFailure(e) => e.fmt(f),
            RespError::StringParseFailure(e) => e.fmt(f),
            RespError::BadBulkStringSize(sz) => write!(f, "Invalid size for BulkString {}", sz),
            RespError::BadArraySize(sz) => write!(f, "Invalid size for Array {}", sz),
        }
    }
}

impl std::error::Error for RespError {}

impl From<std::num::ParseIntError> for RespError {
    fn from(from: std::num::ParseIntError) -> Self {
        RespError::IntParseFailure(from)
    }
}

impl From<std::str::Utf8Error> for RespError {
    fn from(from: std::str::Utf8Error) -> Self {
        RespError::StringParseFailure(from)
    }
}

impl From<std::io::Error> for RespError {
    fn from(from: std::io::Error) -> Self {
        RespError::IOError(from)
    }
}


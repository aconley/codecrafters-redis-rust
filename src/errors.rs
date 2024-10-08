/// Error types for Redis implementation.

/// Errors encountered while handling redis requests.
#[derive(Debug)]
pub(crate) enum RedisError {
    RespParseError(RespError),
    IOError(std::io::Error),
    UnknownRequest(String),
    UnexpectedNumberOfArgs(String),
    UnexpectedArgumentType(String),
    RdbParserError(RdbFileError),
}

/// Errors encountered while parsing RESP values.
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

/// Errors encountered while parsing Rdb files.
#[derive(Debug)]
pub(crate) enum RdbFileError {
    UnknownStartingByte(u8),
    UnexpectedByte { expected: String, actual: u8 },
    NotRedisFile,
    InvalidFile(String),
    IOError(std::io::Error),
    Unimplemented(String),
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
            RedisError::RdbParserError(inner) => inner.fmt(f),
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

impl From<RdbFileError> for RedisError {
    fn from(from: RdbFileError) -> Self {
        RedisError::RdbParserError(from)
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

impl std::fmt::Display for RdbFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbFileError::IOError(inner) => write!(f, "IOError {:?}", inner),
            RdbFileError::UnknownStartingByte(byte) => {
                write!(f, "Unexpected starting byte {:#04x}", byte)
            }
            RdbFileError::UnexpectedByte { expected, actual } => {
                write!(
                    f,
                    "Unexpected byte; expected {} got {:#04x}",
                    expected, actual
                )
            }
            RdbFileError::NotRedisFile => write!(f, "Input was not a redis file"),
            RdbFileError::InvalidFile(inner) => write!(f, "Invalid RDB file: {}", inner),
            RdbFileError::Unimplemented(inner) => write!(f, "Unimplemented: {}", inner),
        }
    }
}

impl std::error::Error for RdbFileError {}

impl From<std::io::Error> for RdbFileError {
    fn from(from: std::io::Error) -> Self {
        RdbFileError::IOError(from)
    }
}

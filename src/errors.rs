// Error types for Redis implementation.

/// Errors encountered while setting up a redis server or handling requests.
#[derive(Debug)]
pub(crate) enum RedisError {
    RespParseError(RespError),      // An error parsing a RESP value.
    IOError(std::io::Error),        // An error performing an IO operation.
    UnknownRequest(String),         // The request type was not recognized.
    UnexpectedNumberOfArgs(String), // The request had an unexpected number of arguments.
    UnexpectedArgumentType(String), // The request had an argument of the wrong type.
    RdbParserError(RdbFileError),   // An error parsing an RDB file.
    ReplicationError(String),       // An error during replication.
    InvalidExpiration(String),      // The expiration time is invalid.
    InvalidPort(i32),              // The port number is invalid.
    InvalidDirectory(String),       // The directory path is invalid.
}

/// Errors encountered while parsing RESP values.
#[derive(Debug)]
pub(crate) enum RespError {
    UnexpectedEnd,           // The input value ended unexpectedly during parsing.
    UnknownStartingByte(u8), // The value being read started with a byte that was not recognized.
    BadBulkStringSize(i64),  // The size of a bulk string was invalid.
    BadArraySize(i64),       // The size of an array was invalid.
    IOError(std::io::Error), // An error performing an IO operation.
    IntParseFailure(std::num::ParseIntError), // An error parsing an integer.
    StringParseFailure(std::str::Utf8Error), // An error parsing a string.
}

/// Errors encountered while parsing Rdb files.
#[derive(Debug)]
pub(crate) enum RdbFileError {
    UnknownStartingByte(u8), // The value being read started with a byte that was not recognized.
    UnexpectedByte { expected: String, actual: u8 }, // The value being read was not the expected byte.
    NotRedisFile,                                    // The input was not a redis file.
    InvalidFile(String),                             // The input was not a valid redis file.
    IOError(std::io::Error),                         // An error performing an IO operation.
    Unimplemented(String),                           // The RDB feature is not implemented.
}

impl std::fmt::Display for RedisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisError::RespParseError(inner) => write!(f, "RESP parsing error {inner:?}"),
            RedisError::IOError(inner) => write!(f, "IOError {inner:?}"),
            RedisError::UnknownRequest(val) => write!(f, "Unknown request: {val:?}"),
            RedisError::UnexpectedNumberOfArgs(val) => {
                write!(f, "Unexpected number of arguments: {val}")
            }
            RedisError::UnexpectedArgumentType(val) => {
                write!(f, "Unexpected argument type: {val}")
            }
            RedisError::RdbParserError(inner) => inner.fmt(f),
            RedisError::ReplicationError(inner) => write!(f, "Redis replication error: {inner}"),
            RedisError::InvalidExpiration(inner) => write!(f, "Invalid expiration: {inner}"),
            RedisError::InvalidPort(port) => write!(f, "Invalid port: {port}"),
            RedisError::InvalidDirectory(dir) => write!(f, "Invalid directory: {dir}"),
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
            RespError::UnknownStartingByte(byte) => write!(f, "Unexpected starting byte {byte}"),
            RespError::IOError(io_err) => io_err.fmt(f),
            RespError::IntParseFailure(e) => e.fmt(f),
            RespError::StringParseFailure(e) => e.fmt(f),
            RespError::BadBulkStringSize(sz) => write!(f, "Invalid size for BulkString {sz}"),
            RespError::BadArraySize(sz) => write!(f, "Invalid size for Array {sz}"),
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

impl From<std::convert::Infallible> for RespError {
    fn from(_from: std::convert::Infallible) -> Self {
        unreachable!("Got infallible error in error conversion")
    }
}

impl std::fmt::Display for RdbFileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RdbFileError::IOError(inner) => write!(f, "IOError {inner:?}"),
            RdbFileError::UnknownStartingByte(byte) => {
                write!(f, "Unexpected starting byte {byte:#04x}")
            }
            RdbFileError::UnexpectedByte { expected, actual } => {
                write!(f, "Unexpected byte; expected {expected} got {actual:#04x}")
            }
            RdbFileError::NotRedisFile => write!(f, "Input was not a redis file"),
            RdbFileError::InvalidFile(inner) => write!(f, "Invalid RDB file: {inner}"),
            RdbFileError::Unimplemented(inner) => write!(f, "Unimplemented: {inner}"),
        }
    }
}

impl std::error::Error for RdbFileError {}

impl From<std::io::Error> for RdbFileError {
    fn from(from: std::io::Error) -> Self {
        RdbFileError::IOError(from)
    }
}

// Error type for Redis implementation.

use crate::resp_parser::RespError;

#[derive(Debug)]
pub(crate) enum RedisError {
    RespParseError(RespError),
    IOError(std::io::Error),
    UnknownRequest(String),
    UnexpectedNumberOfArgs(String),
    UnexpectedArgumentType(String),
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


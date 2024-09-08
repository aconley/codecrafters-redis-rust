// Common utilities.

use crate::errors::RespError;

pub(crate) fn parse_integer(input: &[u8]) -> Result<i64, RespError> {
    Ok(i64::from_str_radix(std::str::from_utf8(input)?, 10)?)
}

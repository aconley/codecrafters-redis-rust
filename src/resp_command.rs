use crate::resp_parser::{RespError, RespParser, RespValue};
/// Redis commands parsed from RESP.

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum RedisRequest<'a> {
    Ping,
    Echo(&'a [u8]),
}

pub(crate) enum RedisResponse<'a> {
    Pong,
    EchoResponse(&'a [u8]),
}

#[derive(Debug)]
pub(crate) enum RedisError {
    RespParseError(RespError),
    IOError(std::io::Error),
    UnknownRequest(String),
    UnexpectedNumberOfArgs(String),
    UnexpectedArgumentType(String),
}

impl<'a> RedisResponse<'a> {
    pub(crate) fn write<W: std::io::Write>(&self, writer: &mut W) -> Result<(), RespError> {
        match self {
            RedisResponse::Pong => RespValue::SimpleString(b"PONG").write(writer)?,
            RedisResponse::EchoResponse(contents) => {
                RespValue::BulkString(contents).write(writer)?
            }
        };
        Ok(())
    }
}

pub(crate) fn parse_commands<'a>(input: &'a [u8]) -> Result<Vec<RedisRequest<'a>>, RedisError> {
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let mut requests = Vec::new();
    let parser = RespParser::new();
    for resp_value in parser.get_values(input)? {
        requests.push(parse_command(resp_value)?);
    }
    Ok(requests)
}

fn parse_command<'a>(value: RespValue<'a>) -> Result<RedisRequest<'a>, RedisError> {
    match value {
        RespValue::Array(values) => {
            if values.is_empty() {
                return Err(RedisError::UnknownRequest("Empty array".to_string()));
            }
            match values[0] {
                RespValue::BulkString(contents) => {
                    let contents_upper: Vec<u8> =
                        contents.iter().map(|u| u.to_ascii_uppercase()).collect();
                    match &contents_upper[..] {
                        b"PING" => parse_ping(&values[1..]),
                        b"ECHO" => parse_echo(&values[1..]),
                        _ => Err(RedisError::UnknownRequest(format!(
                            "Unexpected command name {}",
                            String::from_utf8_lossy(&contents_upper)
                        ))),
                    }
                }
                _ => Err(RedisError::UnknownRequest(format!(
                    "For first element of array expected BulkString got {}",
                    values[0].type_string()
                ))),
            }
        }
        _ => Err(RedisError::UnknownRequest(format!(
            "Expected array got {}",
            value.type_string()
        ))),
    }
}

fn parse_ping<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if !values.is_empty() {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For PING expected 0 args found {}",
            values.len()
        )))
    } else {
        Ok(RedisRequest::Ping)
    }
}

fn parse_echo<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 1 {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For ECHO expected 1 args found {}",
            values.len()
        )))
    } else {
        match values[0] {
            RespValue::BulkString(contents) => Ok(RedisRequest::Echo(contents)),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For ECHO expected argument of BulkString got {}",
                values[0].type_string()
            ))),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resp_parser::RespValue;

    #[test]
    fn parse_ping() {
        let ping_value = RespValue::Array(vec![RespValue::BulkString(b"PING")]);
        let parsed = parse_command(ping_value);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(parsed.unwrap(), RedisRequest::Ping));
    }

    #[test]
    fn fail_to_parse_ping() {
        let ping_value = RespValue::Array(vec![
            RespValue::BulkString(b"PING"),
            RespValue::SimpleString(b"string"),
        ]);
        let parsed = parse_command(ping_value);
        assert!(
            parsed.is_err(),
            "Expected error, got: {:?}",
            parsed.unwrap()
        );
        assert!(matches!(
            parsed.unwrap_err(),
            RedisError::UnexpectedNumberOfArgs(_)
        ));
    }

    #[test]
    fn parse_echo() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"ECHO"),
            RespValue::BulkString(b"contents"),
        ]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(parsed.unwrap(), RedisRequest::Echo(b"contents")));
    }

    #[test]
    fn fail_parse_echo_missing_contents() {
        let echo_value = RespValue::Array(vec![RespValue::BulkString(b"ECHO")]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_err(),
            "Expected error, got: {:?}",
            parsed.unwrap()
        );
        assert!(matches!(
            parsed.unwrap_err(),
            RedisError::UnexpectedNumberOfArgs(_)
        ));
    }

    #[test]
    fn fail_parse_echo_wrong_contents_type() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"ECHO"),
            RespValue::SimpleError(b"ERROR"),
        ]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_err(),
            "Expected error, got: {:?}",
            parsed.unwrap()
        );
        assert!(matches!(
            parsed.unwrap_err(),
            RedisError::UnexpectedArgumentType(_)
        ));
    }

    #[test]
    fn parse_single_command() {
        let input = b"*2\r\n$4\r\nECHO\r\n$8\r\ncontents\r\n";
        let parsed = parse_commands(input);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        let commands = parsed.unwrap();
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], RedisRequest::Echo(b"contents")));
    }

    #[test]
    fn parse_multiple_commands() {
        let input = b"*2\r\n$4\r\nECHO\r\n$8\r\ncontents\r\n*1\r\n$4\r\nPING\r\n";
        let parsed = parse_commands(input);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        let commands = parsed.unwrap();
        assert_eq!(commands.len(), 2);
        assert!(matches!(commands[0], RedisRequest::Echo(b"contents")));
        assert!(matches!(commands[1], RedisRequest::Ping));
    }

    #[test]
    fn write_pong() {
        let mut buffer = Vec::new();
        assert!(RedisResponse::Pong.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"+PONG\r\n");
    }

    #[test]
    fn write_echo() {
        let mut buffer = Vec::new();
        assert!(RedisResponse::EchoResponse(b"hey")
            .write(&mut buffer)
            .is_ok());

        assert_eq!(buffer, b"$3\r\nhey\r\n");
    }
}

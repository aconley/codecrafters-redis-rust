use std::time::{Duration, SystemTime};

use crate::errors::RedisError;
use crate::resp_parser::{parse_integer, RespParser, RespValue};

/// Redis commands parsed from RESP.
#[derive(PartialEq, Clone, Debug)]
pub(crate) enum RedisRequest<'a> {
    Ping,
    Echo(&'a [u8]),
    Set {
        key: &'a [u8],
        value: &'a [u8],
        expiration: Option<SystemTime>,
    },
    ConfigGet(Vec<&'a [u8]>),
    Get(&'a [u8]),
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
                RespValue::BulkString(contents) => match &uppercase(contents)[..] {
                    b"PING" => parse_ping(&values[1..]),
                    b"ECHO" => parse_echo(&values[1..]),
                    b"SET" => parse_set(&values[1..]),
                    b"GET" => parse_get(&values[1..]),
                    b"CONFIG" => parse_config(&values[1..]),
                    _ => Err(RedisError::UnknownRequest(format!(
                        "Unexpected command name {}",
                        String::from_utf8_lossy(&contents)
                    ))),
                },
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

fn parse_set<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 2 && values.len() != 4 {
        return Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For ECHO expected 2 args found {}",
            values.len()
        )));
    };
    if values.len() == 2 {
        return match (&values[0], &values[1]) {
            (RespValue::BulkString(key), RespValue::BulkString(value)) => Ok(RedisRequest::Set {
                key,
                value,
                expiration: None,
            }),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For PUT expected arguments of type BulkString, BulkString got {},{}",
                values[0].type_string(),
                values[1].type_string()
            ))),
        };
    }
    // Version with expiration.
    return match (&values[0], &values[1], &values[2], &values[3]) {
        (RespValue::BulkString(key),
         RespValue::BulkString(value),
         RespValue::BulkString(expiration_type),
         RespValue::BulkString(expiration_value)) =>
            Ok(RedisRequest::Set {
                key,
                value,
                expiration: Some(parse_expiration(expiration_type, expiration_value)?)
            }),
        _ => Err(RedisError::UnexpectedArgumentType(format!(
            "For PUT with expriation expected arguments of type 4x BulkString, BulkString got {},{}, {}, {}",
            values[0].type_string(),
            values[1].type_string(),
            values[2].type_string(),
            values[3].type_string()
        ))),
    };
}

fn parse_get<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 1 {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For GET expected 1 args found {}",
            values.len()
        )))
    } else {
        match values[0] {
            RespValue::BulkString(key) => Ok(RedisRequest::Get(key)),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For GET expected arguments of type BulkString, BulkString got {}",
                values[0].type_string(),
            ))),
        }
    }
}

fn parse_config<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() < 2 {
        return Err(RedisError::UnexpectedNumberOfArgs(
            "For CONFIG expected at least CONFIG <SUBCOMMAND>".to_string(),
        ));
    }
    match values[0] {
        RespValue::BulkString(subcommand) => match &uppercase(subcommand)[..] {
            b"GET" => parse_command_get(&values[1..]),
            _ => Err(RedisError::UnknownRequest(format!(
                "Unknown SUBCOMMAND after CONFIG: {}",
                String::from_utf8_lossy(subcommand)
            ))),
        },
        _ => Err(RedisError::UnexpectedArgumentType(format!(
            "For CONFIG <SUBCOMMAND>, SUBCOMMAND should have been BulkString, got {}",
            values[0].type_string()
        ))),
    }
}

fn parse_command_get<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    let mut params = Vec::<&'a [u8]>::new();
    for (idx, value) in values.iter().enumerate() {
        match value {
            RespValue::BulkString(param) => params.push(*param),
            _ => {
                return Err(RedisError::UnexpectedArgumentType(format!(
                "For CONFIG GET values, expected type BulkString at position {} in values got {}", 
            idx, value.type_string())))
            }
        }
    }

    Ok(RedisRequest::ConfigGet(params))
}

fn uppercase(value: &[u8]) -> Vec<u8> {
    value.iter().map(|u| u.to_ascii_uppercase()).collect()
}

fn parse_expiration(
    expiration_type: &[u8],
    expiration_value: &[u8],
) -> Result<SystemTime, RedisError> {
    match &uppercase(expiration_type)[..] {
        b"PX" => {
            Ok(SystemTime::now() + Duration::from_millis(parse_integer(expiration_value)? as u64))
        }
        _ => Err(RedisError::UnknownRequest(format!(
            "For SET, unexpected expiry spec {}",
            String::from_utf8_lossy(expiration_type)
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::RespError;
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
    fn parse_set() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"SET"),
            RespValue::BulkString(b"key"),
            RespValue::BulkString(b"contents"),
        ]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(
            parsed.unwrap(),
            RedisRequest::Set {
                key: b"key",
                value: b"contents",
                expiration: None
            }
        ));
    }

    #[test]
    fn parse_set_with_expiration() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"SET"),
            RespValue::BulkString(b"key"),
            RespValue::BulkString(b"contents"),
            RespValue::BulkString(b"px"),
            RespValue::BulkString(b"1000"),
        ]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(
            parsed.unwrap(),
            RedisRequest::Set {
                key: b"key",
                value: b"contents",
                expiration: Some(_)
            }
        ));
    }

    #[test]
    fn parse_set_with_bad_expiration_type() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"SET"),
            RespValue::BulkString(b"key"),
            RespValue::BulkString(b"contents"),
            RespValue::BulkString(b"unknown"),
            RespValue::BulkString(b"1000"),
        ]);

        assert!(matches!(
            parse_command(echo_value),
            Err(RedisError::UnknownRequest(_))
        ));
    }

    #[test]
    fn parse_set_with_bad_expiration_value() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"SET"),
            RespValue::BulkString(b"key"),
            RespValue::BulkString(b"contents"),
            RespValue::BulkString(b"px"),
            RespValue::BulkString(b"not a number"),
        ]);

        assert!(matches!(
            parse_command(echo_value),
            Err(RedisError::RespParseError(RespError::IntParseFailure(_)))
        ));
    }

    #[test]
    fn parse_get() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"GET"),
            RespValue::BulkString(b"key"),
        ]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(parsed.unwrap(), RedisRequest::Get(b"key")));
    }

    #[test]
    fn parse_config_get_single() {
        let config_get = RespValue::Array(vec![
            RespValue::BulkString(b"CONFIG"),
            RespValue::BulkString(b"GET"),
            RespValue::BulkString(b"dir"),
        ]);
        assert!(matches!(parse_command(config_get),
            Ok(RedisRequest::ConfigGet(params)) if matches!(params[..], [b"dir"])));
    }

    #[test]
    fn parse_config_get_multiple() {
        let values = RespValue::Array(vec![
            RespValue::BulkString(b"CONFIG"),
            RespValue::BulkString(b"GET"),
            RespValue::BulkString(b"dir"),
            RespValue::BulkString(b"max_concurrency"),
        ]);
        assert!(matches!(parse_command(values),
            Ok(RedisRequest::ConfigGet(params)) if matches!(params[..], [b"dir", b"max_concurrency"])));
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
}

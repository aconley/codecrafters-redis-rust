use std::time::{Duration, SystemTime};

use crate::errors::{RedisError, RespError};
use crate::resp_parser::{parse_integer, RespParser, RespValue};

/// Redis commands parsed from RESP.
///
/// RespValue are the on-the-wire values in the RESP protocol, a
/// RedisRequest is a higher level representation of a single
/// Redis command.  Each should correspond to an action that
/// the server can take, such as setting or fetching a value
/// from the data store.
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
    Keys(&'a [u8]),
    Info(Option<&'a [u8]>),
    ReplConf(ReplConf),
    Psync(Psync),
}

// A REPLCONF command.
#[derive(PartialEq, Clone, Debug)]
pub(crate) enum ReplConf {
    Port(u16),
    Capa(String),
}

// A PSYNC command.
#[derive(PartialEq, Clone, Debug)]
pub(crate) struct Psync {
    pub replid: String,
    pub offset: i32,
}

/// Parses one or more Redis requests from the provided input.
pub(crate) fn parse_commands(input: &[u8]) -> Result<Vec<RedisRequest<'_>>, RedisError> {
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

impl RedisRequest<'_> {
    // Convert a RedisRequest to a RESP value.
    pub(crate) fn to_value(&self) -> RespValue<'_> {
        match self {
            RedisRequest::Ping => RespValue::Array(vec![RespValue::BulkString(b"PING")]),
            RedisRequest::Echo(contents) => RespValue::Array(vec![RespValue::BulkString(contents)]),
            RedisRequest::Set {
                key,
                value,
                expiration,
            } => {
                let mut array = Vec::new();
                array.push(RespValue::BulkString(b"SET"));
                array.push(RespValue::BulkString(key));
                array.push(RespValue::BulkString(value));
                if let Some(expiration) = expiration {
                    array.push(RespValue::BulkString(b"PXAT"));
                    array.push(RespValue::OwningBulkString(
                        expiration
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .expect("Couldn't compute duration since unix epoch")
                            .as_millis()
                            .to_string(),
                    ));
                }
                RespValue::Array(array)
            }
            RedisRequest::Get(key) => RespValue::Array(vec![
                RespValue::BulkString(b"GET"),
                RespValue::BulkString(key),
            ]),
            RedisRequest::ConfigGet(params) => {
                let mut array = Vec::new();
                array.push(RespValue::BulkString(b"CONFIG"));
                for param in params {
                    array.push(RespValue::BulkString(param));
                }
                RespValue::Array(array)
            }
            RedisRequest::Keys(pattern) => RespValue::Array(vec![
                RespValue::BulkString(b"KEYS"),
                RespValue::BulkString(pattern),
            ]),
            RedisRequest::Info(info_type) => match info_type {
                Some(info_type) => RespValue::Array(vec![
                    RespValue::BulkString(b"INFO"),
                    RespValue::BulkString(info_type),
                ]),
                None => RespValue::Array(vec![RespValue::BulkString(b"INFO")]),
            },
            RedisRequest::ReplConf(repl_conf) => {
                let mut array = Vec::new();
                array.push(RespValue::BulkString(b"REPLCONF"));
                match repl_conf {
                    ReplConf::Port(port) => {
                        array.push(RespValue::BulkString(b"listening-port"));
                        array.push(RespValue::OwningBulkString(format!("{port}")));
                    }
                    ReplConf::Capa(capa) => {
                        array.push(RespValue::BulkString(b"capa"));
                        array.push(RespValue::BulkString(capa.as_bytes()));
                    }
                }
                RespValue::Array(array)
            }
            RedisRequest::Psync(psync) => {
                let mut array = Vec::new();
                array.push(RespValue::BulkString(b"PSYNC"));
                array.push(RespValue::OwningBulkString(psync.replid.to_string()));
                array.push(RespValue::OwningBulkString(format!("{}", psync.offset)));
                RespValue::Array(array)
            }
        }
    }
}

/// Converts a RESP value to a RedisRequest.
fn parse_command(value: RespValue) -> Result<RedisRequest, RedisError> {
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
                    b"KEYS" => parse_keys(&values[1..]),
                    b"INFO" => parse_info(&values[1..]),
                    b"REPLCONF" => parse_replconf(&values[1..]),
                    b"PSYNC" => parse_psync(&values[1..]),
                    _ => Err(RedisError::UnknownRequest(format!(
                        "Unexpected command name {}",
                        String::from_utf8_lossy(contents)
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

// Parses a RespValue that represents a PING command.
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

// Parses a RespValue that represents an ECHO command.
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

/// Parses a RespValue that represents a SET command, which may have an optional expiration.
/// This sets the value for a single key in the data store.
fn parse_set<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 2 && values.len() != 4 {
        return Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For SET expected 2 args found {}",
            values.len()
        )));
    };
    // Version without expiration.
    if values.len() == 2 {
        return match (&values[0], &values[1]) {
            (RespValue::BulkString(key), RespValue::BulkString(value)) => Ok(RedisRequest::Set {
                key,
                value,
                expiration: None,
            }),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For SET expected arguments of type BulkString, BulkString got {},{}",
                values[0].type_string(),
                values[1].type_string()
            ))),
        };
    }
    // Version with expiration.
    match (&values[0], &values[1], &values[2], &values[3]) {
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
            "For SET with expriation expected arguments of type 4x BulkString, BulkString got {}, {}, {}, {}",
            values[0].type_string(),
            values[1].type_string(),
            values[2].type_string(),
            values[3].type_string()
        ))),
    }
}

// Parses a RespValue that represents a GET command.  This fetches
// the value of a single key from the data store.
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

// Parses a RespValue that represents a CONFIG command.
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

// Parses a RespValue that represents a CONFIG GET command.
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

// Parses a RespValue that represents a KEYS command, which fetches
// either all keys or a subset of keys matching a pattern.
fn parse_keys<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 1 {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For KEYS expected 1 args found {}",
            values.len()
        )))
    } else {
        match values[0] {
            RespValue::BulkString(pattern) => Ok(RedisRequest::Keys(pattern)),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For KEYS expected arguments of type BulkString, BulkString got {}",
                values[0].type_string(),
            ))),
        }
    }
}

// Parses a RespValue that represents an INFO command, which fetches
// information about the server.
fn parse_info<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.is_empty() {
        Ok(RedisRequest::Info(None))
    } else if values.len() > 1 {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For INFO expected <= 1 args found {}",
            values.len()
        )))
    } else {
        match values[0] {
            RespValue::BulkString(info_type) => Ok(RedisRequest::Info(Some(info_type))),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For INFO expected arguments of type BulkString, BulkString got {}",
                values[0].type_string(),
            ))),
        }
    }
}

// Parses a RespValue that represents a REPLCONF command, which
// configures the replication settings of the server.
fn parse_replconf<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 2 {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For REPLCONF expected 2 args found {}",
            values.len()
        )))
    } else {
        match values[0] {
            RespValue::BulkString(b"listening-port") => Ok(RedisRequest::ReplConf(ReplConf::Port(
                parse_from_bulk_string::<u16>(&values[1])?,
            ))),
            RespValue::BulkString(b"capa") => Ok(RedisRequest::ReplConf(ReplConf::Capa(
                parse_from_bulk_string::<String>(&values[1])?,
            ))),
            _ => Err(RedisError::UnexpectedArgumentType(format!(
                "For INFO expected arguments of type BulkString, BulkString got {}",
                values[0].type_string(),
            ))),
        }
    }
}

// Parses a RespValue that represents a PSYNC command, which is used
// to sync a follower with a leader.
fn parse_psync<'a>(values: &[RespValue<'a>]) -> Result<RedisRequest<'a>, RedisError> {
    if values.len() != 2 {
        Err(RedisError::UnexpectedNumberOfArgs(format!(
            "For PSYNC expected 2 args found {}",
            values.len()
        )))
    } else {
        let replid = parse_from_bulk_string::<String>(&values[0])?;
        let offset = parse_from_bulk_string::<i32>(&values[1])?;
        Ok(RedisRequest::Psync(Psync { replid, offset }))
    }
}

// Converts a byte slice to uppercase.
fn uppercase(value: &[u8]) -> Vec<u8> {
    value.iter().map(|u| u.to_ascii_uppercase()).collect()
}

// Validates that an expiration time is reasonable.
fn validate_expiration(expiration: SystemTime) -> Result<(), RedisError> {
    if expiration <= SystemTime::now() {
        return Err(RedisError::InvalidExpiration("Expiration cannot be in the past".to_string()));
    }
    
    // Prevent extremely far future dates (10 years)
    let max_future = SystemTime::now() + Duration::from_secs(10 * 365 * 24 * 60 * 60);
    if expiration > max_future {
        return Err(RedisError::InvalidExpiration("Expiration too far in future".to_string()));
    }
    Ok(())
}

// Parses an expiration type and value into a SystemTime.
fn parse_expiration(
    expiration_type: &[u8],
    expiration_value: &[u8],
) -> Result<SystemTime, RedisError> {
    let expiration = match &uppercase(expiration_type)[..] {
        b"PX" => {
            SystemTime::now() + Duration::from_millis(parse_integer(expiration_value)? as u64)
        }
        b"PXAT" => {
            SystemTime::UNIX_EPOCH
                + Duration::from_millis(parse_integer(expiration_value)? as u64)
        }
        _ => return Err(RedisError::UnknownRequest(format!(
            "For SET, unexpected expiry spec {}",
            String::from_utf8_lossy(expiration_type)
        ))),
    };
    
    validate_expiration(expiration)?;
    Ok(expiration)
}

// Parse a bulk string into a value of type T.
fn parse_from_bulk_string<T>(input: &RespValue) -> Result<T, RedisError>
where
    T: std::str::FromStr,
    <T as std::str::FromStr>::Err: Into<RespError>,
{
    match input {
        RespValue::BulkString(value) => std::str::from_utf8(value)
            .map_err(|e| RedisError::RespParseError(RespError::StringParseFailure(e)))
            .and_then(|s| {
                s.parse::<T>()
                    .map_err(|e| RedisError::RespParseError(e.into()))
            }),
        _ => Err(RedisError::UnexpectedArgumentType(format!(
            "Expected bulk string, got {}",
            input.type_string()
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
    fn parse_keys() {
        let echo_value = RespValue::Array(vec![
            RespValue::BulkString(b"KEYS"),
            RespValue::BulkString(b"*"),
        ]);
        let parsed = parse_command(echo_value);
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(parsed.unwrap(), RedisRequest::Keys(b"*")));
    }

    #[test]
    fn parse_replconf_port() {
        let replconf_value = RespValue::Array(vec![
            RespValue::BulkString(b"REPLCONF"),
            RespValue::BulkString(b"listening-port"),
            RespValue::BulkString(b"1234"),
        ]);

        let parsed = parse_command(replconf_value);

        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert!(matches!(
            parsed.unwrap(),
            RedisRequest::ReplConf(ReplConf::Port(1234))
        ));
    }

    #[test]
    fn parse_replconf_capa() {
        let replconf_value = RespValue::Array(vec![
            RespValue::BulkString(b"REPLCONF"),
            RespValue::BulkString(b"capa"),
            RespValue::BulkString(b"psync2"),
        ]);

        let parsed = parse_command(replconf_value);

        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        match parsed.unwrap() {
            RedisRequest::ReplConf(ReplConf::Capa(val)) => assert_eq!(val, "psync2"),
            a @ _ => panic!("Unexpected value type {:?}", a),
        }
    }

    #[test]
    fn parse_replconf_unknown() {
        let replconf_value = RespValue::Array(vec![
            RespValue::BulkString(b"REPLCONF"),
            RespValue::BulkString(b"unknown"),
            RespValue::BulkString(b"psync"),
        ]);

        assert!(matches!(
            parse_command(replconf_value),
            Err(RedisError::UnexpectedArgumentType(_))
        ));
    }

    #[test]
    fn parse_psync() {
        let replconf_value = RespValue::Array(vec![
            RespValue::BulkString(b"PSYNC"),
            RespValue::BulkString(b"?"),
            RespValue::BulkString(b"-1"),
        ]);

        let parsed = parse_command(replconf_value);

        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        match parsed.unwrap() {
            RedisRequest::Psync(Psync { replid, offset }) => {
                assert_eq!(replid, "?");
                assert_eq!(offset, -1);
            }
            a @ _ => panic!("Unexpected value type {:?}", a),
        }
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

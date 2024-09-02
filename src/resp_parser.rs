/// A minimal parser for Reddis serialization protocol messages.
///
/// See: https://redis.io/docs/latest/develop/reference/protocol-spec/

const SEPARATOR: &[u8] = b"\r\n";

#[derive(PartialEq, Clone, Debug)]
pub(crate) enum RespValue<'a> {
    SimpleString(&'a [u8]),
    SimpleError(&'a [u8]),
    SimpleInteger(i64),
    BulkString(&'a [u8]),
    NullBulkString,
    Array(Vec<RespValue<'a>>),
    NullArray,
}

#[derive(Debug)]
pub(crate) enum RespError {
    UnexpectedEnd,
    UnknownStartingByte(u8),
    IOError(std::io::Error),
    IntParseFailure(Option<std::num::ParseIntError>),
    BadBulkStringSize(i64),
    BadArraySize(i64),
}

impl<'a> RespValue<'a> {
    pub fn write<W: std::io::Write>(&self, writer: &mut W) -> Result<(), RespError> {
        match self {
            RespValue::SimpleString(ref contents) => {
                writer.write_all(&[b'+'])?;
                writer.write_all(contents)?;
            }
            RespValue::SimpleError(ref contents) => {
                writer.write_all(&[b'-'])?;
                writer.write_all(contents)?;
            }
            RespValue::SimpleInteger(value) => {
                writer.write_all(&[b':'])?;
                writer.write_all(value.to_string().as_bytes())?;
            }
            _ => todo!("implement others"),
        }
        writer.write_all(SEPARATOR)?;
        Ok(())
    }
}

struct RespParser<'a> {
    finder: memchr::memmem::Finder<'a>,
}

struct RespPartialParse<'a> {
    word: &'a [u8],
    remainder: &'a [u8],
}

// The result of parsing an input buffer.  Consists of an
// extracted value followed by a reference to the remainder
// of the buffer.
#[derive(PartialEq, Debug)]
pub(crate) struct RespParseStep<'a> {
    value: RespValue<'a>,
    remainder: &'a [u8],
}
type RespResult<'a> = Result<RespParseStep<'a>, RespError>;

impl<'a> RespParser<'a> {
    fn new() -> Self {
        RespParser {
            finder: memchr::memmem::Finder::new(SEPARATOR),
        }
    }

    // Extracts the next RespValue from the input, returning the value and
    // a slice pointing at the remainder of the input after that word.
    fn next_value<'b>(&self, input: &'b [u8]) -> RespResult<'b> {
        let RespPartialParse { word, remainder } = self.next_word(input)?;
        if word.len() == 0 {
            return Err(RespError::UnexpectedEnd);
        }
        let resp_value = match word[0] {
            b'+' => RespValue::SimpleString(&word[1..]),
            b'-' => RespValue::SimpleError(&word[1..]),
            b':' => parse_integer(&word[1..])?,
            _ => return Err(RespError::UnknownStartingByte(word[0])),
        };
        Ok(RespParseStep {
            value: resp_value,
            remainder,
        })
    }

    /// Extracts the next word from the input, returning the contents and a slice to the
    /// remainder of the input after the next word.
    ///
    //  Example: input = b'First\r\nSecond\r\nThird\r\n' -> { word: b'First', remainder: b'Second\r\nThird\r\n'}
    fn next_word<'b>(&self, input: &'b [u8]) -> Result<RespPartialParse<'b>, RespError> {
        match self.finder.find(input) {
            Some(separator_pos) => Ok(RespPartialParse {
                word: &input[0..separator_pos],
                remainder: &input[separator_pos + 2..],
            }),
            None => Err(RespError::UnexpectedEnd),
        }
    }
}

fn parse_integer(input: &[u8]) -> Result<RespValue, RespError> {
    match std::str::from_utf8(input) {
        Ok(val) => Ok(RespValue::SimpleInteger(i64::from_str_radix(val, 10)?)),
        Err(err) => Err(RespError::IntParseFailure(None)),
    }
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::UnexpectedEnd => write!(f, "Unexpected end of input stream"),
            RespError::UnknownStartingByte(byte) => write!(f, "Unexpected starting byte {}", byte),
            RespError::IOError(io_err) => io_err.fmt(f),
            RespError::IntParseFailure(e) => match e {
                Some(inner) => write!(f, "Unable to parse int {}", inner),
                None => write!(f, "Unable to parse int"),
            },
            RespError::BadBulkStringSize(sz) => write!(f, "Invalid size for BulkString {}", sz),
            RespError::BadArraySize(sz) => write!(f, "Invalid size for Array {}", sz),
        }
    }
}

impl std::error::Error for RespError {}

impl From<std::num::ParseIntError> for RespError {
    fn from(from: std::num::ParseIntError) -> Self {
        RespError::IntParseFailure(Some(from))
    }
}

impl From<std::io::Error> for RespError {
    fn from(from: std::io::Error) -> Self {
        RespError::IOError(from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_simple_string() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"+OK\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::SimpleString(b"OK"),
                remainder: &[]
            }
        );
    }

    #[test]
    fn parses_error() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"-SomeError\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::SimpleError(b"SomeError"),
                remainder: &[]
            }
        );
    }

    #[test]
    fn parses_int() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b":+1000\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::SimpleInteger(1000),
                remainder: &[]
            }
        );
    }

    #[test]
    fn parses_negative_int() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b":-33\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::SimpleInteger(-33),
                remainder: &[]
            }
        );
    }

    #[test]
    fn parse_leaves_remainder() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"-SomeError\r\nStuffAfterError");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::SimpleError(b"SomeError"),
                remainder: b"StuffAfterError"
            }
        );
    }

    #[test]
    fn writes_simple_string() {
        let value = RespValue::SimpleString(b"OK");

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"+OK\r\n");
    }

    #[test]
    fn writes_error() {
        let value = RespValue::SimpleError(b"SomeError");

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"-SomeError\r\n");
    }

    #[test]
    fn writes_integer() {
        let value = RespValue::SimpleInteger(-1);

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b":-1\r\n");
    }

    #[test]
    fn missing_separator_is_error() {
        let parser = RespParser::new();
        let resp = parser.next_value(b"+OK");
        assert!(resp.is_err());
        assert!(matches!(
            resp.unwrap_err(),
            RespError::UnexpectedEnd
        ));
    }

    #[test]
    fn unknown_starting_byte_is_error() {
        let parser = RespParser::new();
        let resp = parser.next_value(b"~24\r\n");
        assert!(resp.is_err());
        assert!(matches!(
            resp.unwrap_err(),
            RespError::UnknownStartingByte(b'~')
        ));
    }

    #[test]
    fn bad_integer_is_error() {
        let parser = RespParser::new();
        let resp = parser.next_value(b":12uhoh33\r\n");
        assert!(resp.is_err(), "Expected error");
        assert!(matches!(
            resp.unwrap_err(),
            RespError::IntParseFailure(_)
        ));
    }
}

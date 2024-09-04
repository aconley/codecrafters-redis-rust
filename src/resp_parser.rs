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
    pub(crate) fn write<W: std::io::Write>(&self, writer: &mut W) -> Result<(), RespError> {
        match self {
            RespValue::SimpleString(ref contents) => {
                writer.write_all(&[b'+'])?;
                writer.write_all(contents)?;
                writer.write_all(SEPARATOR)?;
            }
            RespValue::SimpleError(ref contents) => {
                writer.write_all(&[b'-'])?;
                writer.write_all(contents)?;
                writer.write_all(SEPARATOR)?;
            }
            RespValue::SimpleInteger(value) => {
                writer.write_all(&[b':'])?;
                writer.write_all(value.to_string().as_bytes())?;
                writer.write_all(SEPARATOR)?;
            }
            RespValue::BulkString(ref contents) => {
                writer.write_all(&[b'$'])?;
                writer.write_all(format!("{}", contents.len()).as_bytes())?;
                writer.write_all(SEPARATOR)?;
                writer.write_all(contents)?;
                writer.write_all(SEPARATOR)?;
            }
            RespValue::NullBulkString => writer.write_all(b"$-1\r\n")?,
            RespValue::Array(vals) => {
                writer.write_all(&[b'*'])?;
                writer.write_all(format!("{}", vals.len()).as_bytes())?;
                writer.write_all(SEPARATOR)?;
                for val in vals {
                    val.write(writer)?;
                }
            }
            RespValue::NullArray => writer.write_all(b"*-1\r\n")?,
        }
        Ok(())
    }

    pub(crate) fn type_string(&self) -> String {
        match self {
            RespValue::SimpleString(_) => "SimpleString".to_string(),
            RespValue::SimpleError(_) => "SimpleError".to_string(),
            RespValue::SimpleInteger(_) => "SimpleInteger".to_string(),
            RespValue::BulkString(_) => "BulkString".to_string(),
            RespValue::NullBulkString => "NullBulkString".to_string(),
            RespValue::Array(_) => "Array".to_string(),
            RespValue::NullArray => "NullArray".to_string(),
        }
    }
}

pub(crate) struct RespParser<'a> {
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
    pub(crate) fn new() -> Self {
        RespParser {
            finder: memchr::memmem::Finder::new(SEPARATOR),
        }
    }

    pub(crate) fn get_values<'b>(&self, input: &'b [u8]) -> Result<Vec<RespValue<'b>>, RespError> {
        if input.len() == 0 {
            return Ok(Vec::new());
        }
        let mut resp_values = Vec::new();
        let mut curr_remainder = input;
        while !curr_remainder.is_empty() {
            let RespParseStep{value, remainder} = self.next_value(curr_remainder)?;
            resp_values.push(value);
            curr_remainder = remainder;
        }
        Ok(resp_values)
    }

    // Extracts the next RespValue from the input, returning the value and
    // a slice pointing at the remainder of the input after that word.
    fn next_value<'b>(&self, input: &'b [u8]) -> RespResult<'b> {
        let RespPartialParse { word, remainder } = self.next_word(input)?;
        if word.len() == 0 {
            return Err(RespError::UnexpectedEnd);
        }
        match word[0] {
            b'+' => Ok(RespParseStep {
                value: RespValue::SimpleString(&word[1..]),
                remainder,
            }),
            b'-' => Ok(RespParseStep {
                value: RespValue::SimpleError(&word[1..]),
                remainder,
            }),
            b':' => Ok(RespParseStep {
                value: RespValue::SimpleInteger(self.parse_integer(&word[1..])?),
                remainder,
            }),
            b'$' => self.parse_bulk_string(&word[1..], remainder),
            b'*' => self.parse_array(&word[1..], remainder),
            _ => Err(RespError::UnknownStartingByte(word[0])),
        }
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

    fn parse_integer(&self, input: &[u8]) -> Result<i64, RespError> {
        match std::str::from_utf8(input) {
            Ok(val) => Ok(i64::from_str_radix(val, 10)?),
            Err(_) => Err(RespError::IntParseFailure(None)),
        }
    }

    fn parse_bulk_string<'b>(&self, input: &'b [u8], remainder: &'b [u8]) -> RespResult<'b> {
        let size = self.parse_integer(input)?;
        if size < -1 {
            Err(RespError::BadBulkStringSize(size))
        } else if size == -1 {
            Ok(RespParseStep {
                value: RespValue::NullBulkString,
                remainder,
            })
        } else if (size as usize) > (remainder.len() - 2) {
            Err(RespError::UnexpectedEnd)
        } else if &remainder[(size as usize)..(size as usize + 2)] != SEPARATOR {
            Err(RespError::BadBulkStringSize(size))
        } else {
            Ok(RespParseStep {
                value: RespValue::BulkString(&remainder[0..(size as usize)]),
                remainder: &remainder[(size as usize) + 2..],
            })
        }
    }

    fn parse_array<'b>(&self, input: &'b [u8], remainder: &'b [u8]) -> RespResult<'b> {
        let size = self.parse_integer(input)?;
        if size < -1 {
            Err(RespError::BadArraySize(size))
        } else if size == -1 {
            Ok(RespParseStep {
                value: RespValue::NullArray,
                remainder,
            })
        } else {
            let mut vals = Vec::with_capacity(size as usize);
            let mut curr_remainder = remainder;
            for _ in 0..size {
                let RespParseStep { value, remainder } = self.next_value(&curr_remainder)?;
                vals.push(value);
                curr_remainder = remainder;
            }
            Ok(RespParseStep {
                value: RespValue::Array(vals),
                remainder: &curr_remainder,
            })
        }
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
    fn parses_bulk_string() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"$13\r\nImABulkString\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::BulkString(b"ImABulkString"),
                remainder: &[]
            }
        );
    }

    #[test]
    fn parses_null_bulk_string() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"$-1\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::NullBulkString,
                remainder: &[]
            }
        );
    }

    #[test]
    fn parses_array() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"*2\r\n+OK\r\n$3\r\nBlk\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::Array(vec![
                    RespValue::SimpleString(b"OK"),
                    RespValue::BulkString(b"Blk")
                ]),
                remainder: &[]
            }
        );
    }

    #[test]
    fn parses_null_array() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"*-1\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            RespParseStep {
                value: RespValue::NullArray,
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
    fn get_values() {
        let parser = RespParser::new();
        let parsed = parser.get_values(b"+OK\r\n:33\r\n");
        assert!(
            parsed.is_ok(),
            "Expected ok result, got: {}",
            parsed.err().unwrap()
        );
        assert_eq!(
            parsed.unwrap(),
            vec![RespValue::SimpleString(b"OK"), RespValue::SimpleInteger(33)]
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
    fn writes_bulk_string() {
        let value = RespValue::BulkString(b"ImABulkString");

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"$13\r\nImABulkString\r\n");
    }

    #[test]
    fn writes_null_bulk_string() {
        let value = RespValue::NullBulkString;

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"$-1\r\n");
    }

    #[test]
    fn writes_array() {
        let value = RespValue::Array(vec![
            RespValue::SimpleString(b"hello"),
            RespValue::SimpleError(b"error"),
        ]);

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"*2\r\n+hello\r\n-error\r\n");
    }

    #[test]
    fn writes_null_array() {
        let value = RespValue::NullArray;

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        assert_eq!(buffer, b"*-1\r\n");
    }

    #[test]
    fn round_trips_simple_string() {
        let value = RespValue::SimpleString(b"string");

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(round_tripped_value.is_ok());

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn round_trips_simple_error() {
        let value = RespValue::SimpleError(b"err");

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(round_tripped_value.is_ok());

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn round_trips_simple_int() {
        let value = RespValue::SimpleInteger(202034);

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(round_tripped_value.is_ok());

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn round_trips_bulk_string() {
        let value = RespValue::BulkString(b"IAmABulkStringMyFriend");

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(
            round_tripped_value.is_ok(),
            "Expected successful round trip, got {:?}",
            round_tripped_value.unwrap_err()
        );

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn round_trips_null_bulk_string() {
        let value = RespValue::NullBulkString;

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(
            round_tripped_value.is_ok(),
            "Expected successful round trip, got {:?}",
            round_tripped_value.unwrap_err()
        );

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn round_trips_array() {
        let value = RespValue::Array(vec![
            RespValue::BulkString(b"Blk"),
            RespValue::SimpleInteger(22),
            RespValue::NullArray,
        ]);

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(
            round_tripped_value.is_ok(),
            "Expected successful round trip, got {:?}",
            round_tripped_value.unwrap_err()
        );

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn round_trips_null_array() {
        let value = RespValue::NullArray;

        let mut buffer = Vec::new();
        assert!(value.write(&mut buffer).is_ok());

        let parser = RespParser::new();
        let round_tripped_value = parser.next_value(&buffer);
        assert!(
            round_tripped_value.is_ok(),
            "Expected successful round trip, got {:?}",
            round_tripped_value.unwrap_err()
        );

        assert!(matches!(
            round_tripped_value.unwrap(),
            RespParseStep {
                value,
                remainder: &[]
            }
        ));
    }

    #[test]
    fn missing_separator_is_error() {
        let parser = RespParser::new();
        let resp = parser.next_value(b"+OK");
        assert!(resp.is_err());
        assert!(matches!(resp.unwrap_err(), RespError::UnexpectedEnd));
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
        assert!(matches!(resp.unwrap_err(), RespError::IntParseFailure(_)));
    }

    #[test]
    fn unterminated_bulk_string() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"$26\r\nImAnUnterminatedBulkString");
        assert!(parsed.is_err(), "Expected error");
        let err = parsed.unwrap_err();
        assert!(
            matches!(err, RespError::UnexpectedEnd),
            "Expected unexpected end, got {:?}",
            err
        );
    }

    #[test]
    fn incorrect_bulk_string_length() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"$13\r\nLongerThanExpectedBulkString\r\n");
        assert!(parsed.is_err(), "Expected error");
        let err = parsed.unwrap_err();
        assert!(
            matches!(err, RespError::BadBulkStringSize(13)),
            "Expected unexpected end, got {:?}",
            err
        );
    }

    #[test]
    fn bad_bulk_string_length() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"$-5\r\n");
        assert!(parsed.is_err(), "Expected error");
        assert!(matches!(
            parsed.unwrap_err(),
            RespError::BadBulkStringSize(-5)
        ));
    }

    #[test]
    fn truncated_bulk_string() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"$3\r\nAb");
        assert!(parsed.is_err(), "Expected error");
        assert!(matches!(parsed.unwrap_err(), RespError::UnexpectedEnd));
    }

    #[test]
    fn unterminated_array() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"*2\r\n+OK\r\n-Err");
        assert!(parsed.is_err(), "Expected error");
        let err = parsed.unwrap_err();
        assert!(
            matches!(err, RespError::UnexpectedEnd),
            "Expected unexpected end, got {:?}",
            err
        );
    }

    #[test]
    fn bad_bulk_array_length() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"*-5\r\n");
        assert!(parsed.is_err(), "Expected error");
        assert!(matches!(parsed.unwrap_err(), RespError::BadArraySize(-5)));
    }

    #[test]
    fn truncated_array() {
        let parser = RespParser::new();
        let parsed = parser.next_value(b"*2\r\n+OK\r\n");
        assert!(parsed.is_err(), "Expected error");
        assert!(matches!(parsed.unwrap_err(), RespError::UnexpectedEnd));
    }
}

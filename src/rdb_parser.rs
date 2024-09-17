/// A parser for RDB files.
use std::io::Read;

use crate::{errors::RdbFileError, redis_handler::RedisHandler};

pub(crate) struct RdbReader<R> {
    reader: R,
    buffer: [u8; 1],
}

impl<R> RdbReader<R> {
    pub(crate) fn new(reader: R) -> Self {
        RdbReader {
            reader,
            buffer: [0u8; 1],
        }
    }
}

#[derive(Debug, PartialEq)]
enum RdbValue {
    Header { version: [u8; 4] },
    MetadataSection { key: Vec<u8>, value: Vec<u8> },
    EndOfFile { checksum: [u8; 8] },
}

impl<R> RdbReader<R>
where
    R: Read,
{
    // Create a RedisHandler from an input Reader.
    fn create_handler(&mut self) -> Result<RedisHandler, RdbFileError> {
        self.read_header()?;
        let mut config = std::collections::HashMap::new();
        loop {
            match self.read_next_value()? {
                RdbValue::Header { .. } => {
                    return Err(RdbFileError::InvalidFile(
                        "Multiple file headers".to_string(),
                    ))
                }
                RdbValue::MetadataSection { key, value } => {
                    config.insert(key, value);
                }
                RdbValue::EndOfFile { .. } => return Ok(RedisHandler::new_with_config(config)),
            }
        }
    }

    /// Read the magic header from a reader and verify it is valid.
    fn read_header(&mut self) -> Result<RdbValue, RdbFileError> {
        let mut buffer = [0u8; 9];
        self.reader.read_exact(&mut buffer)?;
        if buffer[0..5] != *b"REDIS" {
            Err(RdbFileError::NotRedisFile)
        } else {
            let mut version = [0u8; 4];
            version.copy_from_slice(&buffer[5..]);
            Ok(RdbValue::Header { version })
        }
    }

    /// Reads the next value from the input.
    pub(crate) fn read_next_value(&mut self) -> Result<RdbValue, RdbFileError> {
        match self.read_next_byte()? {
            0xfa => self.read_metadata_entry(),
            0xfe => self.read_database(),
            0xff => self.read_end_of_file(),
            b => Err(RdbFileError::UnknownStartingByte(b)),
        }
    }

    fn read_metadata_entry(&mut self) -> Result<RdbValue, RdbFileError> {
        Ok(RdbValue::MetadataSection {
            key: self.read_string()?,
            value: self.read_string()?,
        })
    }

    fn read_database(&mut self) -> Result<RdbValue, RdbFileError> {
        todo!("Implement")
    }

    fn read_end_of_file(&mut self) -> Result<RdbValue, RdbFileError> {
        let mut checksum = [0u8; 8];
        self.reader.read_exact(&mut checksum)?;
        Ok(RdbValue::EndOfFile { checksum })
    }

    fn read_string(&mut self) -> Result<Vec<u8>, RdbFileError> {
        let b = self.read_next_byte()?;
        match (b & 0xc0) >> 6 {
            0 => {
                // Next 6 bits of the lead byte specify the length of the string.
                let len = b & 0x3f;
                let mut v = vec![0u8; len as usize];
                self.reader.read_exact(&mut v)?;
                Ok(v)
            }
            1 => {
                // The size is the next 14 bits.
                let len = (((b & 0x3f) as usize) << 8) + self.read_next_byte()? as usize;
                let mut v = vec![0u8; len];
                self.reader.read_exact(&mut v)?;
                Ok(v)
            }
            2 => {
                // The size is the next 4 bytes (ignore the rest of the first byte).
                let mut buffer = [0u8; 4];
                self.reader.read_exact(&mut buffer)?;
                let len = u32::from_be_bytes(buffer) as usize;
                let mut v = vec![0u8; len];
                self.reader.read_exact(&mut v)?;
                Ok(v)
            }
            3 => {
                // Special string encodings of numbers.
                match b {
                    0xc0 => {
                        // 8 bit value.
                        let v = self.read_next_byte()?;
                        Ok(format!("{}", v).into_bytes())
                    }
                    0xc1 => {
                        // 16 bit value.
                        let mut buffer = [0u8; 2];
                        self.reader.read_exact(&mut buffer)?;
                        Ok(format!("{}", i16::from_le_bytes(buffer)).into_bytes())
                    }
                    0xc2 => {
                        // 32 bit value.
                        let mut buffer = [0u8; 4];
                        self.reader.read_exact(&mut buffer)?;
                        Ok(format!("{}", i32::from_le_bytes(buffer)).into_bytes())
                    }
                    _ => Err(RdbFileError::UnknownStartingByte(b)),
                }
            }

            _ => Err(RdbFileError::UnknownStartingByte(b)),
        }
    }

    fn read_next_byte(&mut self) -> Result<u8, RdbFileError> {
        self.reader.read_exact(&mut self.buffer[0..1])?;
        Ok(self.buffer[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_header() {
        let input: [u8; 9] = [0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31];
        let mut reader = RdbReader::new(&input[..]);

        let actual = reader.read_header();
        assert!(
            actual.is_ok(),
            "Expected successful header read, got {:?}",
            actual.unwrap_err()
        );
        assert_eq!(
            actual.unwrap(),
            RdbValue::Header {
                version: [0x30, 0x30, 0x31, 0x31],
            }
        );
    }

    #[test]
    fn read_header_not_redis_file() {
        // Second byte should be 45.
        let input: [u8; 9] = [0x52, 0x46, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31];
        let mut reader = RdbReader::new(&input[..]);

        assert!(matches!(
            reader.read_header(),
            Err(RdbFileError::NotRedisFile)
        ));
    }

    #[test]
    fn read_metadata() {
        let input = [
            0xFA, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2D, 0x76, 0x65, 0x72, 0x06, 0x36, 0x2E,
            0x30, 0x2E, 0x31, 0x36,
        ];
        let mut reader = RdbReader::new(&input[..]);

        let actual = reader.read_next_value();
        assert!(
            actual.is_ok(),
            "Expected successful metadata read, got {:?}",
            actual.unwrap_err()
        );
        assert_eq!(
            actual.unwrap(),
            RdbValue::MetadataSection {
                key: b"redis-ver".to_vec(),
                value: b"6.0.16".to_vec()
            }
        );
    }

    #[test]
    fn read_string() {
        let input = [
            0x0D, 0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x2C, 0x20, 0x57, 0x6F, 0x72, 0x6C, 0x64, 0x21,
        ];
        let mut reader = RdbReader::new(&input[..]);

        let actual = reader.read_string();
        assert!(
            actual.is_ok(),
            "Expected successful string read, got {:?}",
            actual.unwrap_err()
        );
        assert_eq!(actual.unwrap(), b"Hello, World!".to_vec());
    }

    #[test]
    fn read_short_string_integer() {
        let input = [0xC0, 0x7B];
        let mut reader = RdbReader::new(&input[..]);

        let actual = reader.read_string();
        assert!(
            actual.is_ok(),
            "Expected successful string read, got {:?}",
            actual.unwrap_err()
        );
        assert_eq!(actual.unwrap(), b"123".to_vec());
    }

    #[test]
    fn read_medium_string_integer() {
        let input = [0xC1, 0x39, 0x30];
        let mut reader = RdbReader::new(&input[..]);

        let actual = reader.read_string();
        assert!(
            actual.is_ok(),
            "Expected successful string read, got {:?}",
            actual.unwrap_err()
        );
        assert_eq!(actual.unwrap(), b"12345".to_vec());
    }

    #[test]
    fn read_long_string_integer() {
        let input = [0xC2, 0x87, 0xD6, 0x12, 0x00];
        let mut reader = RdbReader::new(&input[..]);

        let actual = reader.read_string();
        assert!(
            actual.is_ok(),
            "Expected successful string read, got {:?}",
            actual.unwrap_err()
        );
        assert_eq!(actual.unwrap(), b"1234567".to_vec());
    }
}

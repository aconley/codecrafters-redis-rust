// The redis data store and related objects.
//
// This object is not thread safe, so callers should ensure that
// only one thread is accessing it.  This isn't necessarily a great
// idea, but follows the actual Redis model, which uses a single thread
// to avoid locking overheads.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io::Read;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::errors::RedisError;
use crate::rdb_parser::RdbReader;
use crate::resp_command::{parse_commands, RedisRequest};
use crate::resp_parser::{RespParser, RespValue};

// The data store for Redis.
#[derive(Debug)]
pub(crate) struct RedisHandler {
    data: RefCell<HashMap<Vec<u8>, ValueType>>,
    replication_info: RedisReplicationInfo,
    config: RefCell<HashMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ValueType {
    value: Vec<u8>,
    expiration: Option<SystemTime>,
}

#[derive(Debug)]
pub(crate) struct RedisReplicationInfo {
    pub(crate) role: RedisRole,
    pub(crate) connected_followers: u16,
    pub(crate) leader_replid: String,
    pub(crate) leader_repl_offset: u32,
    pub(crate) leader_address: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RedisRole {
    Leader,
    Follower,
}

impl RedisHandler {
    pub(crate) fn new() -> Self {
        RedisHandler {
            data: RefCell::new(HashMap::new()),
            replication_info: RedisReplicationInfo::default(),
            config: RefCell::new(HashMap::new()),
        }
    }

    pub(crate) fn new_with_contents(
        config: HashMap<Vec<u8>, Vec<u8>>,
        replication_info: RedisReplicationInfo,
        data: HashMap<Vec<u8>, ValueType>,
    ) -> Self {
        RedisHandler {
            data: RefCell::new(data),
            replication_info: replication_info,
            config: RefCell::new(config),
        }
    }

    pub(crate) fn new_from_file(
        path: std::path::PathBuf,
        replication_info: RedisReplicationInfo,
        config: HashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<Self, RedisError> {
        let input = std::fs::read(path)?;
        Ok(RedisHandler {
            data: RefCell::new(RdbReader::new(&input[..]).read_contents()?),
            replication_info: replication_info,
            config: RefCell::new(config),
        })
    }

    // Handles all the requests in the stream.
    //
    // Precondition: this can only be called from a single threaded context, since the data
    // contents are not protected by a lock.
    pub(crate) async unsafe fn handle_requests(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(), RedisError> {
        // Use a vec to avoid having a large stack state in the state machine.
        let mut input_buf = vec![0u8; 512];
        loop {
            let bytes_read = stream.read(&mut input_buf).await?;
            if bytes_read == 0 {
                break;
            }
            let requests = match parse_commands(&input_buf[0..bytes_read]) {
                Ok(requests) => requests,
                Err(error) => {
                    // There's not much we can do if writing the error fails.
                    let _ = RespValue::SimpleError(format!("{:?}", error).as_bytes())
                        .write_async(stream)
                        .await;
                    continue;
                }
            };

            for request in requests {
                match self.handle_request(request, stream).await {
                    Ok(()) => (),
                    Err(error) => {
                        let _ = RespValue::SimpleError(format!("{:?}", error).as_bytes())
                            .write_async(stream)
                            .await;
                    }
                }
            }
        }
        Ok(())
    }

    // Handles a single request, writing the result to the provided stream.
    async unsafe fn handle_request<'a>(
        &self,
        request: RedisRequest<'a>,
        stream: &mut TcpStream,
    ) -> Result<(), RedisError> {
        match request {
            RedisRequest::Ping => RespValue::SimpleString(b"PONG").write_async(stream).await?,
            RedisRequest::Echo(contents) => {
                RespValue::BulkString(contents).write_async(stream).await?
            }
            RedisRequest::Set {
                key,
                value,
                expiration,
            } => {
                self.data.borrow_mut().insert(
                    key.to_vec(),
                    ValueType {
                        value: value.to_vec(),
                        expiration,
                    },
                );
                RespValue::SimpleString(b"OK").write_async(stream).await?;
            }
            RedisRequest::Get(key) => {
                // We have to make a copy of the value, because while we are paused on the await, another
                // future may overwrite the value for this key and invalidate the reference.
                let value_copy = self.data.borrow().get(key).map(|v| v.to_owned());
                match value_copy {
                    Some(value) if value.is_expired() => {
                        self.data.borrow_mut().remove(key);
                        RespValue::NullBulkString.write_async(stream).await?
                    }
                    Some(ValueType { value, .. }) => {
                        RespValue::BulkString(&value).write_async(stream).await?
                    }
                    None => RespValue::NullBulkString.write_async(stream).await?,
                }
            }
            RedisRequest::ConfigGet(params) => 'config_get: {
                if params.is_empty() {
                    RespValue::NullArray.write_async(stream).await?;
                    break 'config_get;
                }
                // We need to make a copy of all the responses for the await point.
                let mut values = Vec::with_capacity(2 * params.len());
                {
                    let config = self.config.borrow();
                    for param in params {
                        if let Some(value) = config.get(param) {
                            values.push(param.to_owned());
                            values.push(value.to_owned());
                        }
                    }
                }
                let response_array = values
                    .iter()
                    .map(|v| RespValue::BulkString(v))
                    .collect::<Vec<_>>();
                RespValue::Array(response_array).write_async(stream).await?
            }
            RedisRequest::Keys(params) => {
                let keys = match params {
                    b"*" => {
                        // All keys.
                        self.data
                            .borrow()
                            .keys()
                            .map(|k| k.to_owned())
                            .collect::<Vec<_>>()
                    }
                    _ => {
                        return Err(RedisError::UnknownRequest(format!(
                            "Only KEYS * supported, got KEYS {}",
                            String::from_utf8_lossy(params)
                        )));
                    }
                };
                let response_array = keys
                    .iter()
                    .map(|v| RespValue::BulkString(v))
                    .collect::<Vec<_>>();
                RespValue::Array(response_array).write_async(stream).await?
            }
            RedisRequest::Info(None) => self.replication_info.write_async(stream).await?,
            RedisRequest::Info(Some(info_type)) => match info_type {
                b"replication" => self.replication_info.write_async(stream).await?,
                _ => RespValue::NullBulkString.write_async(stream).await?,
            },
            RedisRequest::ReplConf(_) => RespValue::SimpleString(b"OK").write_async(stream).await?,
        }
        Ok(())
    }

    pub(crate) fn configure_replication(&self) -> Result<(), RedisError> {
        if self.replication_info.role == RedisRole::Leader {
            // Nothing to do.
            return Ok(());
        }
        let mut request_response_parser = RequestResponsePairProcessor::new(
            self.replication_info
                .leader_address
                .as_ref()
                .expect("leader_address not populated in Redis follower node"),
        )?;

        request_response_parser.request_expecting_exact_response(
            RespValue::Array(vec![RespValue::BulkString(b"PING")]),
            RespValue::SimpleString(b"PONG"),
        )?;

        request_response_parser.request_expecting_exact_response(
            RespValue::Array(vec![
                RespValue::BulkString(b"REPLCONF"),
                RespValue::BulkString(b"listening-port"),
                RespValue::BulkString(b"6380"),
            ]),
            RespValue::SimpleString(b"OK"),
        )?;

        request_response_parser.request_expecting_exact_response(
            RespValue::Array(vec![
                RespValue::BulkString(b"REPLCONF"),
                RespValue::BulkString(b"capa"),
                RespValue::BulkString(b"psync2"),
            ]),
            RespValue::SimpleString(b"OK"),
        )?;

        // PSYNC results in a non-standard response that we can't parse.
        request_response_parser.request_ignoring_response(
            RespValue::Array(vec![
                RespValue::BulkString(b"PSYNC"),
                RespValue::BulkString(b"?"),
                RespValue::BulkString(b"-1"),
            ]),
        )
    }
}

impl Default for RedisHandler {
    fn default() -> Self {
        RedisHandler::new()
    }
}

impl ValueType {
    pub(crate) fn new(value: Vec<u8>) -> Self {
        ValueType {
            value,
            expiration: None,
        }
    }

    pub(crate) fn new_from_seconds(value: Vec<u8>, seconds: u32) -> Self {
        ValueType {
            value,
            expiration: Some(UNIX_EPOCH + Duration::from_secs(seconds as u64)),
        }
    }

    pub(crate) fn new_from_millis(value: Vec<u8>, millis: u64) -> Self {
        ValueType {
            value,
            expiration: Some(UNIX_EPOCH + Duration::from_millis(millis)),
        }
    }

    fn is_expired(&self) -> bool {
        self.expiration
            .map_or(false, |expiration| SystemTime::now() > expiration)
    }
}

unsafe impl Send for RedisHandler {}
unsafe impl Sync for RedisHandler {}

impl RedisReplicationInfo {
    async fn write_async<W>(&self, writer: &mut W) -> Result<(), RedisError>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        let mut contents = String::default();
        match self.role {
            RedisRole::Leader => {
                contents.push_str("role:master\n");
                contents.push_str("master_replid:");
                contents.push_str(&self.leader_replid);
                contents.push_str(&format!("\nmaster_repl_offset:{}", self.leader_repl_offset));
                contents.push_str(&format!("\nconnected_slaves:{}", self.connected_followers));
            }
            RedisRole::Follower => contents.push_str("role:slave"),
        };
        RespValue::BulkString(contents.as_bytes())
            .write_async(writer)
            .await?;
        Ok(())
    }
}

impl Default for RedisReplicationInfo {
    fn default() -> Self {
        RedisReplicationInfo {
            role: RedisRole::Leader,
            connected_followers: 0,
            leader_replid: String::default(),
            leader_repl_offset: 0,
            leader_address: None,
        }
    }
}

struct RequestResponsePairProcessor<'a> {
    stream: std::net::TcpStream,
    buffer: Vec<u8>,
    parser: RespParser<'a>,
}

impl RequestResponsePairProcessor<'_> {
    fn new(addr: &str) -> Result<RequestResponsePairProcessor, RedisError> {
        Ok(RequestResponsePairProcessor {
            stream: std::net::TcpStream::connect(addr)?,
            buffer: vec![0u8; 128],
            parser: RespParser::new(),
        })
    }

    fn request_expecting_exact_response(
        &mut self,
        request: RespValue,
        expected_response: RespValue,
    ) -> Result<(), RedisError> {
        self.request_expecting_response(request, |v| *v == expected_response)
    }

    /// Makes the provided request, and checks that the response is a single
    /// element matching the provided matcher.
    fn request_expecting_response<F>(
        &mut self,
        request: RespValue,
        expected_response_matcher: F,
    ) -> Result<(), RedisError>
    where
        F: Fn(&RespValue) -> bool,
    {
        request.write(&mut self.stream)?;

        let bytes_read = self.stream.read(&mut self.buffer)?;
        if bytes_read == 0 {
            // Connection closed.
            return Err(RedisError::ReplicationError(format!(
                "Replication connection closed unexpectedly after {:?}",
                request
            )));
        }
        let values = self.parser.get_values(&self.buffer[..bytes_read])?;
        if values.len() != 1 {
            return Err(RedisError::ReplicationError(format!(
                "Expected one response to replication request {:?}, got {}",
                request,
                values.len()
            )));
        }
        if !expected_response_matcher(&values[0]) {
            return Err(RedisError::ReplicationError(format!(
                "Unexpected response during replication to request {:?}; got {:?}",
                request, values[0]
            )));
        }
        Ok(())
    }

    /// Makes the provided requst, reads the response but ignores it.
    /// Necessary because PSYNC returns a non-standard response until we are ready to parse.
    fn request_ignoring_response(&mut self, request: RespValue) -> Result<(), RedisError> {
        request.write(&mut self.stream)?;
        self.stream.read(&mut self.buffer)?;
        Ok(())
    }
}

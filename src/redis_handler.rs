// The redis data store and related objects.
//
// This object is not thread safe, so callers should ensure that
// only one thread is accessing it.  This isn't necessarily a great
// idea, but follows the actual Redis model, which uses a single thread
// to avoid locking overheads.

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

use crate::errors::RedisError;
use crate::rdb_parser::RdbReader;
use crate::resp_command::{parse_commands, RedisRequest};
use crate::resp_parser::RespValue;

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
    pub(crate) connected_slaves: u16,
    pub(crate) master_replid: String,
    pub(crate) master_repl_offset: u32,
}

#[derive(Debug)]
pub(crate) enum RedisRole {
    Master,
    Slave,
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
        }
        Ok(())
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
            RedisRole::Master => {
                contents.push_str("role:master\n");
                contents.push_str("master_replid:");
                contents.push_str(&self.master_replid);
                contents.push_str(&format!("\nmaster_repl_offset:{}", self.master_repl_offset));
                contents.push_str(&format!("\nconnected_slaves:{}", self.connected_slaves));
            }
            RedisRole::Slave => contents.push_str("role:slave"),
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
            role: RedisRole::Master,
            connected_slaves: 0,
            master_replid: String::default(),
            master_repl_offset: 0,
        }
    }
}

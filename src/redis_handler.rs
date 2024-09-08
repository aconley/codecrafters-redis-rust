// The redis data store and related objects.
//
// This object is not thread safe, so callers should ensure that
// only one thread is accessing it.  This isn't necessarily a great
// idea, but follows the actual Redis model, which uses a single thread
// to avoid locking overheads.

use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::errors::RedisError;
use crate::resp_command::{parse_commands, RedisRequest};
use crate::resp_parser::RespValue;

// The data store for Redis.
#[derive(Debug)]
pub(crate) struct RedisHandler {
    data: RefCell<HashMap<Vec<u8>, ValueType>>,
}

#[derive(Clone, Debug)]
struct ValueType {
    value: Vec<u8>,
    expiration: Option<Instant>,
}

impl RedisHandler {
    pub(crate) fn new() -> Self {
        RedisHandler {
            data: RefCell::new(HashMap::new()),
        }
    }

    // Handles all the requests in the stream.
    //
    // Precondition: this can only be called from a single threaded context, since the data
    // contents are not protected by a lock.
    pub(crate) async unsafe fn handle_requests(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(), RedisError> {
        let mut input_buf = [0u8; 512];
        loop {
            let bytes_read = stream.read(&mut input_buf).await?;
            if bytes_read == 0 {
                break;
            }
            let requests = match parse_commands(&input_buf[0..bytes_read]) {
                Ok(requests) => requests,
                Err(error) => {
                    // There's not much we can do if writing the error fails.
                    let _ = stream.write_all(format!("{:?}", error).as_bytes()).await;
                    continue;
                }
            };

            for request in requests {
                match self.handle_request(request, stream).await {
                    Ok(()) => (),
                    Err(error) => {
                        let _ = stream.write_all(format!("{:?}", error).as_bytes()).await;
                        ()
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
                RespValue::SimpleString(b"OK").write_async(stream).await?
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
                        RespValue::BulkString(&value)
                            .write_async(stream)
                            .await?
                    }
                    None => RespValue::NullBulkString.write_async(stream).await?,
                }
            }
        }
        Ok(())
    }
}

impl ValueType {
    fn is_expired(&self) -> bool {
        self.expiration
            .map_or(false, |expiration| Instant::now() > expiration)
    }
}

unsafe impl Send for RedisHandler {}
unsafe impl Sync for RedisHandler {}

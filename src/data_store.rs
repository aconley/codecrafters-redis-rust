// The redis data store and related objects.
//
// This object is not thread safe, so callers should ensure that
// only one thread is accessing it.

use std::collections::HashMap;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::resp_command::{parse_commands, RedisError, RedisRequest, RedisResponse};

// The data store for Redis.
#[derive(Debug)]
pub(crate) struct DataStore {
    // Uses a standard mutex, not a tokio one, because this should never
    // be called across awaits.
    data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl DataStore {
    pub(crate) fn new() -> Self {
        DataStore {
            data: Mutex::new(HashMap::new()),
        }
    }

    // Handles all the requests in the stream.
    pub(crate) async fn handle_requests(&self, mut stream: TcpStream) -> Result<(), RedisError> {
        let mut input_buf = [0u8; 512];
        // A local buffer we write to synchronously, before asynchronously dumping to the stream.
        let mut output_buf = Vec::<u8>::new();
        loop {
            let bytes_read = stream.read(&mut input_buf).await?;
            if bytes_read == 0 {
                break;
            }
            for request in  parse_commands(&input_buf[0..bytes_read])? {
                match request {
                    RedisRequest::Ping => RedisResponse::Pong.write(&mut output_buf)?,
                    RedisRequest::Echo(contents) => {
                        RedisResponse::EchoResponse(contents).write(&mut output_buf)?
                    }
                    RedisRequest::Set { key, value } => {
                        self.data.lock()?.insert(key.to_vec(), value.to_vec());
                        RedisResponse::Ok.write(&mut output_buf)?
                    }
                    RedisRequest::Get(key) => {
                        RedisResponse::GetResult(self.data.lock()?.get(key).map(|v| &**v))
                            .write(&mut output_buf)?
                    }
                };
                stream.write_all(&output_buf).await?;
                output_buf.clear();
            }
        }
        Ok(())
    }
}

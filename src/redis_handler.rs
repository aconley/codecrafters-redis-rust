// The redis data store and related objects.
//
// This object is not thread safe, so callers should ensure that
// only one thread is accessing it.  This isn't necessarily a great
// idea, but follows the actual Redis model, which uses a single thread
// to avoid locking overheads.

use std::cell::RefCell;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::redis_error::RedisError;
use crate::resp_command::{parse_commands, RedisRequest, RedisResponse};

// The data store for Redis.
#[derive(Debug)]
pub(crate) struct RedisHandler {
    data: RefCell<HashMap<Vec<u8>, Vec<u8>>>,
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
        mut stream: TcpStream,
    ) -> Result<(), RedisError> {
        let mut input_buf = [0u8; 512];
        // A local buffer we write to synchronously, before asynchronously dumping to the stream.
        let mut output_buf = Vec::<u8>::new();
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
                match self
                    .handle_request(request, &mut stream, &mut output_buf)
                    .await
                {
                    Ok(()) => (),
                    Err(error) => {
                        let _ = stream.write_all(format!("{:?}", error).as_bytes()).await;
                        ()
                    }
                }
                output_buf.clear();
            }
        }
        Ok(())
    }

    // Handles a single request, writing the result to the provided stream and using the intermediate
    // output buffer.
    async unsafe fn handle_request<'a>(
        &self,
        request: RedisRequest<'a>,
        stream: &mut TcpStream,
        output_buf: &mut Vec<u8>,
    ) -> Result<(), RedisError> {
        match request {
            RedisRequest::Ping => RedisResponse::Pong.write(output_buf)?,
            RedisRequest::Echo(contents) => {
                RedisResponse::EchoResponse(contents).write(output_buf)?
            }
            RedisRequest::Set { key, value } => {
                self.data.borrow_mut().insert(key.to_vec(), value.to_vec());
                RedisResponse::Ok.write(output_buf)?
            }
            RedisRequest::Get(key) => {
                RedisResponse::GetResult(self.data.borrow().get(key).map(|v| &**v))
                    .write(output_buf)?
            }
        }
        stream.write_all(&output_buf).await?;
        Ok(())
    }
}

unsafe impl Send for RedisHandler {}
unsafe impl Sync for RedisHandler {}
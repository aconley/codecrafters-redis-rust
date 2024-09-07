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

use crate::resp_command::{parse_commands, RedisError, RedisRequest, RedisResponse};

// The data store for Redis.
#[derive(Debug)]
pub(crate) struct DataStore {
    data: RefCell<HashMap<Vec<u8>, Vec<u8>>>,
}

impl DataStore {
    pub(crate) fn new() -> Self {
        DataStore {
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
            for request in parse_commands(&input_buf[0..bytes_read])? {
                match request {
                    RedisRequest::Ping => RedisResponse::Pong.write(&mut output_buf)?,
                    RedisRequest::Echo(contents) => {
                        RedisResponse::EchoResponse(contents).write(&mut output_buf)?
                    }
                    RedisRequest::Set { key, value } => {
                        self.data.borrow_mut().insert(key.to_vec(), value.to_vec());
                        RedisResponse::Ok.write(&mut output_buf)?
                    }
                    RedisRequest::Get(key) => {
                        RedisResponse::GetResult(self.data.borrow().get(key).map(|v| &**v))
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

unsafe impl Send for DataStore {}
unsafe impl Sync for DataStore {}

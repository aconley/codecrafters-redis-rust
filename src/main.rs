use resp_command::{parse_commands, RedisError, RedisRequest, RedisResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod resp_command;
mod resp_parser;

const IP_PORT: &str = "127.0.0.1:6379";

// Use only one worker thread so that the handler doesn't require locking.
// This is like actual redis, which is single threaded on the state.
#[tokio::main(worker_threads = 1)]
async fn main() {
    let listener = TcpListener::bind(IP_PORT).await.expect("Error connecting");

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, addr)) => {
                println!("accepted new connection from {}", addr);
                tokio::spawn(async move {
                    handle_requests(stream)
                        .await
                        .expect("Error handling message");
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn handle_requests(mut stream: TcpStream) -> Result<(), RedisError> {
    let mut buf = [0u8; 512];
    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            break;
        }
        let commands = parse_commands(&buf[0..bytes_read])?;
        for command in commands {
            let response = match command {
                RedisRequest::Ping => RedisResponse::Pong,
                RedisRequest::Echo(contents) => RedisResponse::EchoResponse(contents),
            };
            let mut output_buf = Vec::new();
            response.write(&mut output_buf)?;
            stream.write_all(&output_buf).await?;
        }
    }
    Ok(())
}

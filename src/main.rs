use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

mod resp_command;
mod resp_parser;

const IP_PORT: &str = "127.0.0.1:6379";
const PONG: &[u8; 7] = b"+PONG\r\n";

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
                tokio::spawn(async move { pong(stream).await.expect("Error handling message"); } );
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

async fn pong(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 256];
    loop {
        let bytes_read = stream.read(&mut buf).await?;
        if bytes_read == 0 {
            break;
        }
        stream.write_all(PONG).await?;
    }
    Ok(())
}

mod redis_handler;
mod errors;
mod resp_command;
mod resp_parser;
mod utils;

use std::sync::Arc;
use tokio::net::TcpListener;

use crate::redis_handler::RedisHandler;

const IP_PORT: &str = "127.0.0.1:6379";

// Only use one worker thread to obey the contract of data_store::DataStore.
#[tokio::main(worker_threads = 1)]
async fn main() {
    let handler = Arc::new(RedisHandler::new());
    let listener = TcpListener::bind(IP_PORT).await.expect("Error connecting");

    loop {
        match listener.accept().await {
            Ok((mut stream, addr)) => {
                println!("accepted new connection from {}", addr);
                let h = handler.clone();
                tokio::spawn(async move {
                    unsafe {
                        h.handle_requests(&mut stream)
                            .await
                            .expect("Error handling message");
                    }
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

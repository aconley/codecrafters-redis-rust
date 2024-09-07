mod data_store;
mod resp_command;
mod resp_parser;

use std::sync::Arc;
use tokio::net::TcpListener;

use crate::data_store::DataStore;

const IP_PORT: &str = "127.0.0.1:6379";

// Only use one worker thread to obey the contract of data_store::DataStore.
#[tokio::main(worker_threads = 1)]
async fn main() {
    let data_store = Arc::new(DataStore::new());
    let listener = TcpListener::bind(IP_PORT).await.expect("Error connecting");

    loop {
        let stream = listener.accept().await;
        match stream {
            Ok((stream, addr)) => {
                println!("accepted new connection from {}", addr);
                let ds = data_store.clone();
                tokio::spawn(async move {
                    unsafe {
                        ds.handle_requests(stream)
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

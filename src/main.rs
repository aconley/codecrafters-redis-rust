mod redis_handler;
mod errors;
mod resp_command;
mod resp_parser;

use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::redis_handler::RedisHandler;

const IP_PORT: &str = "127.0.0.1:6379";

#[derive(Parser)]
struct RedisArgs {
    dir: Option<String>, 
    dbfilename: Option<String>
}

impl RedisArgs {
    fn to_config_dict(self) -> HashMap<String, String> {
        let mut result = HashMap::new();
        if let Some(dir) = self.dir {
            result.insert("dir".to_string(), dir);
        }
        if let Some(dbfilename) = self.dbfilename {
            result.insert("dbfilename".to_string(), dbfilename);
        }
        result
    }
}

// Only use one worker thread to obey the contract of data_store::DataStore.
#[tokio::main(worker_threads = 1)]
async fn main() {
    let args = RedisArgs::parse();
    let handler = Arc::new(RedisHandler::new_with_config(args.to_config_dict()));

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

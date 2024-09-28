mod errors;
mod rdb_parser;
mod redis_handler;
mod resp_command;
mod resp_parser;

use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::redis_handler::RedisHandler;

const IP: &str = "127.0.0.1";

#[derive(Parser)]
struct RedisArgs {
    #[arg(short, long)]
    dir: Option<String>,

    #[arg(short, long)]
    dbfilename: Option<String>,

    #[arg(short, long, default_value_t = 6379)]
    port: i32,
}

impl RedisArgs {
    fn into_config_dict(self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut result = HashMap::new();
        if let Some(dir) = self.dir {
            result.insert(b"dir".to_vec(), dir.into_bytes());
        }
        if let Some(dbfilename) = self.dbfilename {
            result.insert(b"dbfilename".to_vec(), dbfilename.into_bytes());
        }
        result.insert(b"port".to_vec(), self.port.to_string().into_bytes());
        result
    }
}

// Only use one worker thread to obey the contract of data_store::DataStore.
#[tokio::main(worker_threads = 1)]
async fn main() {
    let args = RedisArgs::parse();
    let port = args.port;
    let handler = match &args.dbfilename {
        Some(filepath) => {
            let mut fully_qualified_path = std::path::PathBuf::new();
            if let Some(dir) = &args.dir {
                fully_qualified_path.push(dir);
            }
            fully_qualified_path.push(filepath);
            if !fully_qualified_path.exists() {
                Arc::new(RedisHandler::new_with_contents(
                    args.into_config_dict(),
                    HashMap::new(),
                ))
            } else {
                Arc::new(
                    RedisHandler::new_from_file(fully_qualified_path, args.into_config_dict())
                        .expect("Error reading rdb file"),
                )
            }
        }
        None => Arc::new(RedisHandler::new_with_contents(
            args.into_config_dict(),
            HashMap::new(),
        )),
    };
    let addr = format!("{}:{}", IP, port);
    let listener = TcpListener::bind(addr).await.expect("Error connecting");

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

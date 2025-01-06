mod errors;
mod rdb_parser;
mod redis_handler;
mod resp_command;
mod resp_parser;

use clap::Parser;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::errors::RedisError;
use crate::redis_handler::{RedisHandler, RedisReplicationInfo};

const IP: &str = "127.0.0.1";

#[derive(Parser)]
struct RedisArgs {
    #[arg(short, long)]
    dir: Option<String>,

    #[arg(short, long)]
    dbfilename: Option<String>,

    #[arg(short, long, default_value_t = 6379)]
    port: i32,

    #[arg(short, long)]
    replicaof: Option<String>,
}

impl RedisArgs {
    fn to_config_dict(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut result = HashMap::new();
        if let Some(dir) = &self.dir {
            result.insert(b"dir".to_vec(), dir.clone().into_bytes());
        }
        if let Some(dbfilename) = &self.dbfilename {
            result.insert(b"dbfilename".to_vec(), dbfilename.clone().into_bytes());
        }
        result.insert(b"port".to_vec(), self.port.to_string().into_bytes());
        result
    }
}

// Only use one worker thread to obey the contract of data_store::DataStore.
#[tokio::main(worker_threads = 1)]
async fn main() {
    let args = RedisArgs::parse();
    let replication_info =
        replication_info_from_args(&args).expect("Unable to parse replication_info");
    let handler = get_handler(&args, replication_info).expect("Unable to get handler");
    
    let addr = format!("{}:{}", IP, args.port);
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

fn get_handler(args: &RedisArgs, replication_info: RedisReplicationInfo) -> Result<Arc<RedisHandler>, RedisError> {
    let handler = match &args.dbfilename {
        Some(filepath) => {
            let mut fully_qualified_path = std::path::PathBuf::new();
            if let Some(dir) = &args.dir {
                fully_qualified_path.push(dir);
            }
            fully_qualified_path.push(filepath);
            if !fully_qualified_path.exists() {
                Arc::new(RedisHandler::new_with_contents(
                    args.to_config_dict(),
                    replication_info,
                    HashMap::new(),
                ))
            } else {
                Arc::new(
                    RedisHandler::new_from_file(
                        fully_qualified_path,
                        replication_info,
                        args.to_config_dict(),
                    )?
                )
            }
        }
        None => Arc::new(RedisHandler::new_with_contents(
            args.to_config_dict(),
            replication_info,
            HashMap::new(),
        )),
    };

    handler
        .configure_replication()?;
    Ok(handler)
}

fn replication_info_from_args(args: &RedisArgs) -> Result<RedisReplicationInfo, RedisError> {
    let mut replication_info = RedisReplicationInfo::default();
    match args.replicaof {
        Some(ref replicaof) => {
            replication_info.role = redis_handler::RedisRole::Follower;
            let replica_elems = replicaof
                .split_whitespace()
                .map(|s| s.to_string())
                .collect::<Vec<String>>();
            if replica_elems.len() != 2 {
                return Err(RedisError::UnexpectedNumberOfArgs(format!(
                    "Expected two components for replicaof, found {} (from {})",
                    replica_elems.len(),
                    replicaof
                )));
            }
            replication_info.leader_address =
                Some(format!("{}:{}", replica_elems[0], replica_elems[1]));
        }
        None => {
            replication_info.role = redis_handler::RedisRole::Leader;
            replication_info.leader_address = None;
            replication_info.leader_replid = rand::thread_rng()
                .sample_iter(&rand::distributions::Alphanumeric)
                .take(40)
                .map(char::from)
                .collect();
            replication_info.leader_repl_offset = 0;
        }
    }
    Ok(replication_info)
}

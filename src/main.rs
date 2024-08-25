use std::io::{Read, Write};
use std::thread;
use std::net::{TcpListener, TcpStream};

const PONG: &[u8; 7] = b"+PONG\r\n";

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(|| pong(stream).expect("Error handling connection"));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn pong(mut stream: TcpStream) -> std::io::Result<()> {
    let mut buf = [0u8; 256];
    loop {
        let bytes_read = stream.read(&mut buf)?;
        if bytes_read == 0 {
            break;
        }
        stream.write_all(PONG)?;
    }
    Ok(())
}

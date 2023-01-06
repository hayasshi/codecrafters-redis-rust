// Uncomment this block to pass the first stage
use std::{net::TcpListener, io::{Read, Write}};

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");

                let mut buf = [0; 10];
                _stream.read(&mut buf).unwrap();

                let command = std::str::from_utf8(&buf).unwrap();
                println!("Received: char_count={}, {}", command.chars().count(), command);
                if command.starts_with("PING") {
                    println!("PING!");
                    _stream.write("+PONG\r\n".as_bytes()).unwrap();
                }
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

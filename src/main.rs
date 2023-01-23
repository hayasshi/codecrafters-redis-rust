// Uncomment this block to pass the first stage
use std::net::TcpListener;
use std::io::{Read, Write};

use anyhow::{Result, bail};

use crate::RESP::*;

#[derive(Debug)]
enum RESP {
    SimpleString(String),
    Errors(String),
    Integers(i64),
    BulkString(Option<String>),
    Arrays(Vec<RESP>)
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                println!("accepted new connection");

                let resp = parse_resp(&mut _stream)?;
                println!("RESP={:?}", resp);
                action_resp(resp, &mut _stream)?;
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

fn action_resp(resp: RESP, write: &mut impl Write) -> Result<()> {
    println!("[action_resp] input: {:?}", resp);
    match resp {
        Arrays(commands) => {
            for command in commands {
                action_resp(command, write)?;
            }
            Ok(())
        },
        SimpleString(s) => {
            if s.as_str() == "ping" {
                write.write("+pong\r\n".as_bytes()).unwrap();
            }
            Ok(())
        },
        BulkString(Some(s)) => {
            if s.as_str() == "ping" {
                write.write("+pong\r\n".as_bytes()).unwrap();
            }
            Ok(())
        }
        _ => todo!()
    }
}

fn parse_resp(read: &mut impl Read) -> Result<RESP> {
    let mut head = [0;1];
    let ret = read.read(&mut head)?;
    if ret == 0 {
        bail!("Invalid bytes")
    } else {
        match head[0] {
            b'+' => parse_simple_string(read),
            b'-' => parse_error(read),
            b':' => parse_integers(read),
            b'$' => parse_bulk_string(read),
            b'*' => parse_arrays(read),
            _    => bail!("Invalid bytes")
        }
    }
}

fn extract_to_separator_from_stream(read: &mut impl Read) -> Result<String> {
    let mut s = String::new();
    let mut b = [0;1];

    loop {
        let ret = read.read(&mut b)?;
        if ret == 0 {
            break;
        }
        match b[0] {
            b'\r' => {
                let first = b[0];
                let mut next = [0;1];
                read.read(&mut next).unwrap();
                let second = next[0];
                if second == b'\n' {
                    break;
                } else {
                    s.push(first as char);
                    s.push(second as char);
                }
            },
            x => {
                s.push(x as char);
            }
        }
    }
    Ok(s)
}


fn parse_simple_string(read: &mut impl Read) -> Result<RESP> {
    let s = extract_to_separator_from_stream(read)?;
    Ok(RESP::SimpleString(s))
}

fn parse_error(read: &mut impl Read) -> Result<RESP> {
    let s = extract_to_separator_from_stream(read)?;
    Ok(RESP::Errors(s))
}

fn parse_integers(read: &mut impl Read) -> Result<RESP> {
    let s = extract_to_separator_from_stream(read)?;
    match s.parse::<i64>() {
        Ok(n) => Ok(RESP::Integers(n)),
        Err(err) => Ok(RESP::Errors(err.to_string()))
    }
}

fn parse_bulk_string(read: &mut impl Read) -> Result<RESP> {
    let s = extract_to_separator_from_stream(read)?;
    match s.parse::<i64>() {
        Ok(-1) => {
            Ok(RESP::BulkString(None))
        },
        Ok(n) => {
            let s = extract_to_separator_from_stream(read)?;
            if s.len() == n as usize {
                Ok(RESP::BulkString(Some(s)))
            } else {
                Ok(RESP::Errors(format!("Size mismatch for bulk string: n={}, string={}", n, s)))
            }
        },
        Err(err) => {
            Ok(RESP::Errors(err.to_string()))
        }
    }
}

fn parse_arrays(read: &mut impl Read) -> Result<RESP> {
    let s = extract_to_separator_from_stream(read)?;
    match s.parse::<i8>() {
        Ok(n) => {
            if n < 0 {
                Ok(RESP::Errors(String::from("()")))
            } else {
                let mut resps: Vec<RESP> = Vec::new();
                for _ in 1..=n {
                    let resp = parse_resp(read)?;
                    resps.push(resp);
                }
                Ok(RESP::Arrays(resps))
            }
        },
        Err(err) => {
            Ok(RESP::Errors(err.to_string()))
        }
    }
}

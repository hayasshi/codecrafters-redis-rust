use std::collections::VecDeque;
// Uncomment this block to pass the first stage
use std::net::TcpListener;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::thread;
use std::collections::HashMap;

use anyhow::{Result, bail, anyhow};

use crate::RESP::*;

#[derive(PartialEq, Debug)]
enum RESP {
    SimpleString(String),
    Errors(String),
    Integers(i64),
    BulkString(Option<String>),
    Arrays(VecDeque<RESP>),
    Empty
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    let storage: Arc<Mutex<HashMap<String, String>>> = Arc::new(Mutex::new(HashMap::new()));
    
    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                let mut storage = Arc::clone(&storage);
                thread::spawn(move || {
                    println!("accepted new connection");

                    loop {
                        let resp = match parse_resp(&mut _stream) {
                            Ok(r) => r,
                            Err(e) => {
                                println!("error: {}", e);
                                RESP::Empty
                            }
                        };
                        if resp == RESP::Empty {
                            break;
                        }
                        println!("RESP={:?}", resp);
                        action_resp(resp, &mut _stream, &mut storage).unwrap();
                    }
                });
            },
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }

    Ok(())
}

fn action_resp(resp: RESP, write: &mut impl Write, storage: &mut Arc<Mutex<HashMap<String, String>>>) -> Result<()> {
    println!("[action_resp] input: {:?}", resp);
    match resp {
        Arrays(mut commands) => {
            action_commands(&mut commands, write, storage)
        },
        other => {
            let mut commands = VecDeque::<RESP>::new();
            commands.push_back(other);
            action_commands(&mut commands, write, storage)
        }
    }
}

fn action_commands(commands: &mut VecDeque<RESP>, write: &mut impl Write, storage: &mut Arc<Mutex<HashMap<String, String>>>) -> Result<()> {

    fn extract_string(resp: RESP) -> Result<String> {
        match resp {
            SimpleString(s) => {
                Ok(s)
            },
            BulkString(Some(s)) => {
                Ok(s)
            },
            _ => bail!(format!("Unexpected RESP type: {:?}", resp))
        }
    }

    fn inner(command: String, lest: &mut VecDeque<RESP>, write: &mut impl Write, storage: &mut Arc<Mutex<HashMap<String, String>>>) -> Result<()> {
        match command.to_uppercase().as_str() {
            "PING" => {
                do_ping(write)
            },
            "ECHO" => {
                do_echo(lest.pop_front().unwrap(), write)
            },
            "SET" => {
                do_set(lest, write, storage)
            },
            "GET" => {
                do_get(lest, write, storage)
            }
            _ => panic!("Unexpected")
        }
    }

    fn do_ping(write: &mut impl Write) -> Result<()> {
        write.write("+PONG\r\n".as_bytes())?;
        Ok(())
    }

    fn do_echo(value: RESP, write: &mut impl Write) -> Result<()> {
        let s = extract_string(value)?;
        write.write(format!("+{}\r\n", s).as_bytes())?;
        Ok(())
    }

    fn do_set(lest: &mut VecDeque<RESP>, write: &mut impl Write, storage: &mut Arc<Mutex<HashMap<String, String>>>) -> Result<()> {
        let key_resp =  lest.pop_front().ok_or(anyhow!("Unexpected RESP structure"))?;
        let key = extract_string(key_resp)?;
        let value_resp =  lest.pop_front().ok_or(anyhow!("Unexpected RESP structure"))?;
        let value = extract_string(value_resp)?;

        let mut storage = storage.lock().unwrap();
        storage.insert(key, value);

        write.write("$2\r\nOK\r\n".as_bytes())?;
        Ok(())
    }

    fn do_get(lest: &mut VecDeque<RESP>, write: &mut impl Write, storage: &mut Arc<Mutex<HashMap<String, String>>>) -> Result<()> {
        let key_resp =  lest.pop_front().ok_or(anyhow!("Unexpected RESP structure"))?;
        let key = extract_string(key_resp)?;

        let storage = storage.lock().unwrap();

        match storage.get(&key) {
            Some(value) => {
                let size = value.chars().count();
                write.write(format!("${}\r\n{}\r\n", size, value).as_bytes())?;
            },
            None => {
                write.write("$-1\r\n".as_bytes())?;
            }
        }
        Ok(())
    }

    while !commands.is_empty() {
        match commands.pop_front().unwrap() {
            SimpleString(s) => {
                inner(s, commands, write, storage)?;
            },
            BulkString(Some(s)) => {
                inner(s, commands, write, storage)?;
            },
            _ => todo!()
        }
    }
    Ok(())
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
                let mut resps: VecDeque<RESP> = VecDeque::new();
                for _ in 1..=n {
                    let resp = parse_resp(read)?;
                    resps.push_back(resp);
                }
                Ok(RESP::Arrays(resps))
            }
        },
        Err(err) => {
            Ok(RESP::Errors(err.to_string()))
        }
    }
}

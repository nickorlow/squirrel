use std::io;
use std::io::{Read, Write};
use std::net::TcpStream;

fn main() {
    match TcpStream::connect("localhost:5433") {
        Ok(mut stream) => {
            println!("Connected to Database");
            loop {
                print!("SQUIRREL: ");
                io::stdout().flush().unwrap();

                let mut msg_str = String::new();
                let bytes = std::io::stdin().read_line(&mut msg_str).unwrap();
                if bytes == 0 {
                    break;
                }
                let msg = msg_str.as_bytes();

                stream.write(msg).unwrap();

                let mut response_size_buffer = [0 as u8; 8];
                stream.read_exact(&mut response_size_buffer).unwrap();
                let response_size: usize = usize::from_le_bytes(response_size_buffer);
                let mut response_buffer = vec![0 as u8; response_size];
                stream.read_exact(&mut response_buffer).unwrap();
                println!(
                    "{}",
                    String::from_utf8(response_buffer).expect("a utf-8 string")
                );
            }
        }
        Err(e) => {
            println!("Failed to connect: {}", e);
        }
    }
    println!("Goodbye!");
}

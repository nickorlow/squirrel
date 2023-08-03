use std::thread;
use std::net::{TcpListener, TcpStream, Shutdown};
use std::io::{Read, Write};
use core::str::Split;
use std::error::Error;
use std::fs;

mod parser;
pub use parser::command::Command;

mod table;
use parser::command::CreateCommand;
pub use table::datatypes::Datatype;
pub use table::table::TableDefinition;

const BUFFER_SIZE: usize = 500;


/*
CREATE TABLE [IF NOT EXISTS] table_name (
   column1 datatype(length) column_contraint,
   column2 datatype(length) column_contraint,
   column3 datatype(length) column_contraint,
   table_constraints
);
 */
fn handle_create(command: CreateCommand) -> Result<TableDefinition, String> {
    println!("Creating table with name: {}", command.table_definition.name);
    let mut file = fs::File::create(format!("./data/tabledefs/{}", command.table_definition.name)).unwrap();
            
    for column in &command.table_definition.column_defs {
        println!("creating col: {} {} {}", column.name, column.data_type.as_str(), column.length);
        let line = format!("{} {} {} \n", column.name, column.data_type.as_str(), column.length);
        file.write_all(line.as_bytes()).unwrap();
    }

    return Ok(command.table_definition);
}

fn run_command(query: String) -> String {
    let response: String;
    if query.chars().nth(0).unwrap() == '\\' {
        // handle PSQL's slash commands e.g.: \dt \d
        return String::from("Slash commands are not yet supported in SQUIRREL");
    }

    let command_result: Result<Command, String> = Command::from_string(query);

    if command_result.is_ok() {
        let command: Command = command_result.unwrap();
        response = match command {
            Command::Create(create_command) => { 
                let result_result = handle_create(create_command); 
                if result_result.is_err() {
                    String::from("Error creating table.") 
                } else {
                    String::from("Table created.") 
                }
            }
            _ => { String::from("Invalid command") }
        }
    } else {
        response = command_result.err().unwrap();
    }
    
    return response;
}

fn handle_client(mut stream: TcpStream) {
    let mut data = [0 as u8; BUFFER_SIZE]; 

    while match stream.read(&mut data) {
        Ok(size) => {
            let mut query_string = String::from_utf8(data.to_vec()).expect("A UTF-8 string");
            println!("Received: {}", query_string);

            let mut i = 0;
            for c in query_string.chars() {
                if c == ';' {
                    query_string = query_string.get(0..i).unwrap().to_string();
                    i = 0;
                    break;
                }
                i += 1;
            }

            let response: String;
            if i == 0 {
                response = run_command(query_string);
            } else {
                response = String::from("No semicolon.");
            }
            
            let mut response_data_size = response.len().to_le_bytes();
            stream.write(&mut response_data_size).unwrap(); // send length of message
            stream.write(response.as_bytes()).unwrap(); // send message
            true
        },
        Err(_) => {
            println!("An error occurred, terminating connection with {}", stream.peer_addr().unwrap());
            stream.shutdown(Shutdown::Both).unwrap();
            false
        }
    } {}
}

fn main() -> std::io::Result<()> {
    fs::remove_dir_all("./data")?; 
    fs::create_dir("./data")?;
    fs::create_dir("./data/tabledefs")?;
    fs::create_dir("./data/blobs")?;
    let listener = TcpListener::bind("0.0.0.0:5433")?;

    for stream in listener.incoming() {
        thread::spawn(|| {
            handle_client(stream.expect("A valid stream"));
            ()
        });
    }

    Ok(())
}
use anyhow::anyhow;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;

mod parser;
pub use parser::command::Command;

mod table;
use parser::command::{CreateCommand, InsertCommand, SelectCommand};
pub use table::datatypes::Datatype;
pub use table::table_definition::{ColumnDefinition, TableDefinition};

const BUFFER_SIZE: usize = 500;

fn handle_create(command: CreateCommand) -> ::anyhow::Result<TableDefinition> {
    let mut file = fs::File::create(format!(
        "./data/tabledefs/{}",
        command.table_definition.name
    ))?;

    for column in &command.table_definition.column_defs {
        let line = format!(
            "{} {} {} \n",
            column.name,
            column.data_type.as_str(),
            column.length
        );
        file.write_all(line.as_bytes())?;
    }

    Ok(command.table_definition)
}

fn read_tabledef(table_name: String) -> ::anyhow::Result<TableDefinition> {
    let file = fs::File::open(format!("./data/tabledefs/{}", table_name))?;

    let mut column_defs = vec![];

    for line in BufReader::new(file).lines() {
        let line_str = line?;
        let parts: Vec<&str> = line_str.split(' ').collect();
        let col_def = ColumnDefinition {
            name: parts[0].to_string(),
            data_type: Datatype::parse_from_str(parts[1])?,
            length: parts[2].parse::<u16>()?.into(),
        };
        column_defs.push(col_def);
    }

    Ok(TableDefinition {
        name: table_name,
        column_defs,
    })
}

fn handle_insert(command: InsertCommand) -> ::anyhow::Result<()> {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(format!("./data/blobs/{}", command.table_name))?;

    let tabledef = read_tabledef(command.table_name)?;

    for col_def in &tabledef.column_defs {
        if let Some(insert_item) = command.items.get(&col_def.name) {
            let bytes = col_def
                .data_type
                .to_bytes(insert_item.column_value.clone())?;
            file.write_all(&bytes)?;
            if bytes.len() < col_def.length {
                let length = col_def.length - bytes.len();
                let empty_bytes = vec![0; length];
                file.write_all(&empty_bytes)?;
            }
        } else {
            return Err(anyhow::anyhow!(
                "ERROR: INSERT statement is missing data for column '{}'",
                col_def.name
            ));
        }
    }

    Ok(())
}

fn handle_select(command: SelectCommand) -> ::anyhow::Result<String> {
    let mut file = fs::File::open(format!("./data/blobs/{}", command.table_name))?;
    let tabledef = read_tabledef(command.table_name)?;
    let mut response = String::new();

    response += "| ";
    for col_def in &tabledef.column_defs {
        response += format!("{} | ", col_def.name).as_str();
    }
    response += "\n";
    response += "-----------\n";
    let mut buf: Vec<u8> = vec![0; tabledef.get_byte_size()];
    while file.read_exact(buf.as_mut_slice()).is_ok() {
        response += "| ";
        let mut idx = 0;
        for col_def in &tabledef.column_defs {
            let len = if col_def.length > 0 {
                col_def.length
            } else {
                1
            };
            let str_val = col_def.data_type.from_bytes(&buf[idx..(idx + len)])?;
            response += format!("{} | ", str_val).as_str();
            idx += len;
        }
        response += "\n";
    }

    Ok(response)
}

fn run_command(query: String) -> ::anyhow::Result<String> {
    if query.starts_with('\\') {
        // handle PSQL's slash commands e.g.: \dt \d
        return Err(anyhow!("Slash commands are not yet supported in SQUIRREL"));
    }

    let command: Command = Command::from_string(query)?;

    match command {
        Command::Create(create_command) => {
            handle_create(create_command)?;
            Ok(String::from("Table Created"))
        }
        Command::Insert(insert_command) => {
            handle_insert(insert_command)?;
            Ok(String::from("Row Inserted"))
        }
        Command::Select(select_command) => handle_select(select_command),
        _ => Err(anyhow!("Invalid command")),
    }
}

fn handle_client(mut stream: TcpStream) {
    let mut data = [0_u8; BUFFER_SIZE];

    while match stream.read(&mut data) {
        Ok(_size) => {
            let query_string = String::from_utf8(data.to_vec()).expect("A UTF-8 string");
            let response: String = run_command(query_string).unwrap();

            let response_data_size = response.len().to_le_bytes();
            stream.write_all(&response_data_size).unwrap(); // send length of message
            stream.write_all(response.as_bytes()).unwrap(); // send message
            true
        }
        Err(_) => {
            println!(
                "An error occurred, terminating connection with {}",
                stream.peer_addr().unwrap()
            );
            stream.shutdown(Shutdown::Both).unwrap();
            false
        }
    } {}
}

fn main() -> std::io::Result<()> {
    //fs::remove_dir_all("./data")?;
    let _ensure_data_exists = fs::create_dir("./data");
    let _ensure_tabledefs_exists = fs::create_dir("./data/tabledefs");
    let _ensure_blob_exists = fs::create_dir("./data/blobs");
    let listener = TcpListener::bind("0.0.0.0:5433")?;

    for stream in listener.incoming() {
        thread::spawn(|| {
            handle_client(stream.expect("A valid stream"));
        });
    }

    Ok(())
}

#[test]
fn insert_statement() -> anyhow::Result<()> {
    let empty_statement = "";
    let regular_statement = "INSERT INTO users (id, name) VALUES (1, \"Test\");";
    let extra_ws_statement =
        "INSERT    INTO     users     (id, name)      VALUES      (1, \"Test\")    ;";
    let min_ws_statement = "INSERT INTO users(id, name) VALUES(1, \"Test\");";
    let str_comma_statement = "INSERT INTO users(id, name) VALUES(1, \"Firstname, Lastname\");";

    let expected_output = Command::Insert(InsertCommand {
        table_name: "users".to_string(),
        items: HashMap::from([
            (
                "id".to_string(),
                InsertItem {
                    column_name: "id".to_string(),
                    column_value: "1".to_string(),
                },
            ),
            (
                "name".to_string(),
                InsertItem {
                    column_name: "name".to_string(),
                    column_value: "\"Test\"".to_string(),
                },
            ),
        ]),
    });

    let expected_output_comma = Command::Insert(InsertCommand {
        table_name: "users".to_string(),
        items: HashMap::from([
            (
                "id".to_string(),
                InsertItem {
                    column_name: "id".to_string(),
                    column_value: "1".to_string(),
                },
            ),
            (
                "name".to_string(),
                InsertItem {
                    column_name: "name".to_string(),
                    column_value: "\"Firstname, Lastname\"".to_string(),
                },
            ),
        ]),
    });

    assert_eq!(
        Command::from_string(String::from(empty_statement)).is_ok(),
        false
    );

    assert_eq!(
        Command::from_string(String::from(regular_statement))?,
        expected_output
    );

    assert_eq!(
        Command::from_string(String::from(extra_ws_statement))?,
        expected_output
    );

    assert_eq!(
        Command::from_string(String::from(min_ws_statement))?,
        expected_output
    );

    assert_eq!(
        Command::from_string(String::from(str_comma_statement))?,
        expected_output_comma
    );

    Ok(())
}

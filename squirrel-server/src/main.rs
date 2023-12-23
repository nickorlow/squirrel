use anyhow::anyhow;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;
use std::collections::HashMap;
use std::cmp;

mod parser;
pub use parser::command::Command;

mod table;
use parser::command::{CreateCommand, InsertCommand, SelectCommand, DeleteCommand, LogicExpression, InsertItem, LogicValue};
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

fn handle_delete(command: DeleteCommand) -> ::anyhow::Result<String> {
    let mut file = fs::File::open(format!("./data/blobs/{}", command.table_name))?;
    let tabledef = read_tabledef(command.table_name.clone())?;
    let mut buf: Vec<u8> = vec![0; tabledef.get_byte_size()];
    let mut row_count: usize = 0;

    let mut new_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(format!("./data/blobs/{}_new", command.table_name))?;

    while file.read_exact(buf.as_mut_slice()).is_ok() {
        let mut row_data: HashMap<String, LogicValue> = HashMap::new();
        let mut idx: usize = 0;
        if let Some(ref le) = command.logic_expression {
            let mut logic_expr = le.clone();
            for col_def in &tabledef.column_defs {
                let len = if col_def.length > 0 {
                    col_def.length
                } else {
                    1
                };
                let str_val = col_def.data_type.from_bytes(&buf[idx..(idx + len)])?;
                idx += len;
                row_data.insert(col_def.name.clone(), LogicValue::from_string(str_val)?); 
            }
            idx = 0;
            logic_expr.fill_values(row_data);
            if !logic_expr.evaluate()? {
                new_file.write_all(&buf)?;
                continue;
            }
        }
        row_count += 1;
    }
    new_file.flush()?;

    let _ = fs::remove_file(format!("./data/blobs/{}", command.table_name))?;
    let _ = fs::rename(format!("./data/blobs/{}_new", command.table_name), format!("./data/blobs/{}", command.table_name))?;

    return Ok(format!("{} Rows Deleted", row_count));
}

fn handle_select(command: SelectCommand) -> ::anyhow::Result<String> {
    let mut file = fs::File::open(format!("./data/blobs/{}", command.table_name))?;
    let tabledef = read_tabledef(command.table_name)?;
    let mut response = String::new();
    let mut column_names: Vec<String> = vec![];

    for col_name in &command.column_names {
        if col_name == "*" {
            for col_defs in &tabledef.column_defs {
                column_names.push(col_defs.name.clone());
            }
        } else {
            column_names.push(col_name.clone());
        }
    }

    let mut buf: Vec<u8> = vec![0; tabledef.get_byte_size()];

    let mut table: HashMap<String, Vec<String>> = HashMap::new();
    let mut longest_cols: HashMap<String, usize> = HashMap::new();
    let mut num_rows: usize = 0;

    for col_name in &column_names {
        table.insert(col_name.clone(), vec![]);
    }

    while file.read_exact(buf.as_mut_slice()).is_ok() {
        let mut idx: usize = 0;
        let mut row_data: HashMap<String, LogicValue> = HashMap::new();
        if let Some(ref le) = command.logic_expression {
            let mut logic_expr = le.clone();
            for col_def in &tabledef.column_defs {
                let len = if col_def.length > 0 {
                    col_def.length
                } else {
                    1
                };
                let str_val = col_def.data_type.from_bytes(&buf[idx..(idx + len)])?;
                idx += len;
                row_data.insert(col_def.name.clone(), LogicValue::from_string(str_val)?); 
            }
            idx = 0;
            logic_expr.fill_values(row_data);
            if !logic_expr.evaluate()? {
                continue;
            }
        }

        for col_def in &tabledef.column_defs {
            let len = if col_def.length > 0 {
                col_def.length
            } else {
                1
            };
            if column_names.iter().any(|col_name| &col_def.name == col_name) { 
                let str_val = col_def.data_type.from_bytes(&buf[idx..(idx + len)])?.trim_matches(char::from(0)).to_string();
                table.get_mut(&col_def.name).unwrap().push(str_val.clone());
                longest_cols.entry(col_def.name.clone()).and_modify(|val| *val = cmp::max(*val, str_val.len())).or_insert(str_val.len());
            }
            idx += len;
        }
        num_rows += 1;
    }



    // construct table string
    response += "| ";
    for col_name in &column_names {
        longest_cols.entry(col_name.clone()).and_modify(|val| *val = cmp::max(*val, col_name.len())).or_insert(col_name.len());
        response += format!("{:0width$} | ", col_name, width = longest_cols.get(col_name).unwrap()).as_str();
    }
    let mut total_length: usize = 1;
    for (col_name, max_len) in longest_cols.clone() {
        total_length += max_len + 3;
    }
    response += "\n";
    for i in 0..total_length {
        response += "-";
    }
    response += "\n";
    for i in 0..num_rows { 
        response += "| ";
        for col_name in &column_names {
            response += format!("{:0width$} | ", table.get(col_name).unwrap()[i], width = longest_cols.get(col_name).unwrap()).as_str();
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

    println!("Parsed Command: {:?}", command);

    match command {
        Command::Create(create_command) => {
            let result = handle_create(create_command);
            if result.is_ok() {
                Ok(String::from("Table Created"))
            } else {
                Ok(result.err().unwrap().to_string())
            }
        }
        Command::Insert(insert_command) => {
            let result = handle_insert(insert_command);
            if result.is_ok() {
                Ok(String::from("Data Inserted"))
            } else {
                Ok(result.err().unwrap().to_string())
            }
        }
        Command::Select(select_command) => {
            let result = handle_select(select_command);
            if result.is_ok() {
                Ok(result?)
            } else {
                Ok(result.err().unwrap().to_string())
            }
        }
        Command::Delete(delete_command) => {
            let result = handle_delete(delete_command);
            if result.is_ok() {
                Ok(result?)
            } else {
                Ok(result.err().unwrap().to_string())
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) -> ::anyhow::Result<()> {
    let mut data = [0_u8; BUFFER_SIZE];

    while match stream.read(&mut data) {
        Ok(_size) => {
            let query_string = String::from_utf8(data.to_vec())?;
            let response_res: ::anyhow::Result<String> = run_command(query_string);

            let response = match response_res {
                Ok(result) => result,
                Err(err_msg) => String::from(format!("Error: {}", err_msg.to_string()))
            };
            
            let response_data_size = response.len().to_le_bytes();
            stream.write_all(&response_data_size)?; // send length of message
            stream.write_all(response.as_bytes())?; // send message
            true
        }
        Err(_) => {
            println!(
                "An error occurred, terminating connection with {}",
                stream.peer_addr()?
            );
            stream.shutdown(Shutdown::Both)?;
            false
        }
    } {}

    Ok(())
}

fn main() -> std::io::Result<()> {
    //fs::remove_dir_all("./data")?;
    let _ensure_data_exists = fs::create_dir("./data");
    let _ensure_tabledefs_exists = fs::create_dir("./data/tabledefs");
    let _ensure_blob_exists = fs::create_dir("./data/blobs");
    let listener = TcpListener::bind("0.0.0.0:5433")?;

    for stream in listener.incoming() {
        thread::spawn(|| -> ::anyhow::Result<()> {
            handle_client(stream?)?;
            Ok(())
        });
    }

    Ok(())
}

#[test]
fn logical_expression() -> anyhow::Result<()> {
    assert_eq!(Command::le_from_string(String::from("1 < 5")).unwrap().evaluate().unwrap(), true);
    assert_eq!(Command::le_from_string(String::from("1 > 5")).unwrap().evaluate().unwrap(), false);
    assert_eq!(Command::le_from_string(String::from("1 <= 5")).unwrap().evaluate().unwrap(), true);
    assert_eq!(Command::le_from_string(String::from("1 >= 5")).unwrap().evaluate().unwrap(), false);
    assert_eq!(Command::le_from_string(String::from("5 >= 5")).unwrap().evaluate().unwrap(), true);
    assert_eq!(Command::le_from_string(String::from("5 <= 5")).unwrap().evaluate().unwrap(), true);
    assert_eq!(Command::le_from_string(String::from("5 = 5")).unwrap().evaluate().unwrap(), true);
    assert_eq!(Command::le_from_string(String::from("5 AND 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("5 OR 5")).unwrap().evaluate().is_ok(), false);

    assert_eq!(Command::le_from_string(String::from("'Test' = 'Test'")).unwrap().evaluate().unwrap(), true);
    assert_eq!(Command::le_from_string(String::from("'Test' = 'Text'")).unwrap().evaluate().unwrap(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' <= 'Test'")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' >= 'Test'")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' < 'Test'")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' > 'Test'")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' AND 'Test'")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' OR 'Test'")).unwrap().evaluate().is_ok(), false);

    assert_eq!(Command::le_from_string(String::from("'Test' < 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' > 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' <= 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' >= 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' >= 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' <= 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' = 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' AND 5")).unwrap().evaluate().is_ok(), false);
    assert_eq!(Command::le_from_string(String::from("'Test' OR 5")).unwrap().evaluate().is_ok(), false);

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

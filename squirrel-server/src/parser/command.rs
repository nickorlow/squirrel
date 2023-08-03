use crate::{TableDefinition, Datatype};
use crate::table::table::Column;

pub enum Command {
    Select,
    Create(CreateCommand),
    Insert,
    Delete
}

pub struct CreateCommand {
    pub table_definition: TableDefinition,
}

impl Command {
    pub fn from_string(command_str: String) -> Result<Command, String> {
        let mut parts = command_str.split(' ');

        match parts.nth(0).unwrap() {
            "CREATE" => {
                let object = String::from(parts.nth(0).unwrap());
                if object.eq_ignore_ascii_case("TABLE") {
                    let mut column_definitions: Vec<Column> = vec![];

                    let column_def_begin_idx = command_str.chars().position(|c| c == '(').unwrap() + 1;
                    let column_def_end_idx = command_str.chars().position(|c| c == ')').unwrap();
                    let coldef_str = command_str.get(column_def_begin_idx..column_def_end_idx).unwrap().to_string();
                    let col_strs = coldef_str.split(',');

                    for col_str in col_strs {
                        println!("{}", col_str);
                        let mut parts = col_str.split_ascii_whitespace();
                        let mut col: Column = Column {
                            length: 0,
                            name: parts.nth(0).unwrap().to_string(),
                            data_type: Datatype::from_str(parts.nth(0).unwrap()).unwrap()
                        };
                        let len = parts.nth(0);
                        if len.is_some() {
                            if col.data_type.has_len() {
                                col.length = len.unwrap().parse().unwrap();
                            } else {
                                return Err(format!("ERROR: Datatype '{}' does not accept a length parameter", col.data_type.as_str()));
                            }
                        } else if col.data_type.has_len() {
                            return Err(format!("ERROR: Datatype '{}' requires a length parameter", col.data_type.as_str()));
                        }
                        
                        column_definitions.push(col);
                    }

                    return Ok(Command::Create(CreateCommand {
                        table_definition: TableDefinition { 
                            name: String::from(parts.nth(0).unwrap()),
                            column_defs: column_definitions
                        }
                    }))
                } else {
                    return Err(format!("ERROR: syntax error at or near '{}'", object));
                }
            },
            _ => { Err(String::from("Unable to parse command")) }
        }
    }
}
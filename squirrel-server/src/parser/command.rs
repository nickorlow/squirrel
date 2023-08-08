use std::collections::{HashMap, HashSet};

use crate::table::table_definition::ColumnDefinition;
use crate::{Datatype, TableDefinition};
use anyhow::anyhow;

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    Select(SelectCommand),
    Create(CreateCommand),
    Insert(InsertCommand),
    Delete,
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateCommand {
    pub table_definition: TableDefinition,
}

#[derive(Debug, Eq, PartialEq)]
pub struct InsertCommand {
    pub table_name: String,
    pub items: HashMap<String, InsertItem>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SelectCommand {
    pub table_name: String,
    // TODO Later: pub column_names: Vec<String>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct InsertItem {
    pub column_name: String,
    pub column_value: String,
}

enum CreateParserState {
    Object,
    TableName,
    ColumnName,
    ColumnDefinitions,
    ColumnDatatype,
    ColumnDefinitionEnd,
    ColumnLength,
    Semicolon,
}

enum SelectParserState {
    Wildcard, // Temporary, col selection coming soon
    FromKeyword,
    TableName,
    Semicolon,
}

enum InsertParserState {
    IntoKeyword,
    TableName,
    ColumnListBegin,
    ColumnName,
    ColumnNameEnd,
    ValuesKeyword,
    ValuesListBegin,
    Value,
    ValueEnd,
    Semicolon,
}

pub fn tokenizer(text: String) -> Vec<String> {
    let parts = HashSet::from([' ', ',', ';', '(', ')']);
    let mut tokens: Vec<String> = vec![];
    let mut cur_str = String::new();
    let mut in_quotes = false;

    for cur_char in text.chars() {
        if cur_char == '\"' {
            in_quotes = !in_quotes;
        }

        if !in_quotes && parts.contains(&cur_char) {
            if !cur_str.is_empty() {
                tokens.push(cur_str);
                cur_str = String::new();
            }
            if cur_char != ' ' {
                tokens.push(cur_char.to_string());
            }
        } else {
            cur_str.push(cur_char);
        }
    }

    tokens
}

impl Command {
    fn parse_insert_command(tokens: &mut Vec<String>) -> ::anyhow::Result<Command> {
        let mut state: InsertParserState = InsertParserState::IntoKeyword;

        let mut table_name = String::new();
        let mut column_name = String::new();
        let mut column_val = String::new();

        let mut column_list: Vec<String> = vec![];
        let mut value_list: Vec<String> = vec![];

        while let Some(token) = &tokens.pop() {
            match state {
                InsertParserState::IntoKeyword => {
                    if !token.eq_ignore_ascii_case("INTO") {
                        return Err(anyhow!("Expected to find INTO at or near '{}'", token));
                    } else {
                        state = InsertParserState::TableName;
                    }
                }
                InsertParserState::TableName => {
                    table_name = token.to_string();
                    state = InsertParserState::ColumnListBegin;
                }
                InsertParserState::ColumnListBegin => {
                    if token != "(" {
                        return Err(anyhow!(
                            "Unexpected token at or near '{}'. Expected start of column list",
                            token
                        ));
                    }
                    state = InsertParserState::ColumnName;
                }
                InsertParserState::ColumnName => {
                    column_name = token.to_string();
                    state = InsertParserState::ColumnNameEnd;
                }
                InsertParserState::ColumnNameEnd => {
                    if token == "," {
                        state = InsertParserState::ColumnName;
                    } else if token == ")" {
                        state = InsertParserState::ValuesKeyword;
                    } else {
                        return Err(anyhow!(
                            "Unexpected token at or near '{}'. Expected comma or rparen.",
                            token
                        ));
                    }
                    column_list.push(column_name.clone());
                }
                InsertParserState::ValuesKeyword => {
                    if token != "VALUES" {
                        return Err(anyhow!(
                            "Unexpected token at or near '{}'. Expected 'VALUES'.",
                            token
                        ));
                    }
                    state = InsertParserState::ValuesListBegin;
                }
                InsertParserState::ValuesListBegin => {
                    if token != "(" {
                        return Err(anyhow!(
                            "Unexpected token at or near '{}'. Expected start of values list",
                            token
                        ));
                    }
                    state = InsertParserState::Value;
                }
                InsertParserState::Value => {
                    column_val = token.to_string();
                    state = InsertParserState::ValueEnd;
                }
                InsertParserState::ValueEnd => {
                    if token == "," {
                        state = InsertParserState::Value;
                    } else if token == ")" {
                        state = InsertParserState::Semicolon;
                    } else {
                        return Err(anyhow!(
                            "Unexpected token at or near '{}'. Expected comma or rparen.",
                            token
                        ));
                    }

                    value_list.push(column_val.clone());
                }
                InsertParserState::Semicolon => {
                    if token != ";" {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));
                    } else {
                        let mut insert_item_list: HashMap<String, InsertItem> = HashMap::new();
                        for item in column_list.iter().zip(&mut value_list.iter_mut()) {
                            let (col_name, value) = item;

                            insert_item_list.insert(
                                col_name.clone().trim().to_string(),
                                InsertItem {
                                    column_name: col_name.trim().to_string(),
                                    column_value: value.trim().to_string(),
                                },
                            );
                        }
                        return Ok(Command::Insert(InsertCommand {
                            table_name,
                            items: insert_item_list,
                        }));
                    }
                }
            }
        }

        Err(anyhow!("Unexpected end of input"))
    }

    fn parse_select_command(tokens: &mut Vec<String>) -> ::anyhow::Result<Command> {
        let mut state: SelectParserState = SelectParserState::Wildcard;

        // intermediate tmp vars
        let mut table_name = String::new();

        while let Some(token) = &tokens.pop() {
            match state {
                SelectParserState::Wildcard => {
                    if token != "*" {
                        return Err(anyhow!("Expected to find selection at or near '{}' (SQUIRREL does not support column seletion)", token));
                    } else {
                        state = SelectParserState::FromKeyword;
                    }
                }
                SelectParserState::FromKeyword => {
                    if !token.eq_ignore_ascii_case("FROM") {
                        return Err(anyhow!("Expected to find FROM at or near '{}'", token));
                    } else {
                        state = SelectParserState::TableName;
                    }
                }
                SelectParserState::TableName => {
                    table_name = token.to_string();
                    state = SelectParserState::Semicolon;
                }
                SelectParserState::Semicolon => {
                    if token != ";" {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));
                    } else {
                        return Ok(Command::Select(SelectCommand { table_name }));
                    }
                }
            }
        }

        Err(anyhow!("Unexpected end of input"))
    }

    fn parse_create_command(tokens: &mut Vec<String>) -> ::anyhow::Result<Command> {
        let mut state: CreateParserState = CreateParserState::Object;
        let mut col_defs: Vec<ColumnDefinition> = vec![];

        // intermediate tmp vars
        let mut table_name = String::new();
        let mut data_type: Option<Datatype> = None;
        let mut length = 0;
        let mut col_name = String::new();

        while let Some(token) = &tokens.pop() {
            match state {
                CreateParserState::Object => match token.to_uppercase().as_str() {
                    "TABLE" => {
                        state = CreateParserState::TableName;
                    }
                    _ => return Err(anyhow!("Can't create object of type '{}'", token.as_str())),
                },
                CreateParserState::TableName => {
                    state = CreateParserState::ColumnDefinitions;
                    table_name = token.to_string();
                }
                CreateParserState::ColumnDefinitions => {
                    if token != "(" {
                        return Err(anyhow!("Could not find column list"));
                    } else {
                        state = CreateParserState::ColumnName;
                    }
                }
                CreateParserState::ColumnName => {
                    col_name = token.to_string();
                    state = CreateParserState::ColumnDatatype;
                }
                CreateParserState::ColumnDatatype => {
                    let dtype = Datatype::parse_from_str(token)?;
                    if dtype.has_len() {
                        state = CreateParserState::ColumnLength;
                    } else {
                        state = CreateParserState::ColumnDefinitionEnd;
                    }
                    data_type = Some(dtype);
                }
                CreateParserState::ColumnLength => {
                    length = token.parse()?;
                    state = CreateParserState::ColumnDefinitionEnd;
                }
                CreateParserState::ColumnDefinitionEnd => {
                    if let Some(data_type_val) = data_type {
                        let column_def = ColumnDefinition {
                            data_type: data_type_val,
                            length,
                            name: col_name,
                        };

                        length = 0;
                        col_name = String::new();
                        data_type = None;

                        col_defs.push(column_def);

                        match token.as_str() {
                            "," => {
                                state = CreateParserState::ColumnName;
                            }
                            ")" => {
                                state = CreateParserState::Semicolon;
                            }
                            _ => return Err(anyhow!("Expected end")),
                        }
                    } else {
                        return Err(anyhow!("Could not find datatype for column {}", col_name));
                    }
                }
                CreateParserState::Semicolon => {
                    if token != ";" {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));
                    } else {
                        return Ok(Command::Create(CreateCommand {
                            table_definition: TableDefinition {
                                name: table_name,
                                column_defs: col_defs,
                            },
                        }));
                    }
                }
            }
        }

        Err(anyhow!("Unexpected end of input"))
    }

    pub fn from_string(command_str: String) -> ::anyhow::Result<Command> {
        let mut tokens: Vec<String> = tokenizer(command_str);
        tokens.reverse();
        if let Some(token) = tokens.pop() {
            return match token.to_uppercase().as_str() {
                "CREATE" => Self::parse_create_command(&mut tokens),
                "INSERT" => Self::parse_insert_command(&mut tokens),
                "SELECT" => Self::parse_select_command(&mut tokens),
                _ => Err(anyhow!("Unknown command '{}'", token)),
            };
        }

        Err(anyhow!("Unexpected end of statement"))
    }
}

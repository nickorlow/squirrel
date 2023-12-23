use std::collections::{HashMap, HashSet};
use std::mem;

use crate::table::table_definition::ColumnDefinition;
use crate::{Datatype, TableDefinition};
use anyhow::anyhow;

#[derive(Debug, Eq, PartialEq)]
pub enum Command {
    Select(SelectCommand),
    Create(CreateCommand),
    Insert(InsertCommand),
    Delete(DeleteCommand),
}

#[derive(Debug, Eq, PartialEq)]
pub struct CreateCommand {
    pub table_definition: TableDefinition,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DeleteCommand {
    pub table_name: String,
    pub logic_expression: Option<LogicExpression>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct InsertCommand {
    pub table_name: String,
    pub items: HashMap<String, InsertItem>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct SelectCommand {
    pub table_name: String,
    pub column_names: Vec<String>,
    pub logic_expression: Option<LogicExpression>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct InsertItem {
    pub column_name: String,
    pub column_value: String,
}

#[derive(Debug, Eq, PartialEq, Clone)]
enum LogicalOperator {
    Equal,
    GreaterThan,
    LessThan,
    GreaterThanEqualTo,
    LessThanEqualTo,
    And,
    Or,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum LogicValue {
    StringValue(String),
    U8Value(u8),
    BoolValue(bool),
    ColumnName(String),
}

#[derive(Debug, Eq, PartialEq)]
pub enum LogicSide {
   // pub expression: LogicExpression,
    Value(LogicValue)
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct LogicExpression {
    pub left_hand: LogicValue,
    pub right_hand: LogicValue,
    pub operator: LogicalOperator
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
    ColumnName,
    ColumnNameCommaOrFrom,
    TableName,
    WhereKeywordOrSemicolon,
    Semicolon,
}

enum DeleteParserState {
    FromKeyword,
    TableName,
    WhereKeywordOrSemicolon,
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

#[derive(Debug)]
enum LogicExpressionParserState {
   NumberOrQuoteOrColname,
   StringValue,
   EndQuote,
   Operator,
}


pub fn tokenizer(text: String) -> Vec<String> {
    let parts = HashSet::from([' ', ',', ';', '(', ')', '\'']);
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
    tokens.push(cur_str);

    tokens
}

impl LogicValue {
    pub fn from_string(string: String) -> ::anyhow::Result<LogicValue> {
        let test = string.parse::<u8>();
        match test {
           Ok(u8_val) => {
               return Ok(LogicValue::U8Value(u8_val));
           },
           Err(_) => {
               let res = string.trim_matches(char::from(0));
               return Ok(LogicValue::StringValue(res.to_string()));
           }, 
        }  
    }
}

impl LogicExpression {
    pub fn is_valid(&self) -> bool {
        return mem::discriminant(&self.left_hand) == mem::discriminant(&self.right_hand); 
    }
    
    pub fn is_evaluatable(&self) -> bool {
        return mem::discriminant(&self.left_hand) != mem::discriminant(&LogicValue::ColumnName(String::from(""))) &&  
                    mem::discriminant(&self.right_hand) != mem::discriminant(&LogicValue::ColumnName(String::from("")));
    }

    pub fn fill_values(&mut self, hmap: HashMap<String, LogicValue>) -> ::anyhow::Result<()> {
        for (name, value) in hmap {
            if self.left_hand == LogicValue::ColumnName(name.clone()) {
                self.left_hand = value.clone();
            }
            if self.right_hand == LogicValue::ColumnName(name.clone()) {
                self.right_hand = value.clone();
            }
        }
        Ok(())
    }

    pub fn evaluate(&self) -> ::anyhow::Result<bool> {
        if !self.is_evaluatable() {
            return Err(anyhow!("Logical expression has not been properly filled. (Do you have a typo in a column name?)")); 
        }
        if !self.is_valid() {
            return Err(anyhow!("Logical expression is comparing 2 differing datatypes")); 
        }
        println!("{:?}", self);
        match self.left_hand {
            LogicValue::StringValue(_) => {
                return self.evaluate_string();
            }
            LogicValue::BoolValue(_) => {
                return self.evaluate_bool();
            }
            LogicValue::U8Value(_) => {
                return self.evaluate_u8();
            }
            LogicValue::ColumnName(_) => {
                return Err(anyhow!("Cannot compare names of 2 columns, only values"));
            }
        }
    }

    fn evaluate_string(&self) -> ::anyhow::Result<bool> {
        match self.operator {
            LogicalOperator::Equal => {
                return Ok(self.left_hand == self.right_hand);
            }
            _ => {
                return Err(anyhow!("Invalid operator for datatype varchar"));
            }
        }
    }

    fn evaluate_bool(&self) -> ::anyhow::Result<bool>{
        match self.operator {
            LogicalOperator::Equal => {
                return Ok(self.left_hand == self.right_hand);
            }
            LogicalOperator::And => {
                if let LogicValue::BoolValue(left) = self.left_hand {
                    if let LogicValue::BoolValue(right) = self.right_hand {
                        return Ok(left && right);
                    }
                }
                return Err(anyhow!("Mismatched datatypes"));
            }
            LogicalOperator::Or => {
                if let LogicValue::BoolValue(left) = self.left_hand {
                    if let LogicValue::BoolValue(right) = self.right_hand {
                        return Ok(left || right);
                    }
                }
                return Err(anyhow!("Mismatched datatypes"));
            }
            _ => {
                return Err(anyhow!("Invalid operator for datatype bool"));
            }
        }
    }

    fn evaluate_u8(&self) -> ::anyhow::Result<bool>{
        match self.operator {
            LogicalOperator::Equal => {
                return Ok(self.left_hand == self.right_hand);
            }
            LogicalOperator::GreaterThan => {
                if let LogicValue::U8Value(left) = self.left_hand {
                    if let LogicValue::U8Value(right) = self.right_hand {
                        return Ok(left > right);
                    }
                }
                return Err(anyhow!("Mismatched datatypes"));
            }
            LogicalOperator::LessThan => {
                if let LogicValue::U8Value(left) = self.left_hand {
                    if let LogicValue::U8Value(right) = self.right_hand {
                        return Ok(left < right);
                    }
                }
                return Err(anyhow!("Mismatched datatypes"));
            }
            LogicalOperator::GreaterThanEqualTo => {
                if let LogicValue::U8Value(left) = self.left_hand {
                    if let LogicValue::U8Value(right) = self.right_hand {
                        return Ok(left >= right);
                    }
                }
                return Err(anyhow!("Mismatched datatypes"));
            }
            LogicalOperator::LessThanEqualTo => {
                if let LogicValue::U8Value(left) = self.left_hand {
                    if let LogicValue::U8Value(right) = self.right_hand {
                        return Ok(left <= right);
                    }
                }
                return Err(anyhow!("Mismatched datatypes"));
            }
            _ => {
                return Err(anyhow!("Invalid operator for datatype integer"));
            }
        }
    }
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

    fn parse_logic_expression(tokens: &mut Vec<String>) -> ::anyhow::Result<LogicExpression> {
        let mut state: LogicExpressionParserState = LogicExpressionParserState::NumberOrQuoteOrColname;
        let mut left_hand: Option<LogicValue> = None;
        let mut right_hand: Option<LogicValue> = None;
        let mut operator: Option<LogicalOperator> = None;

        while let Some(token) = &tokens.pop() {
            match state {
                LogicExpressionParserState::NumberOrQuoteOrColname => {
                    if token == "'" {
                        state = LogicExpressionParserState::StringValue;
                    } else {
                        let test = token.parse::<u8>();
                         match test {
                            Ok(u8_val) => {
                                if left_hand.is_none() {
                                    left_hand = Some(LogicValue::U8Value(u8_val)); 
                                    state = LogicExpressionParserState::Operator;
                                } else {
                                    right_hand = Some(LogicValue::U8Value(u8_val)); 
                                    return Ok(LogicExpression {left_hand: left_hand.unwrap(), right_hand: right_hand.unwrap(), operator: operator.unwrap()});
                                } 
                            },
                            Err(_) => {
                                if left_hand.is_none() {
                                    left_hand = Some(LogicValue::ColumnName(token.to_string())); 
                                    state = LogicExpressionParserState::Operator;
                                } else {
                                    right_hand = Some(LogicValue::ColumnName(token.to_string())); 
                                    return Ok(LogicExpression {left_hand: left_hand.unwrap(), right_hand: right_hand.unwrap(), operator: operator.unwrap()});
                                } 
                            }, 
                        }  
                    }

                }
                LogicExpressionParserState::StringValue => {
                    let mut value:  Option<LogicValue> = None;
                    if token == "'" {
                        value = Some(LogicValue::StringValue("".to_string()));
                    } else {
                        value = Some(LogicValue::StringValue(token.to_string()));
                    }
                    if left_hand.is_none() {
                       left_hand = value; 
                    } else {
                       right_hand = value; 
                    } 
                    state = LogicExpressionParserState::EndQuote;
                }
                LogicExpressionParserState::EndQuote => {
                    if token == "'" {
                        if right_hand.is_none() {
                            state = LogicExpressionParserState::Operator; 
                        } else {
                            return Ok(LogicExpression {left_hand: left_hand.unwrap(), right_hand: right_hand.unwrap(), operator: operator.unwrap()});
                        } 
                    } else {
                        return Err(anyhow!("Expected end quote at or near {}", token));
                    }
                }
                LogicExpressionParserState::Operator => {
                    operator = match token.as_str() {
                        "OR" => Some(LogicalOperator::Or),
                        "AND" => Some(LogicalOperator::And),
                        "=" => Some(LogicalOperator::Equal),
                        ">" => Some(LogicalOperator::GreaterThan),
                        "<" => Some(LogicalOperator::LessThan),
                        ">=" => Some(LogicalOperator::GreaterThanEqualTo),
                        "<=" => Some(LogicalOperator::LessThanEqualTo),
                        _ => return Err(anyhow!("Unknown operator {}", token))
                    };
                    state = LogicExpressionParserState::NumberOrQuoteOrColname;
                }
            }
        }

        Err(anyhow!("Unexpected end of input"))
    }

    fn parse_select_command(tokens: &mut Vec<String>) -> ::anyhow::Result<Command> {
        let mut state: SelectParserState = SelectParserState::ColumnName;

        // intermediate tmp vars
        let mut table_name = String::new();
        let mut column_names: Vec<String> = vec![];
        let mut logic_expression: Option<LogicExpression> = None;

        while let Some(token) = &tokens.pop() {
            match state {
                SelectParserState::ColumnName => {
                    if token.eq_ignore_ascii_case("FROM") {
                        return Err(anyhow!("Did not expect FROM keyword at or near '{}'", token));
                    } else {
                        column_names.push(token.clone());
                        state = SelectParserState::ColumnNameCommaOrFrom;
                    }
                }
                SelectParserState::ColumnNameCommaOrFrom => {
                    if token == "," {
                        state = SelectParserState::ColumnName;
                    } else if token.eq_ignore_ascii_case("FROM") {
                        state = SelectParserState::TableName;
                    } else {
                        return Err(anyhow!("Expected comma or FROM keyword at or near '{}'", token));
                    }
                }
                SelectParserState::TableName => {
                    table_name = token.to_string();
                    state = SelectParserState::WhereKeywordOrSemicolon;
                }
                SelectParserState::WhereKeywordOrSemicolon => {
                    if token == ";" {
                        return Ok(Command::Select(SelectCommand { table_name, column_names, logic_expression: None }));
                    } else if token == "WHERE" {
                        logic_expression = Some(Self::parse_logic_expression(tokens)?);
                        state = SelectParserState::Semicolon;
                    } else {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));

                    }
                }
                SelectParserState::Semicolon => {
                    if token != ";" {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));
                    } else {
                        return Ok(Command::Select(SelectCommand { table_name, column_names, logic_expression }));
                    }
                }
            }
        }

        Err(anyhow!("Unexpected end of input"))
    }


    fn parse_delete_command(tokens: &mut Vec<String>) -> ::anyhow::Result<Command> {
        let mut state: DeleteParserState = DeleteParserState::FromKeyword;

        // intermediate tmp vars
        let mut table_name = String::new();
        let mut logic_expression: Option<LogicExpression> = None;

        while let Some(token) = &tokens.pop() {
            match state {
                DeleteParserState::FromKeyword => {
                    if !token.eq_ignore_ascii_case("FROM") {
                        return Err(anyhow!("Expected FROM keyword at or near '{}'", token));
                    } else {
                        state = DeleteParserState::TableName;
                    }
                }
                DeleteParserState::TableName => {
                    table_name = token.to_string();
                    state = DeleteParserState::WhereKeywordOrSemicolon;
                }
                DeleteParserState::WhereKeywordOrSemicolon => {
                    if token == ";" {
                        return Ok(Command::Delete(DeleteCommand { table_name, logic_expression: None }));
                    } else if token == "WHERE" {
                        logic_expression = Some(Self::parse_logic_expression(tokens)?);
                        state = DeleteParserState::Semicolon;
                    } else {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));
                    }
                }
                DeleteParserState::Semicolon => {
                    if token != ";" {
                        return Err(anyhow!("Expected semicolon at or near '{}'", token));
                    } else {
                        return Ok(Command::Delete(DeleteCommand { table_name, logic_expression }));
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
                "DELETE" => Self::parse_delete_command(&mut tokens),
                _ => Err(anyhow!("Unknown command '{}'", token)),
            };
        }

        Err(anyhow!("Unexpected end of statement"))
    }
    
    pub fn le_from_string(command_str: String) -> ::anyhow::Result<LogicExpression> {
        let mut tokens: Vec<String> = tokenizer(command_str.clone());
        println!("{}", command_str);
        println!("{:?}", tokens);
        tokens.reverse();
        return Ok(Self::parse_logic_expression(&mut tokens)?);
    }
}

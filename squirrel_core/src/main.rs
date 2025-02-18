pub mod parser;
pub mod table;

pub use crate::parser::command::Command;
use crate::parser::command::{CreateCommand, InsertCommand, SelectCommand, DeleteCommand, LogicExpression, InsertItem, DataValue, FunctionCall, ValueExpression};
pub use crate::table::datatypes::Datatype;
pub use crate::table::table_definition::{ColumnDefinition, TableDefinition};

use anyhow::anyhow;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread;
use std::collections::HashMap;
use std::cmp;

fn main() { 
}

#[test]
fn value_expression() -> anyhow::Result<()> {
    let tests = HashMap::from([
        ( 
            "TEST()",
            Ok(ValueExpression::FunctionCall(FunctionCall { function_name: String::from("TEST"), parameters: vec![]}))
        ),
        ( 
            "TEST_TWO()",
            Ok(ValueExpression::FunctionCall(FunctionCall { function_name: String::from("TEST_TWO"), parameters: vec![]}))
        ),
        (
            "\"Name\'",
            Err(anyhow!("Error"))
        ),
        (
            "id",
            Ok(ValueExpression::ColumnName(String::from("id")))
        ),
        (
            "55",
            Ok(ValueExpression::DataValue(DataValue::U8Value(55)))
        ),
        (
            "\"Name\"",
            Ok(ValueExpression::DataValue(DataValue::StringValue(String::from("Name"))))
        ),

    ]);

    for (string, expected) in tests {
        match expected {
            Ok(expected_res) => {
                assert_eq!(
                    parser::command::Command::value_expression_from_string(String::from(string)).unwrap(), 
                    expected_res); 
            },
            Err(_) => {
                assert_eq!(
                    parser::command::Command::value_expression_from_string(String::from(string)).is_ok(),
                    false
                );
            }
        }
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
                    column_value: "Test".to_string(),
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
                    column_value: "Firstname, Lastname".to_string(),
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

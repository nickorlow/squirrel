use crate::Datatype;

pub struct Column {
    pub name: String,
    pub data_type: Datatype,
    pub length: u16 // used for char(n), varchar(n)
}

pub struct TableDefinition {
    pub name: String,
    pub column_defs: Vec<Column>,
}
use crate::Datatype;

#[derive(Debug, Eq, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: Datatype,
    pub length: usize, // used for char(n), varchar(n)
}

#[derive(Debug, Eq, PartialEq)]
pub struct TableDefinition {
    pub name: String,
    pub column_defs: Vec<ColumnDefinition>,
}

impl TableDefinition {
    pub fn get_byte_size(&self) -> usize {
        let mut sum: usize = 0;
        for col_def in self.column_defs.iter() {
            // TODO HACK FIXME
            // We should keep track of length
            // even for built-in datatypes.
            sum += if col_def.length > 0 {
                col_def.length
            } else {
                1
            };
        }
        return sum;
    }
}

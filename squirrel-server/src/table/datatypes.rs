pub enum Datatype {
    Integer,
    CharacterVarying,
}

impl Datatype {
    pub fn as_str(&self) -> &'static str {
        match self {
            Datatype::CharacterVarying => "varchar",
            Datatype::Integer => "integer"
        }
    }

    pub fn has_len(&self) -> bool {
        match self {
            Datatype::CharacterVarying => true,
            Datatype::Integer => false
        }
    }

    pub fn from_str(string: &str) -> Result<Datatype, String> {
        match string {
            "varchar" => return Ok(Datatype::CharacterVarying),
            "character varying" => return Ok(Datatype::CharacterVarying),
            "integer" => return Ok(Datatype::Integer),
            "int" => return Ok(Datatype::Integer),
            "int4" => return Ok(Datatype::Integer),
            _ => {return Err(String::from("Undefined data type"))}
        }
    }
}
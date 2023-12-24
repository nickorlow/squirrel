use anyhow::anyhow;

#[derive(Debug, Eq, PartialEq)]
pub enum Datatype {
    Integer,
    CharacterVarying,
}

impl Datatype {
    pub fn as_str(&self) -> &'static str {
        match self {
            Datatype::CharacterVarying => "varchar",
            Datatype::Integer => "integer",
        }
    }

    pub fn has_len(&self) -> bool {
        match self {
            Datatype::CharacterVarying => true,
            Datatype::Integer => false,
        }
    }

    pub fn to_bytes(&self, data_val: String) -> ::anyhow::Result<Vec<u8>> {
        match self {
            Datatype::CharacterVarying => {
                let mut str_bytes = data_val.as_bytes().to_vec();
                Ok(str_bytes)
            }
            Datatype::Integer => {
                let val = data_val.parse::<u8>()?;
                Ok(vec![val])
            }
        }
    }

    pub fn from_bytes(&self, data_val: &[u8]) -> ::anyhow::Result<String> {
        match self {
            Datatype::CharacterVarying => {
                let str_val = String::from_utf8(data_val.to_vec())?;
                Ok(str_val)
            }
            Datatype::Integer => {
                if let Some(val) = data_val.first() {
                    Ok(format!("{}", val))
                } else {
                    Err(anyhow!("Unable to parse Integer"))
                }
            }
        }
    }

    pub fn parse_from_str(string: &str) -> ::anyhow::Result<Datatype> {
        match string {
            "varchar" => Ok(Datatype::CharacterVarying),
            "character varying" => Ok(Datatype::CharacterVarying),
            "integer" => Ok(Datatype::Integer),
            "int" => Ok(Datatype::Integer),
            "int8" => Ok(Datatype::Integer),
            _ => Err(anyhow!("Undefined data type")),
        }
    }
}

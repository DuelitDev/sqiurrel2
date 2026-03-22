use super::codec::{encode_str, read_i64, read_f64, read_str, read_u8};
use super::error::{Result, StorageErr};
use std::io::Read;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColumnId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    Real,
    Bool,
    Text,
}

impl DataType {
    pub fn default(&self) -> DataValue {
        match self {
            Self::Int => DataValue::Int(0),
            Self::Real => DataValue::Real(0.0),
            Self::Bool => DataValue::Bool(false),
            Self::Text => DataValue::Text(Box::from("")),
        }
    }

    pub fn read_from(r: &mut impl Read) -> Result<Self> {
        match read_u8(r)? {
            1 => Ok(Self::Int),
            2 => Ok(Self::Real),
            3 => Ok(Self::Bool),
            4 => Ok(Self::Text),
            v => Err(StorageErr::Corrupted(format!("unknown data_type tag: {v}"))),
        }
    }

    pub fn encode(self) -> u8 {
        match self {
            Self::Int => 1,
            Self::Real => 2,
            Self::Bool => 3,
            Self::Text => 4,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    Int(i64),
    Real(f64),
    Bool(bool),
    Text(Box<str>),
}

impl DataValue {
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Int(_) => DataType::Int,
            Self::Real(_) => DataType::Real,
            Self::Bool(_) => DataType::Bool,
            Self::Text(_) => DataType::Text,
        }
    }

    pub fn read_from(r: &mut impl Read) -> Result<Self> {
        let tag = read_u8(r)?;
        match tag {
            1 => Ok(Self::Int(read_i64(r)?)),
            2 => Ok(Self::Real(read_f64(r)?)),
            3 => {
                let v = read_u8(r)?;
                Ok(Self::Bool(v != 0))
            }
            4 => Ok(Self::Text(read_str(r)?)),
            _ => Err(StorageErr::Corrupted(format!(
                "unknown data_value tag: {tag}"
            ))),
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::Int(v) => {
                buf.push(1);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Self::Real(v) => {
                buf.push(2);
                buf.extend_from_slice(&v.to_le_bytes());
            }
            Self::Bool(v) => {
                buf.push(3);
                buf.push(if *v { 1 } else { 0 });
            }
            Self::Text(v) => {
                buf.push(4);
                encode_str(buf, v);
            }
        }
    }
}

use super::error::{Result, StorageErr};
use crate::schema::{DataType, DataValue};
use std::io::Read;

pub struct Decoder<R: Read> {
    r: R,
}

impl<R: Read> Decoder<R> {
    pub fn new(r: R) -> Self {
        Self { r }
    }

    pub fn into_inner(self) -> R {
        self.r
    }

    pub fn u8(&mut self) -> Result<u8> {
        let mut buf = [0u8; 1];
        self.r.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    pub fn u16(&mut self) -> Result<u16> {
        let mut buf = [0u8; 2];
        self.r.read_exact(&mut buf)?;
        Ok(u16::from_le_bytes(buf))
    }

    pub fn u32(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.r.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }

    pub fn u64(&mut self) -> Result<u64> {
        let mut buf = [0u8; 8];
        self.r.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }

    pub fn i64(&mut self) -> Result<i64> {
        let mut buf = [0u8; 8];
        self.r.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }

    pub fn f64(&mut self) -> Result<f64> {
        let mut buf = [0u8; 8];
        self.r.read_exact(&mut buf)?;
        Ok(f64::from_le_bytes(buf))
    }

    pub fn bool(&mut self) -> Result<bool> {
        let b = self.u8()?;
        match b {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(StorageErr::Corrupted(format!("invalid bool value: {b}"))),
        }
    }

    pub fn text(&mut self) -> Result<Box<str>> {
        let len = self.u32()? as usize;
        let mut buf = vec![0u8; len];
        self.r.read_exact(&mut buf)?;
        String::from_utf8(buf)
            .map(|s| s.into_boxed_str())
            .map_err(|e| StorageErr::Corrupted(format!("invalid UTF-8: {e}")))
    }

    pub fn ty(&mut self) -> Result<DataType> {
        let ty_id = self.u8()?;
        match ty_id {
            0 => Ok(DataType::Nil),
            1 => Ok(DataType::Int),
            2 => Ok(DataType::Real),
            3 => Ok(DataType::Bool),
            4 => Ok(DataType::Text),
            _ => Err(StorageErr::Corrupted(format!("invalid type id: {ty_id}"))),
        }
    }

    pub fn value(&mut self, ty: DataType) -> Result<DataValue> {
        match ty {
            DataType::Nil => Ok(DataValue::Nil),
            DataType::Int => self.i64().map(DataValue::Int),
            DataType::Real => self.f64().map(DataValue::Real),
            DataType::Bool => self.bool().map(DataValue::Bool),
            DataType::Text => self.text().map(DataValue::Text),
        }
    }
}

pub struct Encoder {
    buf: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        self.buf.as_slice()
    }

    pub fn u8(&mut self, v: u8) {
        self.buf.push(v);
    }

    pub fn u16(&mut self, v: u16) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u32(&mut self, v: u32) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn u64(&mut self, v: u64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn i64(&mut self, v: i64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn f64(&mut self, v: f64) {
        self.buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn bool(&mut self, v: bool) {
        self.buf.push(if v { 1 } else { 0 });
    }

    pub fn text(&mut self, s: &str) {
        self.buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        self.buf.extend_from_slice(s.as_bytes());
    }

    pub fn ty(&mut self, ty: DataType) {
        let ty_id = match ty {
            DataType::Nil => 0,
            DataType::Int => 1,
            DataType::Real => 2,
            DataType::Bool => 3,
            DataType::Text => 4,
        };
        self.u8(ty_id);
    }

    pub fn value(&mut self, val: &DataValue) {
        match val {
            DataValue::Nil => (),
            DataValue::Int(i) => self.i64(*i),
            DataValue::Real(r) => self.f64(*r),
            DataValue::Bool(b) => self.bool(*b),
            DataValue::Text(s) => self.text(s),
        }
    }
}

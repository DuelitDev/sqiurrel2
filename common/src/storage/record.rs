use super::codec::{Decoder, Encoder};
use super::error::{Result, StorageErr};
use super::{ColId, RowId, SeqNo, TableId};
use crate::schema::{DataType, DataValue};
use std::io::{Read, Write};

fn write_rec(w: &mut impl Write, rec: &impl Recordable, seq_no: SeqNo) {
    // encode record to payload
    let mut enc = Encoder::new();
    rec.encode(&mut enc);
    let payload = enc.into_inner();
    // build header
    let mut enc = Encoder::new();
    enc.u32(payload.len() as u32 + 16);
    enc.u32(crc32fast::hash(&payload));
    enc.u32(seq_no.0);
    enc.u8(rec.tag());
    enc.u8(0); // flags
    enc.u16(0); // reserved
    let header = enc.into_inner();
    // write header and payload
    w.write_all(&header).unwrap();
    w.write_all(&payload).unwrap();
}

pub(super) fn read_rec(r: &mut impl Read) -> Result<Record> {
    let mut dec = Decoder::new(r);
    let len = dec.u32()?;
    if len < 16 {
        return Err(StorageErr::Corrupted(format!("record length too small: {len}")));
    }
    let crc = dec.u32()?;
    let tag = dec.u8()?;
    let _flags = dec.u8()?;
    let _reserved = dec.u16()?;
    // read payload to buffer
    let mut payload = vec![0; (len - 16) as usize];
    dec.into_inner().read_exact(&mut payload)?;
    // verify crc
    if crc != crc32fast::hash(&payload) {
        return Err(StorageErr::Corrupted("invalid crc".to_string()));
    }
    // decode payload according to tag
    let mut dec = Decoder::new(payload.as_slice());
    let payload = match tag {
        TableCreate::TAG => TableCreate::decode(&mut dec)?,
        TableDrop::TAG => TableDrop::decode(&mut dec)?,
        ColumnCreate::TAG => ColumnCreate::decode(&mut dec)?,
        ColumnAlter::TAG => ColumnAlter::decode(&mut dec)?,
        ColumnDrop::TAG => ColumnDrop::decode(&mut dec)?,
        RowInsert::TAG => RowInsert::decode(&mut dec)?,
        RowUpdate::TAG => RowUpdate::decode(&mut dec)?,
        RowDelete::TAG => RowDelete::decode(&mut dec)?,
        _ => return Err(StorageErr::InvalidRecordTag(tag)),
    };
    //
    Ok(payload)
}

pub enum Record {
    TableCreate(TableCreate),
    TableDrop(TableDrop),
    ColumnCreate(ColumnCreate),
    ColumnAlter(ColumnAlter),
    ColumnDrop(ColumnDrop),
    RowInsert(RowInsert),
    RowUpdate(RowUpdate),
    RowDelete(RowDelete),
}

impl Record {
    pub(super) fn write_to(&self, w: &mut impl Write, seq_no: SeqNo) {
        match self {
            Self::TableCreate(r) => write_rec(w, r, seq_no),
            Self::TableDrop(r) => write_rec(w, r, seq_no),
            Self::ColumnCreate(r) => write_rec(w, r, seq_no),
            Self::ColumnAlter(r) => write_rec(w, r, seq_no),
            Self::ColumnDrop(r) => write_rec(w, r, seq_no),
            Self::RowInsert(r) => write_rec(w, r, seq_no),
            Self::RowUpdate(r) => write_rec(w, r, seq_no),
            Self::RowDelete(r) => write_rec(w, r, seq_no),
        }
    }
}

trait Recordable: Sized {
    const TAG: u8;

    #[inline]
    fn tag(&self) -> u8 {
        Self::TAG
    }

    fn encode(&self, enc: &mut Encoder);
    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record>;
}

pub struct TableCreate {
    pub table_id: TableId,
    pub table_name: Box<str>,
}

impl Recordable for TableCreate {
    const TAG: u8 = 11;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.text(&self.table_name);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        Ok(Record::TableCreate(Self {
            table_id: TableId(dec.u64()?),
            table_name: dec.text()?,
        }))
    }
}

pub struct TableDrop {
    pub table_id: TableId,
}

impl Recordable for TableDrop {
    const TAG: u8 = 12;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        Ok(Record::TableDrop(Self { table_id: TableId(dec.u64()?) }))
    }
}

pub struct ColumnCreate {
    pub table_id: TableId,
    pub col_id: ColId,
    pub col_type: DataType,
    pub col_name: Box<str>,
}

impl Recordable for ColumnCreate {
    const TAG: u8 = 31;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.col_id.0);
        enc.ty(self.col_type);
        enc.text(&self.col_name);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        Ok(Record::ColumnCreate(Self {
            table_id: TableId(dec.u64()?),
            col_id: ColId(dec.u64()?),
            col_type: dec.ty()?,
            col_name: dec.text()?,
        }))
    }
}

pub struct ColumnAlter {
    pub table_id: TableId,
    pub col_id: ColId,
    pub new_col_type: DataType,
    pub new_col_name: Box<str>,
}

impl Recordable for ColumnAlter {
    const TAG: u8 = 32;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.col_id.0);
        enc.ty(self.new_col_type);
        enc.text(&self.new_col_name);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        Ok(Record::ColumnAlter(Self {
            table_id: TableId(dec.u64()?),
            col_id: ColId(dec.u64()?),
            new_col_type: dec.ty()?,
            new_col_name: dec.text()?,
        }))
    }
}

pub struct ColumnDrop {
    pub table_id: TableId,
    pub col_id: ColId,
}

impl Recordable for ColumnDrop {
    const TAG: u8 = 33;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.col_id.0);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        Ok(Record::ColumnDrop(Self {
            table_id: TableId(dec.u64()?),
            col_id: ColId(dec.u64()?),
        }))
    }
}

pub struct RowInsert {
    pub table_id: TableId,
    pub row_id: RowId,
    pub count: u64,
    pub values: Vec<DataValue>,
}

impl Recordable for RowInsert {
    const TAG: u8 = 51;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.row_id.0);
        enc.u64(self.count);
        for value in &self.values {
            enc.value(value);
        }
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        let table_id = TableId(dec.u64()?);
        let row_id = RowId(dec.u64()?);
        let count = dec.u64()?;
        let mut values = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let ty = dec.ty()?;
            let data = dec.value(ty)?;
            values.push(data);
        }
        Ok(Record::RowInsert(Self { table_id, row_id, count, values }))
    }
}

pub struct RowUpdate {
    pub table_id: TableId,
    pub row_id: RowId,
    pub count: u64,
    pub patches: Vec<(ColId, DataValue)>,
}

impl Recordable for RowUpdate {
    const TAG: u8 = 52;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.row_id.0);
        enc.u64(self.count);
        for (col_id, value) in &self.patches {
            enc.u64(col_id.0);
            enc.value(value);
        }
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        let table_id = TableId(dec.u64()?);
        let row_id = RowId(dec.u64()?);
        let count = dec.u64()?;
        let mut patches = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let col_id = ColId(dec.u64()?);
            let ty = dec.ty()?;
            let data = dec.value(ty)?;
            patches.push((col_id, data));
        }
        Ok(Record::RowUpdate(Self { table_id, row_id, count, patches }))
    }
}

pub struct RowDelete {
    pub table_id: TableId,
    pub row_id: RowId,
}

impl Recordable for RowDelete {
    const TAG: u8 = 53;

    fn encode(&self, enc: &mut Encoder) {
        enc.u64(self.table_id.0);
        enc.u64(self.row_id.0);
    }

    fn decode(dec: &mut Decoder<&[u8]>) -> Result<Record> {
        Ok(Record::RowDelete(Self {
            table_id: TableId(dec.u64()?),
            row_id: RowId(dec.u64()?),
        }))
    }
}

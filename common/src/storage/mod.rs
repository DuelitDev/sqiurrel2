mod codec;
mod header;
mod record;
mod state;

pub mod error;

use crate::schema::{DataType, DataValue};
use error::Result;
pub use error::StorageErr;
use header::FileHeader;
use record::*;
pub use state::{ColState, DbState, RowState, TableState};
use std::fs::File;
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ColId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SeqNo(pub u32);

#[derive(Debug)]
pub struct Storage {
    pub path: PathBuf,
    file: File,
    header: FileHeader,
    pub db: DbState,
    seq_no: u32,
}

impl Storage {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        match File::options().read(true).write(true).open(&path) {
            Ok(mut file) => {
                let header = FileHeader::read_from(&mut file)?;
                let mut storage = Self { path, file, header, db: DbState::default(), seq_no: 0 };
                storage.replay()?;
                Ok(storage)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut file = File::options()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(&path)?;
                let header = FileHeader::new();
                header.write_to(&mut file)?;
                Ok(Self { path, file, header, db: DbState::default(), seq_no: 0 })
            }
            Err(e) => Err(e.into()),
        }
    }
}

impl Storage {
    fn replay(&mut self) -> Result<()> {
        loop {
            match read_rec(&mut self.file) {
                Ok(record) => {
                    self.seq_no += 1;
                    self.db.apply(record)?;
                }
                Err(StorageErr::Io(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    break;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl Storage {
    fn next_seq(&mut self) -> SeqNo {
        self.seq_no += 1;
        SeqNo(self.seq_no)
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.db.table_names.contains_key(name)
    }

    pub fn resolve_table(&self, name: &str) -> Result<&TableState> {
        self.db.get_table(name).ok_or(StorageErr::InvalidSchema("table not found"))
    }

    pub fn create_table(&mut self, name: &str) -> Result<TableId> {
        let table_id = self.db.alloc_table_id();
        let seq = self.next_seq();
        let rec = Record::TableCreate(TableCreate {
            table_id,
            table_name: name.into(),
        });
        rec.write_to(&mut self.file, seq);
        self.db.apply(rec)?;
        Ok(table_id)
    }

    pub fn create_column(
        &mut self,
        table_id: TableId,
        name: &str,
        dt: DataType,
    ) -> Result<ColId> {
        let col_id = self.db.alloc_col_id();
        let seq = self.next_seq();
        let rec = Record::ColumnCreate(ColumnCreate {
            table_id,
            col_id,
            col_type: dt,
            col_name: name.into(),
        });
        rec.write_to(&mut self.file, seq);
        self.db.apply(rec)?;
        Ok(col_id)
    }

    pub fn insert_row(
        &mut self,
        table_id: TableId,
        values: Vec<DataValue>,
    ) -> Result<RowId> {
        let row_id = self.db.alloc_row_id();
        let count = values.len() as u64;
        let seq = self.next_seq();
        let rec = Record::RowInsert(RowInsert {
            table_id,
            row_id,
            count,
            values,
        });
        rec.write_to(&mut self.file, seq);
        self.db.apply(rec)?;
        Ok(row_id)
    }

    pub fn drop_table(&mut self, table_id: TableId) -> Result<()> {
        let seq = self.next_seq();
        let rec = Record::TableDrop(TableDrop { table_id });
        rec.write_to(&mut self.file, seq);
        self.db.apply(rec)?;
        Ok(())
    }
}

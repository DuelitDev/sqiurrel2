mod codec;
mod header;
mod record;

pub mod error;

use error::Result;
pub use error::StorageErr;
use header::FileHeader;
use meta::{ColumnMeta, TableMeta};
use record::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use crate::schema::{DataType, DataValue};

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
    tables: HashMap<TableId, TableMeta>,
    table_names: HashMap<Box<str>, TableId>,
}

impl Storage {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        match File::options().read(true).write(true).open(&path) {
            Ok(mut file) => {
                let header = FileHeader::read_from(&mut file)?;
                let mut storage = Self {
                    path,
                    file,
                    header,
                    tables: HashMap::new(),
                    table_names: HashMap::new(),
                };
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
                Ok(Self {
                    path,
                    file,
                    header,
                    tables: HashMap::new(),
                    table_names: HashMap::new(),
                })
            }
            Err(e) => Err(e.into()),
        }
    }

    pub fn create_table(&mut self, name: &str) -> Result<TableId> {
        if self.table_names.contains_key(name) {
            return Err(StorageErr::InvalidSchema("table name already exists"));
        }

        let table_id = TableId(self.header.next_table_id);
        self.header.next_table_id += 1;
        let seq_no = self.header.next_seq_no;
        self.header.next_seq_no += 1;

        let record = Record::TableCreate { table_id, name: name.into() };
        write_rec(&mut self.file, &record, seq_no)?;
        self.header.flush_to(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;

        let Record::TableCreate { table_id, name: boxed_name } = record else {
            unreachable!()
        };
        self.table_names.insert(boxed_name.clone(), table_id);
        self.tables.insert(
            table_id,
            TableMeta {
                id: table_id,
                name: boxed_name,
                alive: true,
                columns: Vec::new(),
                rows: HashMap::new(),
            },
        );

        Ok(table_id)
    }

    pub fn drop_table(&mut self, table: TableId) -> Result<()> {
        match self.tables.get(&table) {
            Some(meta) if meta.alive => {}
            _ => return Err(StorageErr::TableNotFound(table)),
        }

        let seq_no = self.header.next_seq_no;
        self.header.next_seq_no += 1;

        let record = Record::TableDrop { table_id: table };
        write_rec(&mut self.file, &record, seq_no)?;
        self.header.flush_to(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;

        let meta = self.tables.get_mut(&table).unwrap();
        meta.alive = false;
        let name = meta.name.clone();
        self.table_names.remove(&*name);

        Ok(())
    }

    pub fn table_exists(&self, name: &str) -> bool {
        self.table_names.contains_key(name)
    }

    pub fn resolve_table(&self, name: &str) -> Result<&TableMeta> {
        let table = self
            .table_names
            .get(name)
            .copied()
            .ok_or(StorageErr::InvalidSchema("table not found"))?;
        match self.tables.get(&table) {
            Some(meta) if meta.alive => Ok(meta),
            _ => Err(StorageErr::TableNotFound(table)),
        }
    }

    pub fn create_column(
        &mut self,
        table: TableId,
        name: &str,
        data_type: DataType,
    ) -> Result<ColumnId> {
        let meta = match self.tables.get(&table) {
            Some(m) if m.alive => m,
            _ => return Err(StorageErr::TableNotFound(table)),
        };
        if meta.columns.iter().any(|c| c.alive && &*c.name == name) {
            return Err(StorageErr::InvalidSchema("column name already exists"));
        }

        let column_id = ColumnId(self.header.next_col_id);
        self.header.next_col_id += 1;
        let seq_no = self.header.next_seq_no;
        self.header.next_seq_no += 1;

        let record = Record::ColumnCreate {
            table_id: table,
            column_id,
            name: name.into(),
            data_type,
        };
        write_rec(&mut self.file, &record, seq_no)?;
        self.header.flush_to(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;

        let meta = self.tables.get_mut(&table).unwrap();
        meta.columns.push(ColumnMeta {
            id: column_id,
            name: name.into(),
            data_type,
            alive: true,
        });

        Ok(column_id)
    }

    pub fn alter_column(
        &mut self,
        table: TableId,
        column: ColumnId,
        new_name: Option<&str>,
        new_data_type: Option<DataType>,
    ) -> Result<()> {
        todo!("alter_column")
    }

    pub fn drop_column(&mut self, table: TableId, column: ColumnId) -> Result<()> {
        let meta = match self.tables.get(&table) {
            Some(m) if m.alive => m,
            _ => return Err(StorageErr::TableNotFound(table)),
        };
        if !meta.columns.iter().any(|c| c.id == column && c.alive) {
            return Err(StorageErr::InvalidSchema("column not found"));
        }

        let seq_no = self.header.next_seq_no;
        self.header.next_seq_no += 1;

        let record = Record::ColumnDrop { table_id: table, column_id: column };
        write_rec(&mut self.file, &record, seq_no)?;
        self.header.flush_to(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;

        let col = self
            .tables
            .get_mut(&table)
            .unwrap()
            .columns
            .iter_mut()
            .find(|c| c.id == column)
            .unwrap();
        col.alive = false;

        Ok(())
    }

    pub fn resolve_columns(&self, table: TableId) -> Result<&[ColumnMeta]> {
        match self.tables.get(&table) {
            Some(meta) if meta.alive => Ok(&meta.columns),
            _ => Err(StorageErr::TableNotFound(table)),
        }
    }

    pub fn resolve_column(&self, table: TableId, name: &str) -> Result<&ColumnMeta> {
        let columns = self.resolve_columns(table)?;
        columns
            .iter()
            .find(|c| &*c.name == name)
            .ok_or(StorageErr::InvalidSchema("column not found"))
    }

    pub fn insert_row(
        &mut self,
        table: TableId,
        values: Vec<DataValue>,
    ) -> Result<RowId> {
        let meta = match self.tables.get(&table) {
            Some(m) if m.alive => m,
            _ => return Err(StorageErr::TableNotFound(table)),
        };
        let live_cols: Vec<_> = meta.columns.iter().filter(|c| c.alive).collect();
        if values.len() != live_cols.len() {
            return Err(StorageErr::InvalidSchema("value count mismatch"));
        }
        let pairs: Vec<(ColumnId, DataValue)> =
            live_cols.iter().map(|c| c.id).zip(values).collect();

        let row_id = RowId(self.header.next_row_id);
        self.header.next_row_id += 1;
        let seq_no = self.header.next_seq_no;
        self.header.next_seq_no += 1;

        let record =
            Record::RowInsert { table_id: table, row_id, values: pairs.clone() };
        write_rec(&mut self.file, &record, seq_no)?;
        self.header.flush_to(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;

        let meta = self.tables.get_mut(&table).unwrap();
        meta.rows.insert(
            row_id,
            crate::storage::meta::RowState {
                alive: true,
                values: pairs.into_iter().collect(),
            },
        );

        Ok(row_id)
    }

    pub fn update_row(
        &mut self,
        table: TableId,
        row: RowId,
        values: Vec<(ColumnId, DataValue)>,
    ) -> Result<()> {
        todo!("update_row")
    }

    pub fn delete_row(&mut self, table: TableId, row: RowId) -> Result<()> {
        let exists = match self.tables.get(&table) {
            Some(m) if m.alive => m.rows.get(&row).map(|r| r.alive).unwrap_or(false),
            _ => return Err(StorageErr::TableNotFound(table)),
        };
        if !exists {
            return Err(StorageErr::InvalidSchema("row not found"));
        }

        let seq_no = self.header.next_seq_no;
        self.header.next_seq_no += 1;

        let record = Record::RowDelete { table_id: table, row_id: row };
        write_rec(&mut self.file, &record, seq_no)?;
        self.header.flush_to(&mut self.file)?;
        self.file.seek(SeekFrom::End(0))?;

        self.tables.get_mut(&table).unwrap().rows.get_mut(&row).unwrap().alive = false;

        Ok(())
    }

    pub fn iter_rows(
        &self,
        table: TableId,
    ) -> Result<impl Iterator<Item = (RowId, &HashMap<ColumnId, DataValue>)>> {
        let meta = match self.tables.get(&table) {
            Some(m) if m.alive => m,
            _ => return Err(StorageErr::TableNotFound(table)),
        };
        Ok(meta.rows.iter().filter(|(_, s)| s.alive).map(|(id, s)| (*id, &s.values)))
    }
}

impl Storage {
    fn replay(&mut self) -> Result<()> {
        loop {
            match read_rec(&mut self.file)? {
                Some(record) => self.apply(record)?,
                None => break,
            }
        }
        Ok(())
    }

    fn apply(&mut self, record: Record) -> Result<()> {
        match record {
            Record::TableCreate { table_id, name } => {
                if self.tables.contains_key(&table_id) {
                    return Err(StorageErr::Corrupted(format!(
                        "duplicate table_id: {}",
                        table_id.0
                    )));
                }
                if self.table_names.contains_key(&*name) {
                    return Err(StorageErr::Corrupted(format!(
                        "duplicate table name: {name}"
                    )));
                }
                self.table_names.insert(name.clone(), table_id);
                self.tables.insert(
                    table_id,
                    TableMeta {
                        id: table_id,
                        name,
                        alive: true,
                        columns: Vec::new(),
                        rows: HashMap::new(),
                    },
                );
            }
            Record::TableDrop { table_id } => {
                let name = {
                    let meta = self.tables.get_mut(&table_id).ok_or_else(|| {
                        StorageErr::Corrupted(format!(
                            "drop unknown table: {}",
                            table_id.0
                        ))
                    })?;
                    meta.alive = false;
                    meta.name.clone()
                };
                self.table_names.remove(&*name);
            }
            Record::ColumnCreate { table_id, column_id, name, data_type } => {
                let meta = self.tables.get_mut(&table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "column_create unknown table: {}",
                        table_id.0
                    ))
                })?;
                meta.columns.push(ColumnMeta {
                    id: column_id,
                    name,
                    data_type,
                    alive: true,
                });
            }
            Record::ColumnDrop { table_id, column_id } => {
                let meta = self.tables.get_mut(&table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "column_drop unknown table: {}",
                        table_id.0
                    ))
                })?;
                let col =
                    meta.columns.iter_mut().find(|c| c.id == column_id).ok_or_else(
                        || {
                            StorageErr::Corrupted(format!(
                                "column_drop unknown column: {}",
                                column_id.0
                            ))
                        },
                    )?;
                col.alive = false;
            }
            Record::RowInsert { table_id, row_id, values } => {
                let meta = self.tables.get_mut(&table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_insert unknown table: {}",
                        table_id.0
                    ))
                })?;
                meta.rows.insert(
                    row_id,
                    crate::storage::meta::RowState {
                        alive: true,
                        values: values.into_iter().collect(),
                    },
                );
            }
            Record::RowDelete { table_id, row_id } => {
                let meta = self.tables.get_mut(&table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_delete unknown table: {}",
                        table_id.0
                    ))
                })?;
                let row = meta.rows.get_mut(&row_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_delete unknown row: {}",
                        row_id.0
                    ))
                })?;
                row.alive = false;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_path(name: &str) -> PathBuf {
        let dir = tempfile::tempdir().unwrap();
        // leak the dir so it isn't cleaned up mid-test
        let path = dir.path().join(format!("{name}.db"));
        std::mem::forget(dir);
        path
    }

    #[test]
    fn new_file_has_empty_state() {
        let path = test_path("new_empty");
        let storage = Storage::open(&path).unwrap();
        assert!(storage.tables.is_empty());
        assert!(storage.table_names.is_empty());
    }

    #[test]
    fn create_table_assigns_id() {
        let path = test_path("create_id");
        let mut storage = Storage::open(&path).unwrap();
        let id1 = storage.create_table("users").unwrap();
        let id2 = storage.create_table("posts").unwrap();
        assert_eq!(id1, TableId(1));
        assert_eq!(id2, TableId(2));
    }

    #[test]
    fn duplicate_table_name_errors() {
        let path = test_path("dup_name");
        let mut storage = Storage::open(&path).unwrap();
        storage.create_table("users").unwrap();
        assert!(storage.create_table("users").is_err());
    }

    #[test]
    fn drop_table_removes_name() {
        let path = test_path("drop_name");
        let mut storage = Storage::open(&path).unwrap();
        let id = storage.create_table("users").unwrap();
        storage.drop_table(id).unwrap();
        assert!(!storage.table_names.contains_key("users"));
        assert!(!storage.tables[&id].alive);
    }

    #[test]
    fn drop_missing_table_errors() {
        let path = test_path("drop_missing");
        let mut storage = Storage::open(&path).unwrap();
        assert!(storage.drop_table(TableId(99)).is_err());
    }

    #[test]
    fn can_reuse_name_after_drop() {
        let path = test_path("reuse_name");
        let mut storage = Storage::open(&path).unwrap();
        let id1 = storage.create_table("users").unwrap();
        storage.drop_table(id1).unwrap();
        let id2 = storage.create_table("users").unwrap();
        assert_ne!(id1, id2);
        assert!(storage.table_names.contains_key("users"));
    }

    #[test]
    fn reopen_replays_creates() {
        let path = test_path("replay_creates");
        {
            let mut s = Storage::open(&path).unwrap();
            s.create_table("users").unwrap();
            s.create_table("posts").unwrap();
        }
        {
            let s = Storage::open(&path).unwrap();
            assert!(s.table_names.contains_key("users"));
            assert!(s.table_names.contains_key("posts"));
            assert_eq!(s.tables.len(), 2);
        }
    }

    #[test]
    fn reopen_replays_drop() {
        let path = test_path("replay_drop");
        {
            let mut s = Storage::open(&path).unwrap();
            let id = s.create_table("users").unwrap();
            s.create_table("posts").unwrap();
            s.drop_table(id).unwrap();
        }
        {
            let s = Storage::open(&path).unwrap();
            assert!(!s.table_names.contains_key("users"));
            assert!(s.table_names.contains_key("posts"));
            assert!(!s.tables[&TableId(1)].alive);
            assert!(s.tables[&TableId(2)].alive);
        }
    }

    #[test]
    fn header_counters_persist() {
        let path = test_path("header_counters");
        {
            let mut s = Storage::open(&path).unwrap();
            s.create_table("a").unwrap(); // table_id=1
            s.create_table("b").unwrap(); // table_id=2
        }
        {
            let mut s = Storage::open(&path).unwrap();
            let id = s.create_table("c").unwrap();
            assert_eq!(id, TableId(3));
        }
    }
}

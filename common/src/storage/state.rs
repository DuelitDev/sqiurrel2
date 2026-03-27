use super::error::{Result, StorageErr};
use super::record::Record;
use super::{ColId, RowId, TableId};
use crate::schema::{DataType, DataValue};
use std::collections::HashMap;

// ── 컬럼 상태 ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ColState {
    pub id: ColId,
    pub name: Box<str>,
    pub data_type: DataType,
    pub alive: bool,
}

// ── 행 상태 ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RowState {
    pub id: RowId,
    pub values: HashMap<ColId, DataValue>,
    pub alive: bool,
}

// ── 테이블 상태 ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TableState {
    pub id: TableId,
    pub name: Box<str>,
    pub alive: bool,
    /// 선언 순서를 유지하기 위해 Vec 사용
    pub cols: Vec<ColState>,
    pub rows: HashMap<RowId, RowState>,
}

impl TableState {
    pub fn live_cols(&self) -> impl Iterator<Item = &ColState> {
        self.cols.iter().filter(|c| c.alive)
    }

    pub fn get_col(&self, id: ColId) -> Option<&ColState> {
        self.cols.iter().find(|c| c.id == id)
    }

    pub fn get_col_by_name(&self, name: &str) -> Option<&ColState> {
        self.cols.iter().find(|c| c.alive && &*c.name == name)
    }
}

// ── 전체 DB 상태 ───────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct DbState {
    pub tables: HashMap<TableId, TableState>,
    /// 이름 → TableId 역인덱스 (alive 테이블만 유지)
    pub table_names: HashMap<Box<str>, TableId>,
    next_table_id: u64,
    next_col_id: u64,
    next_row_id: u64,
}

impl Default for DbState {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
            table_names: HashMap::new(),
            next_table_id: 1,
            next_col_id: 1,
            next_row_id: 1,
        }
    }
}

impl DbState {
    pub fn get_table(&self, name: &str) -> Option<&TableState> {
        let id = self.table_names.get(name)?;
        self.tables.get(id)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableState> {
        let id = *self.table_names.get(name)?;
        self.tables.get_mut(&id)
    }

    pub(super) fn alloc_table_id(&mut self) -> TableId {
        let id = TableId(self.next_table_id);
        self.next_table_id += 1;
        id
    }

    pub(super) fn alloc_col_id(&mut self) -> ColId {
        let id = ColId(self.next_col_id);
        self.next_col_id += 1;
        id
    }

    pub(super) fn alloc_row_id(&mut self) -> RowId {
        let id = RowId(self.next_row_id);
        self.next_row_id += 1;
        id
    }

    pub fn apply(&mut self, record: Record) -> Result<()> {
        match record {
            // ── 테이블 ──────────────────────────────────────────────────────
            Record::TableCreate(r) => {
                self.next_table_id = self.next_table_id.max(r.table_id.0 + 1);
                if self.tables.contains_key(&r.table_id) {
                    return Err(StorageErr::Corrupted(format!(
                        "duplicate table_id: {}",
                        r.table_id.0
                    )));
                }
                self.table_names.insert(r.table_name.clone(), r.table_id);
                self.tables.insert(
                    r.table_id,
                    TableState {
                        id: r.table_id,
                        name: r.table_name,
                        alive: true,
                        cols: Vec::new(),
                        rows: HashMap::new(),
                    },
                );
            }

            Record::TableDrop(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "drop unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                self.table_names.remove(&*table.name.clone());
                table.alive = false;
            }

            // ── 컬럼 ──────────────────────────────────────────────────────
            Record::ColumnCreate(r) => {
                self.next_col_id = self.next_col_id.max(r.col_id.0 + 1);
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "column_create unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                table.cols.push(ColState {
                    id: r.col_id,
                    name: r.col_name,
                    data_type: r.col_type,
                    alive: true,
                });
            }

            Record::ColumnAlter(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "column_alter unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                let col = table.cols.iter_mut().find(|c| c.id == r.col_id).ok_or_else(
                    || {
                        StorageErr::Corrupted(format!(
                            "column_alter unknown col: {}",
                            r.col_id.0
                        ))
                    },
                )?;
                col.name = r.new_col_name;
                col.data_type = r.new_col_type;
            }

            Record::ColumnDrop(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "column_drop unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                let col = table.cols.iter_mut().find(|c| c.id == r.col_id).ok_or_else(
                    || {
                        StorageErr::Corrupted(format!(
                            "column_drop unknown col: {}",
                            r.col_id.0
                        ))
                    },
                )?;
                col.alive = false;
            }

            // ── 행 ────────────────────────────────────────────────────────
            Record::RowInsert(r) => {
                self.next_row_id = self.next_row_id.max(r.row_id.0 + 1);
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_insert unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                // live_cols 순서대로 values를 매핑
                let live_cols: Vec<ColId> =
                    table.cols.iter().filter(|c| c.alive).map(|c| c.id).collect();
                let values: HashMap<ColId, DataValue> =
                    live_cols.into_iter().zip(r.values).collect();
                table
                    .rows
                    .insert(r.row_id, RowState { id: r.row_id, values, alive: true });
            }

            Record::RowUpdate(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_update unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                let row = table.rows.get_mut(&r.row_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_update unknown row: {}",
                        r.row_id.0
                    ))
                })?;
                for (col_id, value) in r.patches {
                    row.values.insert(col_id, value);
                }
            }

            Record::RowDelete(r) => {
                let table = self.tables.get_mut(&r.table_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_delete unknown table: {}",
                        r.table_id.0
                    ))
                })?;
                let row = table.rows.get_mut(&r.row_id).ok_or_else(|| {
                    StorageErr::Corrupted(format!(
                        "row_delete unknown row: {}",
                        r.row_id.0
                    ))
                })?;
                row.alive = false;
            }
        }
        Ok(())
    }
}

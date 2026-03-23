use super::{ColumnId, RowId, TableId};
use crate::schema::{DataType, DataValue};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub id: TableId,
    pub name: Box<str>,
    pub alive: bool,
    pub columns: Vec<ColumnMeta>,
    pub rows: HashMap<RowId, RowState>,
}

#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub id: ColumnId,
    pub name: Box<str>,
    pub data_type: DataType,
    pub alive: bool,
}

#[derive(Debug, Clone)]
pub struct RowState {
    pub alive: bool,
    pub values: HashMap<ColumnId, DataValue>,
}

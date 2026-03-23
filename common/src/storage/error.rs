use super::{ColumnId, RowId, TableId};
use thiserror::Error;

pub type Result<T> = std::result::Result<T, StorageErr>;

#[derive(Debug, Error)]
pub enum StorageErr {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("corrupted: {0}")]
    Corrupted(String),

    #[error("table not found: {}", .0.0)]
    TableNotFound(TableId),

    #[error("column not found: {}", .0.0)]
    ColumnNotFound(ColumnId),

    #[error("row not found: {}", .0.0)]
    RowNotFound(RowId),

    #[error("invalid schema: {0}")]
    InvalidSchema(&'static str),

    #[error("invalid row: {0}")]
    InvalidRow(&'static str),
}

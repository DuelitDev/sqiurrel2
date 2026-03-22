use crate::query::error::QueryErr;
use crate::storage::error::StorageErr;

#[derive(Debug, serde::Serialize)]
#[serde(tag = "type", content = "data")]
pub enum QueryResult {
    Rows { columns: Vec<String>, rows: Vec<Vec<String>> },
    Count(usize),
    Success,
    Error(String),
}

impl From<QueryErr> for QueryResult {
    fn from(e: QueryErr) -> Self {
        QueryResult::Error(e.to_string())
    }
}

impl From<StorageErr> for QueryResult {
    fn from(e: StorageErr) -> Self {
        QueryResult::Error(e.to_string())
    }
}

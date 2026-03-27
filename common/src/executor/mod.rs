use crate::query::{Expr, QueryErr, Stmt};
use crate::schema::{DataType, DataValue};
use crate::storage::{Storage, StorageErr};

#[derive(serde::Serialize)]
pub enum QueryResult {
    Rows { columns: Vec<String>, rows: Vec<Vec<String>> },
    Count(usize),
    Success,
}

#[derive(Debug, thiserror::Error)]
pub enum SQRLErr {
    #[error("{0}")]
    StorageErr(#[from] StorageErr),

    #[error("{0}")]
    QueryErr(#[from] QueryErr),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableAlreadyExists(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("column count mismatch: expected {expected}, got {got}")]
    ColumnCountMismatch { expected: usize, got: usize },
}

pub type Result<T> = std::result::Result<T, SQRLErr>;

pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}

impl Executor {
    fn eval(&self, expr: &Expr) -> Result<DataValue> {
        match expr {
            Expr::Nil => Ok(DataValue::Nil),
            Expr::Int(i) => Ok(DataValue::Int(*i)),
            Expr::Real(r) => Ok(DataValue::Real(*r)),
            Expr::Bool(b) => Ok(DataValue::Bool(*b)),
            Expr::Text(s) => Ok(DataValue::Text(s.clone())),
            _ => todo!("unimplemented expression: {expr:?}"),
        }
    }
}

impl Executor {
    pub fn run(&mut self, stmt: Stmt) -> Result<QueryResult> {
        match stmt {
            Stmt::Create { table_name, defines, if_not_exists } => {
                self.run_create(&table_name, defines, if_not_exists)
            }
            Stmt::InsertValues { table_name, columns, rows } => {
                self.run_insert_values(&table_name, columns, rows)
            }
            Stmt::Drop { table_name, if_exists, cascade } => {
                self.run_drop(&table_name, if_exists, cascade)
            }
            _ => todo!("unimplemented statement: {stmt:?}"),
        }
    }

    fn run_create(
        &mut self,
        table_name: &str,
        defines: Vec<(Box<str>, DataType)>,
        if_not_exists: bool,
    ) -> Result<QueryResult> {
        if if_not_exists && self.storage.table_exists(table_name) {
            return Ok(QueryResult::Success);
        }
        let table_id = self.storage.create_table(table_name)?;
        for (name, dt) in defines {
            self.storage.create_column(table_id, &name, dt)?;
        }
        Ok(QueryResult::Success)
    }

    fn run_insert_values(
        &mut self,
        table_name: &str,
        columns: Vec<Box<str>>,
        rows: Vec<Vec<Expr>>,
    ) -> Result<QueryResult> {
        let table = self.storage.resolve_table(table_name)?;
        let table_id = table.id;
        let targets: Vec<_> = if columns.is_empty() {
            table.live_cols().map(|c| c.id).collect()
        } else {
            todo!("column name resolution not implemented yet")
        };
        let mut count = 0;
        for values in rows {
            if values.len() != targets.len() {
                return Err(SQRLErr::ColumnCountMismatch {
                    expected: targets.len(),
                    got: values.len(),
                });
            }
            let values = values
                .into_iter()
                .map(|v| self.eval(&v))
                .collect::<Result<Vec<_>>>()?;
            // TODO: column order & type checking
            self.storage.insert_row(table_id, values)?;
            count += 1;
        }
        Ok(QueryResult::Count(count))
    }
    fn run_drop(
        &mut self,
        table_name: &str,
        if_exists: bool,
        _cascade: bool,
    ) -> Result<QueryResult> {
        if if_exists && !self.storage.table_exists(table_name) {
            return Ok(QueryResult::Success);
        }
        let table_id = self.storage.resolve_table(table_name)?.id;
        self.storage.drop_table(table_id)?;
        Ok(QueryResult::Success)
    }
}

pub mod result;

use crate::query::parser::ValType;
use crate::query::{Expr, Lexer, Parser, Stmt};
use crate::storage::Storage;
use crate::storage::types::{DataType, DataValue};
use result::QueryResult;

pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    pub fn run(&mut self, src: &str) -> Vec<QueryResult> {
        let lexer = Lexer::new(src);
        let mut parser = match Parser::new(lexer) {
            Ok(p) => p,
            Err(e) => return vec![e.into()],
        };
        let stmts = match parser.parse() {
            Ok(s) => s,
            Err(e) => return vec![e.into()],
        };
        let mut results = vec![];
        for stmt in stmts {
            let result = match stmt {
                Stmt::Create { table, columns, if_not_exists } => {
                    self.exec_create(&table, columns, if_not_exists)
                }
                Stmt::InsertValues { table, columns, rows } => {
                    self.exec_insert_values(&table, columns, rows)
                }
                Stmt::Drop { table, if_exists, cascade: _ } => {
                    self.exec_drop(&table, if_exists)
                }
                _ => todo!(),
            };
            if matches!(result, QueryResult::Error(_)) {
                results.push(result);
                return results;
            } else {
                results.push(result);
            }
        }
        results
    }

    fn adapt_type(&self, vt: ValType) -> DataType {
        match vt {
            ValType::Int => DataType::Int,
            ValType::Real => DataType::Real,
            ValType::Bool => DataType::Bool,
            ValType::Text => DataType::Text,
        }
    }

    fn eval(expr: &Expr) -> DataValue {
        match expr {
            Expr::Int(n) => DataValue::Int(*n),
            Expr::Float(f) => DataValue::Real(*f),
            Expr::Bool(b) => DataValue::Bool(*b),
            Expr::Text(s) => DataValue::Text(s.clone()),
            Expr::Null => DataValue::Int(0),
            other => panic!("non-literal expression in INSERT VALUES: {other:?}"),
        }
    }

    fn exec_create(
        &mut self,
        table: &str,
        columns: Vec<(Box<str>, ValType)>,
        if_not_exists: bool,
    ) -> QueryResult {
        if if_not_exists && self.storage.table_exists(table) {
            return QueryResult::Success;
        }
        let table_id = match self.storage.create_table(table) {
            Ok(id) => id,
            Err(e) => return e.into(),
        };
        for (name, vt) in columns {
            let dt = self.adapt_type(vt);
            if let Err(e) = self.storage.create_column(table_id, &name, dt) {
                return e.into();
            }
        }
        QueryResult::Success
    }

    fn exec_insert_values(
        &mut self,
        table: &str,
        columns: Vec<Box<str>>,
        rows: Vec<Vec<Expr>>,
    ) -> QueryResult {
        let meta = match self.storage.resolve_table(table) {
            Ok(m) => m.clone(),
            Err(e) => return e.into(),
        };

        let target_col_ids: Vec<_> = if columns.is_empty() {
            meta.columns.iter().map(|c| c.id).collect()
        } else {
            let mut ids = Vec::with_capacity(columns.len());
            for col_name in &columns {
                match meta.columns.iter().find(|c| &*c.name == &**col_name) {
                    Some(c) => ids.push(c.id),
                    None => {
                        return QueryResult::Error(format!(
                            "column not found: {col_name}"
                        ));
                    }
                }
            }
            ids
        };

        let mut count = 0usize;
        for row_exprs in rows {
            if row_exprs.len() != target_col_ids.len() {
                return QueryResult::Error(format!(
                    "expected {} value(s), got {}",
                    target_col_ids.len(),
                    row_exprs.len()
                ));
            }

            let values: Vec<DataValue> = meta
                .columns
                .iter()
                .map(|col| match target_col_ids.iter().position(|&id| id == col.id) {
                    Some(i) => Self::eval(&row_exprs[i]),
                    None => col.data_type.default(),
                })
                .collect();

            if let Err(e) = self.storage.insert_row(meta.id, values) {
                return e.into();
            }
            count += 1;
        }
        QueryResult::Count(count)
    }

    fn exec_drop(&mut self, table: &str, if_exists: bool) -> QueryResult {
        let table = match self.storage.resolve_table(table) {
            Ok(m) => m,
            Err(_) if if_exists => return QueryResult::Success,
            Err(e) => return e.into(),
        };
        match self.storage.drop_table(table.id) {
            Ok(()) => QueryResult::Success,
            Err(e) => e.into(),
        }
    }
}

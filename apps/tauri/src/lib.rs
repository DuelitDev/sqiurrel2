use common::executor::Executor;
use common::storage::Storage;
use common::query::{Lexer, Parser};
use std::sync::Mutex;
use tauri::{Manager, State};

type DbState = Mutex<Executor>;

#[tauri::command]
fn run_query(
    state: State<DbState>,
    src: String,
) -> Result<Vec<common::executor::QueryResult>, String> {
    let lexer = Lexer::new(src.as_str());
    let mut parser = Parser::new(lexer).map_err(|e| e.to_string())?;
    let stmts = parser.parse().map_err(|e| e.to_string())?;
    let mut exec = state.lock().unwrap();
    let mut results = Vec::with_capacity(stmts.len());
    for stmt in stmts {
        let result = exec.run(stmt).map_err(|e| e.to_string())?;
        results.push(result);
    }
    Ok(results)
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            let data_dir = app.path().app_data_dir().expect("no app data dir");
            std::fs::create_dir_all(&data_dir)?;
            let db_path = data_dir.join("db.sqrl");
            let storage = Storage::open(db_path).expect("failed to open storage");
            app.manage(Mutex::new(Executor::new(storage)));
            Ok(())
        })
        .invoke_handler(tauri::generate_handler![run_query])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

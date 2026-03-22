use common::executor::Executor;
use common::storage::Storage;
use std::sync::Mutex;
use tauri::{Manager, State};

type DbState = Mutex<Executor>;

#[tauri::command]
fn run_query(
    state: State<DbState>,
    src: String,
) -> Vec<common::executor::result::QueryResult> {
    state.lock().unwrap().run(&src)
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

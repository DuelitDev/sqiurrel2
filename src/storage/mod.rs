use crate::executor::{ColumnId, RowId, TableId};
use crate::var_char::VarChar;
use std::cmp::PartialEq;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::SeekFrom;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};
use tokio::{fs, io};

#[repr(u8)]
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
pub enum DataType {
    Int = 11,
    Float = 12,
    Bool = 13,
    String = 14,
}

#[derive(PartialEq, Clone, Debug)]
pub enum DataValue {
    Int(i64),
    Float(f64),
    Bool(bool),
    VChar(VarChar),
}

impl DataType {
    pub fn as_str(self) -> &'static str {
        match self {
            DataType::Int => "Int",
            DataType::Float => "Float",
            DataType::Bool => "Bool",
            DataType::String => "String",
        }
    }

    pub fn try_from_str(value: &str) -> io::Result<Self> {
        match value {
            "Int" => Ok(DataType::Int),
            "Float" => Ok(DataType::Float),
            "Bool" => Ok(DataType::Bool),
            "String" => Ok(DataType::String),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown data type: {value}"),
            )),
        }
    }

    pub fn byte_len(self) -> u64 {
        match self {
            DataType::Int => std::mem::size_of::<i64>() as u64,
            DataType::Float => std::mem::size_of::<f64>() as u64,
            DataType::Bool => std::mem::size_of::<u8>() as u64,
            DataType::String => (crate::var_char::VAR_CHAR_CAPACITY * std::mem::size_of::<char>()) as u64,
        }
    }
}

impl DataValue {
    pub fn verify(self, data_type: DataType) -> bool {
        match self {
            DataValue::Int(_) => DataType::Int == data_type,
            DataValue::Float(_) => DataType::Float == data_type,
            DataValue::Bool(_) => DataType::Bool == data_type,
            DataValue::VChar(_) => DataType::String == data_type,
        }
    }
}

const ROWS_PER_FILE: u64 = 256;

pub async fn create_table(name: String) -> io::Result<TableId> {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    let val = hasher.finish();
    fs::create_dir(val.to_string()).await?;
    let mut file = fs::File::create(format!("{}/schema", val)).await?;
    file.write_all("LAST_ID 0000000000000000\n".as_bytes())
        .await?;
    file.write_all(format!("NAME {}\n", name).as_bytes())
        .await?;
    file.flush().await?;
    Ok(TableId(val))
}

pub async fn create_column(
    table_id: TableId,
    col_name: String,
    col_type: DataType,
) -> tokio::io::Result<ColumnId> {
    let mut hasher = DefaultHasher::new();
    col_name.hash(&mut hasher);
    let val = hasher.finish();
    let mut file = fs::File::options()
        .append(true)
        .open(format!("{}/schema", table_id.0))
        .await?;
    file.write_all(format!("COLUMN {} {} {col_name}\n", val, col_type.as_str()).as_bytes())
        .await?;
    file.flush().await?;
    Ok(ColumnId(val))
}

pub async fn write_data(fd: &mut fs::File, data: DataValue) -> io::Result<()> {
    match data {
        DataValue::Int(i) => fd.write_all(&i.to_be_bytes()).await, // 8 bytes
        DataValue::Float(f) => fd.write_all(&f.to_be_bytes()).await, // 8 bytes
        DataValue::Bool(b) => fd.write_all(&[b as u8]).await,      // 1 byte
        DataValue::VChar(s) => {
            let var_char = VarChar::try_from(s).unwrap();
            fd.write_all(&var_char.as_bytes()).await
        }
    }
}

pub async fn create_row(table_id: TableId, values: Vec<DataValue>) -> io::Result<RowId> {
    let mut schema_file = fs::File::options()
        .write(true)
        .read(true)
        .open(format!("{}/schema", table_id.0))
        .await?;
    let error = || io::Error::new(io::ErrorKind::Other, "Schema file is corrupted");
    let mut buf = String::new();
    schema_file.read_to_string(&mut buf).await?;

    let mut lines = buf.lines();
    let last_id = buf[8..24].to_string();
    let last_id = u64::from_str_radix(&last_id, 16).unwrap();

    let tb_name = lines
        .nth(1)
        .ok_or_else(error)?
        .strip_prefix("NAME ")
        .ok_or_else(error)?
        .to_string();
    let mut cols = Vec::new();
    for line in lines {
        let mut column_data = line.split(' ');
        column_data.next().ok_or_else(error)?;
        let col_id = column_data.next().ok_or_else(error)?;
        let col_type = column_data.next().ok_or_else(error)?;
        let col_name = column_data.next().ok_or_else(error)?;
        cols.push((
            col_id.to_string(),
            col_type.to_string(),
            col_name.to_string(),
        ));
    }
    let id = last_id + 1;
    let file_num = id / ROWS_PER_FILE;
    let filepath = format!("{}/{}", table_id.0, file_num);
    // write row to data file
    let mut data_file = fs::File::options()
        .create(true)
        .append(true)
        .open(filepath)
        .await?;
    for value in values {
        write_data(&mut data_file, value).await?;
    }
    // rewrite schema with updated last_id
    let schema = format!("LAST_ID {:016x}\nNAME {}\n", id, tb_name);
    schema_file.seek(SeekFrom::Start(0)).await?;
    schema_file.write_all(schema.as_bytes()).await?;
    for (col_id, col_type, col_name) in cols {
        let col = format!("COLUMN {col_id} {col_type} {col_name}\n");
        schema_file.write_all(col.as_bytes()).await?;
    }
    Ok(RowId(id))
}

pub fn get_table_hash(name: &str) -> TableId {
    let mut hasher = DefaultHasher::new();
    name.hash(&mut hasher);
    TableId(hasher.finish())
}

async fn read_schema(
    table_id: TableId,
) -> io::Result<(u64, String, Vec<(ColumnId, DataType, String)>)> {
    let mut schema_file = fs::File::open(format!("{}/schema", table_id.0)).await?;
    let mut buf = String::new();
    schema_file.read_to_string(&mut buf).await?;

    let error = || io::Error::new(io::ErrorKind::InvalidData, "Schema file is corrupted");
    let mut lines = buf.lines();

    let last_id_line = lines.next().ok_or_else(error)?;
    let last_id = last_id_line
        .strip_prefix("LAST_ID ")
        .ok_or_else(error)
        .and_then(|value| u64::from_str_radix(value, 16).map_err(|_| error()))?;

    let table_name = lines
        .next()
        .ok_or_else(error)?
        .strip_prefix("NAME ")
        .ok_or_else(error)?
        .to_string();

    let mut columns = Vec::new();
    for line in lines {
        let mut parts = line.splitn(4, ' ');

        let keyword = parts.next().ok_or_else(error)?;
        if keyword != "COLUMN" {
            return Err(error());
        }

        let column_id = parts
            .next()
            .ok_or_else(error)
            .and_then(|value| value.parse::<u64>().map_err(|_| error()))?;

        let column_type = parts
            .next()
            .ok_or_else(error)
            .and_then(DataType::try_from_str)?;

        let column_name = parts.next().ok_or_else(error)?.to_string();

        columns.push((ColumnId(column_id), column_type, column_name));
    }

    Ok((last_id, table_name, columns))
}

async fn read_data(fd: &mut fs::File, data_type: DataType) -> io::Result<DataValue> {
    match data_type {
        DataType::Int => {
            let mut buf = [0u8; 8];
            fd.read_exact(&mut buf).await?;
            Ok(DataValue::Int(i64::from_be_bytes(buf)))
        }
        DataType::Float => {
            let mut buf = [0u8; 8];
            fd.read_exact(&mut buf).await?;
            Ok(DataValue::Float(f64::from_be_bytes(buf)))
        }
        DataType::Bool => {
            let mut buf = [0u8; 1];
            fd.read_exact(&mut buf).await?;
            Ok(DataValue::Bool(buf[0] != 0))
        }
        DataType::String => {
            let mut buf = [0u8; crate::var_char::VAR_CHAR_CAPACITY * std::mem::size_of::<char>()];
            fd.read_exact(&mut buf).await?;

            let chars: [char; crate::var_char::VAR_CHAR_CAPACITY] = unsafe { std::mem::transmute(buf) };
            let len = chars
                .iter()
                .position(|ch| *ch == char::default())
                .unwrap_or(crate::var_char::VAR_CHAR_CAPACITY);

            let s = String::from_iter(chars[..len].iter());
            let vchar = VarChar::try_from(s.as_str()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "Stored string is too long")
            })?;

            Ok(DataValue::VChar(vchar))
        }
    }
}

pub async fn read_row(table_id: TableId, row_id: RowId) -> io::Result<Vec<DataValue>> {
    if row_id.0 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "RowId must start from 1",
        ));
    }

    let (last_id, _table_name, columns) = read_schema(table_id).await?;
    if row_id.0 > last_id {
        return Err(io::Error::new(io::ErrorKind::NotFound, "Row not found"));
    }

    let row_size = columns
        .iter()
        .map(|(_, data_type, _)| data_type.byte_len())
        .sum::<u64>();

    let file_num = row_id.0 / ROWS_PER_FILE;
    let row_index_in_file = (row_id.0 - 1) % ROWS_PER_FILE;
    let offset = row_index_in_file * row_size;

    let mut data_file = fs::File::open(format!("{}/{}", table_id.0, file_num)).await?;
    data_file.seek(SeekFrom::Start(offset)).await?;

    let mut row = Vec::with_capacity(columns.len());
    for (_, data_type, _) in columns {
        row.push(read_data(&mut data_file, data_type).await?);
    }

    Ok(row)
}

mod test {
    use super::*;
    #[tokio::test]
    async fn test_create_and_read() {
        std::fs::remove_dir_all("6025841138654200372").unwrap();
        let table_id = create_table("users".to_string()).await.unwrap();
        println!("Created table with ID: {}", table_id.0);
        create_column(table_id, "name".to_string(), DataType::String)
            .await
            .unwrap();
        create_column(table_id, "age".to_string(), DataType::Int)
            .await
            .unwrap();
        create_row(
            table_id,
            vec![
                DataValue::VChar(VarChar::try_from("Alice").unwrap()),
                DataValue::Int(30),
            ],
        )
        .await
        .unwrap();

        let table_id = get_table_hash("users");
        let (last_id, table_name, columns) = read_schema(table_id).await.unwrap();
        println!("Table Name: {}", table_name);
        println!("Last ID: {}", last_id);
        for (col_id, col_type, col_name) in columns {
            println!("Column ID: {}, Type: {}, Name: {}", col_id.0, col_type.as_str(), col_name);
        }
        let row = read_row(table_id, RowId(1)).await.unwrap();
        println!("Row 1: {:?}", row);
    }
}

# SQiurrel Storage Format v2

파일은 다음 순서로 구성된다.

1. File Header
2. Record
3. Record
4. ...

DB의 현재 상태는 파일 앞부분에 직접 저장하지 않는다.
대신 테이블, 컬럼, 로우에 대한 연산을 레코드로 계속 추가한다.
파일을 열 때는 처음부터 끝까지 읽으면서 현재 상태를 재구성(replay)한다.

## 엔디안과 기본 규칙

- 모든 정수와 실수는 little-endian
- 문자열은 UTF-8
- 문자열은 길이와 바이트를 함께 저장
- Rust enum 메모리 배치를 그대로 쓰지 않는다
- 모든 타입은 명시적인 태그 값으로 직렬화한다

## File Header

File Header의 크기는 64 bytes로 고정한다.

| Offset | Size | Type  | Name          | Description                  |
|-------:|-----:|:------|:--------------|:-----------------------------|
|      0 |    4 | bytes | magic         | Always 0x4c525153 ("SQRL")   |
|      4 |    1 | u8    | version       | Always 2                     |
|      5 |    1 | u8    | header_len    | Always 64                    |
|      6 |    2 | u16   | flags         | 0 on init                    |
|      8 |   56 | bytes | reserved      | Always 0                     |

replay 완료 후 다음 ID들을 계산한다:

- next_table_id: replay 중 발견한 table_id의 최댓값 + 1
- next_col_id: replay 중 발견한 col_id의 최댓값 + 1
- next_row_id: replay 중 발견한 row_id의 최댓값 + 1
- next_seq_no: replay 중 발견한 seq_no의 최댓값 + 1
- 레코드가 하나도 없으면 모두 1로 초기화한다.

## Record Header

각 레코드는 16 bytes의 Record Header와 payload로 구성된다.

| Offset | Size | Type | Name      | Description                       |
|-------:|-----:|:-----|:----------|:----------------------------------|
|      0 |    4 | u32  | total_len | length including header & payload |
|      4 |    4 | u32  | crc32     | checksum for payload              |
|      8 |    4 | u32  | seq_no    | issued from next_seq_no           |
|     12 |    1 | u8   | rec_type  | record type                       |
|     13 |    1 | u8   | flags     | 0 on init                         |
|     14 |    2 | u16  | reserved  | Always 0                          |

규칙:

- total_len은 최소 16 이상이어야 한다
- crc32는 payload에 대해서만 계산한다
- seq_no는 1씩 증가해야 한다
- 알 수 없는 rec_type은 Corruption으로 처리한다

## Record Kind

| Value | Name         |
|------:|:-------------|
|     1 | TableCreate  |
|     2 | TableDrop    |
|     3 | ColumnCreate |
|     4 | ColumnAlter  |
|     5 | ColumnDrop   |
|     6 | RowInsert    |
|     7 | RowUpdate    |
|     8 | RowDelete    |

## 기본 타입 직렬화

### DataType 태그

| Value | DataType |
|------:|:---------|
|     1 | Nil      |
|     2 | Int      |
|     3 | Real     |
|     4 | Bool     |
|     5 | Text     |

### DataValue 형식

DataValue는 다음 형식으로 저장한다.

1. tag: u8
2. payload: tag에 따라 달라짐

각 payload는 다음과 같다.

| Tag | Variant | Payload     |
|----:|:--------|:------------|
|   1 | Nil     | void        |
|   2 | Int     | i64         |
|   3 | Real    | f64         |
|   4 | Bool    | u8 (0 or 1) |
|   5 | Text    | string      |

### 문자열

문자열은 다음 형식으로 저장한다.

1. len: u32
2. bytes: len 길이의 UTF-8 바이트

빈 문자열은 len = 0으로 저장한다.

## Payload 포맷

### TableCreate

| Order | Type   | Name     |
|------:|:-------|:---------|
|     1 | u64    | table_id |
|     2 | string | name     |

규칙:

- 살아있는 테이블 이름은 유일해야 한다
- 같은 table_id가 다시 나오면 손상으로 본다

### TableDrop

| Order | Type | Name     |
|------:|:-----|:---------|
|     1 | u64  | table_id |

규칙:

- 테이블은 논리 삭제한다
- drop된 테이블은 조회 대상에서 제외한다

### ColumnCreate

| Order | Type   | Name      |
|------:|:-------|:----------|
|     1 | u64    | table_id  |
|     2 | u64    | col_id    |
|     3 | u8     | col_type  |
|     4 | string | col_name  |

규칙:

- 컬럼 순서는 생성 순서로 고정한다
- 같은 테이블 안에서 살아있는 컬럼 이름은 유일해야 한다

### ColumnAlter

| Order | Type   | Name          |
|------:|:-------|:--------------|
|     1 | u64    | table_id      |
|     2 | u64    | col_id        |
|     3 | u8     | new_col_type  |
|     4 | string | new_col_name  |

### ColumnDrop

| Order | Type | Name      |
|------:|:-----|:----------|
|     1 | u64  | table_id  |
|     2 | u64  | col_id    |

규칙:

- 컬럼은 논리 삭제한다
- 기존 row 데이터는 그대로 둔다
- 조회 시 drop된 컬럼은 제외한다

### RowInsert

| Order | Type               | Name        |
|------:|:-------------------|:------------|
|     1 | u64                | table_id    |
|     2 | u64                | row_id      |
|     3 | u32                | count       |
|     4 | repeated DataValue | values      |

규칙:

- 값의 순서는 현재 활성 컬럼 순서와 일치해야 한다
- value_count는 현재 활성 컬럼 수와 같아야 한다
- 각 값의 타입은 해당 컬럼 타입과 일치해야 한다

### RowUpdate

| Order | Type           | Name        |
|------:|:---------------|:------------|
|     1 | u64            | table_id    |
|     2 | u64            | row_id      |
|     3 | u32            | count       |
|     4 | repeated patch | patches     |

patch 형식:

| Order | Type      | Name      |
|------:|:----------|:----------|
|     1 | u64       | column_id |
|     2 | DataValue | value     |

규칙:

- row 전체가 아니라 일부 컬럼만 수정한다
- column_id는 살아있는 컬럼이어야 한다
- 값의 타입은 컬럼 타입과 일치해야 한다

### RowDelete

| Order | Type | Name     |
|------:|:-----|:---------|
|     1 | u64  | table_id |
|     2 | u64  | row_id   |

규칙:

- row는 논리 삭제한다
- 이후 rows 조회에서 제외한다

## Replay 규칙

파일을 열 때는 다음 순서로 상태를 재구성한다.

1. File Header 읽기
2. magic, version, header_len 검증
3. header 뒤부터 EOF까지 레코드 순차 스캔
4. 각 레코드의 total_len 검증
5. payload 읽기
6. crc32 검증
7. kind별 replay 수행

메모리에서 유지할 최소 상태:

- table_id에서 table meta로의 매핑
- 테이블 이름에서 table_id로의 매핑
- table_id별 활성 컬럼 목록
- table_id와 row_id로 row 상태 조회

## 손상 처리

다음 경우는 파일 손상으로 간주한다.

- magic 불일치
- version 불일치
- header_len 불일치
- total_len이 16보다 작음
- 레코드 길이가 파일 범위를 벗어남
- crc32 불일치
- 존재하지 않는 table_id를 참조하는 레코드
- 존재하지 않는 column_id를 참조하는 레코드
- 활성 컬럼 수와 RowInsert의 value_count 불일치
- 컬럼 타입과 DataValue 타입 불일치
- seq_no의 gap은 engine이 strict 모드일 때만 corruption 처리한다
- 마지막 레코드만 깨졌다면 tail corruption으로 보고 마지막 레코드만 무시할 수 있다
- 중간 레코드가 깨졌다면 전체 파일 손상으로 처리한다

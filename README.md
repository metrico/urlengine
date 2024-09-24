<img src="https://private-user-images.githubusercontent.com/1423657/365836696-9003897d-db6f-4a79-9443-9b72766b511b.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MjY5NTM1MjksIm5iZiI6MTcyNjk1MzIyOSwicGF0aCI6Ii8xNDIzNjU3LzM2NTgzNjY5Ni05MDAzODk3ZC1kYjZmLTRhNzktOTQ0My05YjcyNzY2YjUxMWIucG5nP1gtQW16LUFsZ29yaXRobT1BV1M0LUhNQUMtU0hBMjU2JlgtQW16LUNyZWRlbnRpYWw9QUtJQVZDT0RZTFNBNTNQUUs0WkElMkYyMDI0MDkyMSUyRnVzLWVhc3QtMSUyRnMzJTJGYXdzNF9yZXF1ZXN0JlgtQW16LURhdGU9MjAyNDA5MjFUMjExMzQ5WiZYLUFtei1FeHBpcmVzPTMwMCZYLUFtei1TaWduYXR1cmU9OTQyNmEwNjgyODRkYzczYmEyYjUyYjNmMDk0NmUwZTAwMDdkYzE3NzdiMjA4ZTkwM2I3NGZiMmFiMWFjMTBiMCZYLUFtei1TaWduZWRIZWFkZXJzPWhvc3QifQ.8SvmSfOk-XGK0iscfD9TCq_PK4kpAdCqfkyLRTcv7e4" width=150 >

# DuckDB URL Engine
This basic example is designed to explore [DuckDB HTTPFS](https://duckdb.org/docs/extensions/httpfs/https.html) remote read/write integrations.

### Demo
A public demo instance is available at [https://duckserver.glitch.me](https://duckserver.glitch.me)


```mermaid
sequenceDiagram
    autonumber
    DuckDB->>NodeJS: POST Request
    loop Javascript
        NodeJS->>NodeJS: WRITE
    end
    NodeJS-->>DuckDB: POST Response
    DuckDB->>NodeJS: GET Request
    loop Javascript
        NodeJS->>NodeJS: READ
    end
    NodeJS-->>DuckDB: GET Response
```

##### Features
- [x] INSERT Files via POST
- [x] SELECT Files via GET

#### Usage
##### Golang
Install and run the example service :
```
cd go/
go mod tidy
PORT=80 go run server.go
```
##### NodeJS
Install and run the example service :
```
cd nodejs/
npm install
PORT=80 npm start
```

#### 📦 DuckDB

You can COPY and SELECT from the URL Engine using extensions `json`,`csv`,`parquet`

```sql
D SET enable_http_write = 1;

D COPY (SELECT version() as version, 9999 as number) TO 'https://duckserver.glitch.me/test.json';
D SELECT * FROM read_json_auto('https://duckserver.glitch.me/test.json');
┌─────────┬────────┐
│ version │ number │
│ varchar │ int64  │
├─────────┼────────┤
│ v1.1.0  │   9999 │
└─────────┴────────┘

D COPY (SELECT version() as version, 9999 as number) TO 'https://duckserver.glitch.me/test.parquet';
D SELECT * FROM read_parquet('https://duckserver.glitch.me/test.parquet');
┌─────────┬────────┐
│ version │ number │
│ varchar │ int64  │
├─────────┼────────┤
│ v1.1.0  │   9999 │
└─────────┴────────┘

D SELECT * FROM parquet_schema('https://duckserver.glitch.me/test.parquet');
┌──────────────────────┬───────────────┬────────────┬─────────────┬───┬────────────────┬───────┬───────────┬──────────┬──────────────┐
│      file_name       │     name      │    type    │ type_length │ … │ converted_type │ scale │ precision │ field_id │ logical_type │
│       varchar        │    varchar    │  varchar   │   varchar   │   │    varchar     │ int64 │   int64   │  int64   │   varchar    │
├──────────────────────┼───────────────┼────────────┼─────────────┼───┼────────────────┼───────┼───────────┼──────────┼──────────────┤
│ https://duckserver…  │ duckdb_schema │            │             │ … │                │       │           │          │              │
│ https://duckserver…  │ version       │ BYTE_ARRAY │             │ … │ UTF8           │       │           │          │              │
│ https://duckserver…  │ number        │ INT32      │             │ … │ INT_32         │       │           │          │              │
├──────────────────────┴───────────────┴────────────┴─────────────┴───┴────────────────┴───────┴───────────┴──────────┴──────────────┤
│ 3 rows                                                                                                        11 columns (9 shown) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


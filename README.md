# DuckDB URL Engine
This basic example illustrates a simple [URL Table Engine](https://DuckDB.com/docs/en/engines/table-engines/special/url/) server for DuckDB

##### ⏱️ Why
> DuckDB is super fast and already has all the functions one could dream. What is this for?

This example is designed to understand the underlying formats and unleash imagination for integrators.

```mermaid
sequenceDiagram
    autonumber
    DuckDB->>NodeJS: POST Request
    loop Javascript
        NodeJS->>NodeJS: INSERT
    end
    NodeJS-->>DuckDB: POST Response
    DuckDB->>NodeJS: GET Request
    loop Javascript
        NodeJS->>NodeJS: SELECT
    end
    NodeJS-->>DuckDB: GET Response
```

##### Features
- [x] INSERT Files via POST
- [x] SELECT Files via GET

#### Setup
Install and run the example service :
```
npm install
npm start
```

#### 📦 DuckDB

You can COPY and SELECT from the URL Engine using extensions `json`,`csv`,`parquet`
```
D COPY (SELECT version() as version, 9999 as number) TO 'https://duckserver.glitch.me/test.parquet';
D SELECT * FROM read_json_auto('https://duckserver.glitch.me/test.parquet');
┌─────────┬────────┐
│ version │ number │
│ varchar │ int64  │
├─────────┼────────┤
│ v1.1.0  │   9999 │
└─────────┴────────┘
```


#### 📦 ClickHouse
Create a `url_engine_table` table pointed at our service :
```sql
CREATE TABLE url_engine_node
(
    `key` String,
    `value` UInt64
)
ENGINE = URL('http://127.0.0.1:3123/', JSONEachRow)
```
 
 ##### ▶️ INSERT
 ```sql
 INSERT INTO url_engine_node VALUES ('hello',1), ('world', 2)
 ```
 ##### ◀️ SELECT
 ```sql
SELECT * FROM url_engine_node

Query id: d65b429e-76aa-49f3-b376-ebd3fbc9cd1a

┌─key───┬─value─┐
│ hello │     1 │
│ world │     2 │
└───────┴───────┘

2 rows in set. Elapsed: 0.005 sec. 
 ```
 

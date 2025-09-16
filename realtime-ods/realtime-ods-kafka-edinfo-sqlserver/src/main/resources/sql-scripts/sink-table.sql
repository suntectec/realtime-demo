CREATE TABLE TestDBOOrders
(
    id              BIGINT,
    order_id        STRING,
    supplier_id     INT,
    item_id         INT,
    status          STRING,
    qty             INT,
    net_price       INT,
    issued_at       TIMESTAMP,
    completed_at    TIMESTAMP,
    spec            STRING,
    created_at      TIMESTAMP,
    updated_at      TIMESTAMP,
    _row_kind STRING,
    _ingestion_time TIMESTAMP,
    _process_time   TIMESTAMP,
    _source_time    TIMESTAMP
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:sqlserver://192.168.138.15:14330;database=TestDB',
      'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
      'username' = 'SA',
      'password' = 'YourStrong!Passw0rd',
      'table-name' = 'test.dbo.orders');
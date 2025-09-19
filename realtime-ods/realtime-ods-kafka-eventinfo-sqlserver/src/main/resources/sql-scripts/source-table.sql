CREATE TABLE OdsOrdersTopic
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
    _source_time    TIMESTAMP,
    _record_time TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'    -- reads and writes a Kafka record's timestamp
) WITH (
      'connector' = 'kafka',
      'topic' = 'ods_orders_topic',
      'properties.bootstrap.servers' = '192.168.138.15:9092',
      'properties.group.id' = 'data_group',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'json');
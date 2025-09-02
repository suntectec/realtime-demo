```
SqlServer Transaction Log Source 原始 Debezium Schema 格式：
SourceRecord{
    sourcePartition={
        server=sqlserver_transaction_log_source
    }, 
    sourceOffset={
        transaction_id=null, 
        event_serial_no=0, 
        commit_lsn=NULL, 
        change_lsn=80
    }
} 
ConnectRecord{
    topic='sqlserver_transaction_log_source.INV.orders', 
    kafkaPartition=null, 
    key=Struct{
        id=4624368
    }, 
    keySchema=Schema{
        sqlserver_transaction_log_source.INV.orders.Key:STRUCT
    }, 
    value=Struct{
        after=Struct{
            id=4624368,order_id=f407b4f6-6d7f-47b1-8d13-ca6bdc117c32,supplier_id=3425,item_id=41,status=created,qty=1200,net_price=120,issued_at=1753262105447,completed_at=1753262105447,created_at=1753262105447,updated_at=1753262105447
        },
        source=Struct{
            version=1.9.8.Final,
            connector=sqlserver,
            name=sqlserver_transaction_log_source,
            ts_ms=0,
            db=inventory,
            schema=INV,
            table=orders,
            change_lsn=80
        },
        op=r,
        ts_ms=1756085701722
    }, 
    valueSchema=Schema{
        sqlserver_transaction_log_source.INV.orders.Envelope:STRUCT
    }, 
    timestamp=null, 
    headers=ConnectHeaders(headers=)
}
```

```
# StringDebeziumDeserializationSchema
SourceRecord{sourcePartition={server=sqlserver_transaction_log_source}, sourceOffset={transaction_id=null, event_serial_no=0, commit_lsn=NULL, change_lsn=80}} ConnectRecord{topic='sqlserver_transaction_log_source.INV.orders', kafkaPartition=null, key=Struct{id=4624368}, keySchema=Schema{sqlserver_transaction_log_source.INV.orders.Key:STRUCT}, value=Struct{after=Struct{id=4624368,order_id=f407b4f6-6d7f-47b1-8d13-ca6bdc117c32,supplier_id=3425,item_id=41,status=created,qty=1200,net_price=120,issued_at=1753262105447,completed_at=1753262105447,created_at=1753262105447,updated_at=1753262105447},source=Struct{version=1.9.8.Final,connector=sqlserver,name=sqlserver_transaction_log_source,ts_ms=0,db=inventory,schema=INV,table=orders,change_lsn=80},op=r,ts_ms=1756085701722}, valueSchema=Schema{sqlserver_transaction_log_source.INV.orders.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

# JsonDebeziumDeserializationSchema
{"before":null,"after":{"id":2,"order_id":"6eaa804c-5d1d-4b2f-ac92-021783a10d87","supplier_id":3016,"item_id":47,"status":"shipped","qty":600,"net_price":1310,"issued_at":1753243341153,"completed_at":1753243341153,"spec":null,"created_at":1753243341153,"updated_at":1753271606100},"source":{"version":"1.9.8.Final","connector":"sqlserver","name":"sqlserver_transaction_log_source","ts_ms":1755584849460,"snapshot":"last","db":"inventory","sequence":null,"schema":"INV","table":"orders","change_lsn":null,"commit_lsn":"0000002e:00002a40:0003","event_serial_no":null},"op":"r","ts_ms":1755584849448,"transaction":null}

# 自定义 SqlserverDeserializationSchema
{"op":"READ","dbName":"inventory","after":{"completed_at":"2025-07-23 04:02:21","updated_at":"2025-07-23 11:53:26","item_id":47,"qty":600,"created_at":"2025-07-23 04:02:21","net_price":1310,"id":2,"order_id":"6eaa804c-5d1d-4b2f-ac92-021783a10d87","supplier_id":3016,"issued_at":"2025-07-23 04:02:21","status":"shipped"},"schemaName":"INV","tableName":"orders"}
{"op":"CREATE","dbName":"inventory","after":{"completed_at":"2025-07-23 04:02:21","updated_at":"2025-07-23 11:53:26","item_id":47,"qty":600,"created_at":"2025-07-23 04:02:21","net_price":1310,"id":5,"order_id":"111","supplier_id":3016,"issued_at":"2025-07-23 04:02:21","status":"shipped"},"schemaName":"INV","tableName":"orders"}
{"op":"UPDATE","dbName":"inventory","after":{"completed_at":"2025-07-23 04:02:21","updated_at":"2025-07-23 11:53:26","item_id":47,"qty":999,"created_at":"2025-07-23 04:02:21","net_price":1310,"id":5,"order_id":"111","supplier_id":3016,"issued_at":"2025-07-23 04:02:21","status":"shipped"},"schemaName":"INV","tableName":"orders"}
{"op":"DELETE","before":{"completed_at":"2025-07-23 04:02:21","updated_at":"2025-07-23 11:53:26","item_id":47,"qty":999,"created_at":"2025-07-23 04:02:21","net_price":1310,"id":5,"order_id":"111","supplier_id":3016,"issued_at":"2025-07-23 04:02:21","status":"shipped"},"dbName":"inventory","schemaName":"INV","tableName":"orders"}

```
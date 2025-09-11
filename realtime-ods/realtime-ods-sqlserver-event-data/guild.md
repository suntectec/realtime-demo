```shell
scp target/realtime-ods-sqlserver-event-data-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/usrlib
```

```
$FLINK_HOME/bin/flink run -d \
-c com.sands.realtime.ods.sqlserver.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods-sqlserver-event-data-1.0-SNAPSHOT.jar
```

### Resume from a retained checkpoint

```
$FLINK_HOME/bin/flink run \
-s s3://lakehouse/flink-checkpoints/OdsBaseAPP/0caca4a1d6e5bdbff69bbfcf1b67a6e5/chk-3 \
-c com.sands.realtime.ods.sqlserver.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods-sqlserver-event-data-1.0-SNAPSHOT.jar
```

### Stopping a Job with Savepoint

```
$FLINK_HOME/bin/flink stop \
--savepointPath s3://lakehouse/flink-savepoints/ \
3e5991e7b712fd468fdbcfa6fadb2dfa
```

### Resuming from Savepoints

```
$FLINK_HOME/bin/flink run \
--detached \
--fromSavepoint s3://lakehouse/flink-savepoints/savepoint-3e5991-71be682ff4a0 \
--class com.sands.realtime.ods.sqlserver.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods-sqlserver-event-data-1.0-SNAPSHOT.jar
```
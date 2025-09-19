```
$FLINK_HOME/bin/flink run -d \
-c com.sands.realtime.common.utils.FlinkSQLExecutor \
$FLINK_HOME/lib/common/realtime-common-1.0-SNAPSHOT.jar \
--sql $FLINK_HOME/scripts/sqlserver2paimon.s3.sql
```
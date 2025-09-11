```shell
scp target/realtime-common-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/lib/common
```

```
$FLINK_HOME/bin/stop-cluster.sh && $FLINK_HOME/bin/start-cluster.sh
```
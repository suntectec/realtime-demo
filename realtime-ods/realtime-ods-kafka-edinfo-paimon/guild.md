```shell
scp target/realtime-ods-kafka-edinfo-paimon-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/usrlib
```

```
$FLINK_HOME/bin/flink run -d \
-c com.sands.realtime.ods.app.OdsSinkToPaimonAPP \
$FLINK_HOME/usrlib/realtime-ods-kafka-edinfo-paimon-1.0-SNAPSHOT.jar
```

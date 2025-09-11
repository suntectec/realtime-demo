```shell
scp target/realtime-ods-1.0-SNAPSHOT.jar Data.Eng@192.168.138.15:/opt/poc-allin1/native/flink/flink-1.20.1/usrlib
```

```
$FLINK_HOME/bin/flink run \
-c com.sands.realtime.ods.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods-1.0-SNAPSHOT.jar
```

### Resume from a retained checkpoint

```
$FLINK_HOME/bin/flink run \
-s s3://lakehouse/flink-checkpoints/OdsBaseAPP/95a87f0b6533d254551caa9bad5965ae/chk-1 \
-c com.sands.realtime.ods.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods-1.0-SNAPSHOT.jar
```
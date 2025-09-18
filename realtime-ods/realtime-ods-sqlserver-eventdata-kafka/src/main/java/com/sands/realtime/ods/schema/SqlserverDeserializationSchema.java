package com.sands.realtime.ods.schema;

import com.alibaba.fastjson.JSON;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.jose4j.json.internal.json_simple.JSONObject;

import java.util.HashMap;
import java.util.Map;

/**
 * 自定义反序列化格式，将数据按照标准统一数据输出
 *
 * @author Jagger
 * @since 2025/8/22 15:32
 */
public class SqlserverDeserializationSchema implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = -1L;

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        Map<String, Object> resultMap = new HashMap<>();
        /* String topic = sourceRecord.topic();
        String[] split = topic.split("[.]");
        String database = split[1];
        String table = split[2];
        resultMap.put("db", database);
        resultMap.put("tableName", table); */
        // 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        // 获取数据本身
        Struct struct = (Struct) sourceRecord.value();
        Struct after = struct.getStruct("after");
        Struct before = struct.getStruct("before");
        String op = operation.name();
        resultMap.put("op", op);
        // 获取 db、table、ts_ms
        Struct source = struct.getStruct("source");
        String tableCatalog = source.getString("db");
        String tableSchema = source.getString("schema");
        String tableName = source.getString("table");
        Long sourceTsMs = source.getInt64("ts_ms"); // payload.source.ts_ms: 源系统创建事件的时间戳。source_database_update_time 源数据库更新时间戳
        Long tsMs = struct.getInt64("ts_ms"); // payload.ts_ms: 连接器处理事件时的时间戳。Debezium 连接器处理的JVM系统时间戳
        resultMap.put("table_catalog", tableCatalog);
        resultMap.put("table_schema", tableSchema);
        resultMap.put("table_name", tableName);
        resultMap.put("source_time", sourceTsMs);
        resultMap.put("ingestion_time", tsMs);

        // 新增,更新或者初始化
        if (op.equals(Envelope.Operation.CREATE.name()) || op.equals(Envelope.Operation.READ.name()) || op.equals(Envelope.Operation.UPDATE.name())) {
            JSONObject afterJson = new JSONObject();
            if (after != null) {
                Schema schema = after.schema();
                for (Field field : schema.fields()) {
                    afterJson.put(field.name(), after.get(field.name()));
                }
                resultMap.put("after", afterJson);
            }
        }

        // 删除
        if (op.equals(Envelope.Operation.DELETE.name())) {
            JSONObject beforeJson = new JSONObject();
            if (before != null) {
                Schema schema = before.schema();
                for (Field field : schema.fields()) {
                    beforeJson.put(field.name(), before.get(field.name()));
                }
                resultMap.put("before", beforeJson);
            }
        }

        collector.collect(JSON.toJSONString(resultMap));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}

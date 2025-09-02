package com.sands.realtime.ods.source;

import com.sands.realtime.ods.schema.SqlserverDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;

import java.util.Properties;

/**
 * 一个 sqlserver 源，使用了自定义的 debezium deserialization schema 和 datetime converter
 *
 * @author Jagger
 * @since 2025/8/19 11:17
 */
public class SqlserverOdsSource {
    public static SqlServerIncrementalSource<String> getSqlServerOdsSource(ParameterTool parameter, String  databaseList, String tableList) {
        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(parameter.get("sqlserver.host"))
                        .port(parameter.getInt("sqlserver.port"))
                        .username(parameter.get("sqlserver.username"))
                        .password(parameter.get("sqlserver.password"))
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .deserializer(new SqlserverDeserializationSchema())
                        .debeziumProperties(getDebeziumProperties())
                        .startupOptions(StartupOptions.initial())
                        .build();
        return sqlServerSource;
    }

    public static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.put("converters", "sqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.type", "com.sands.realtime.ods.converter.DateTimeDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.database.type", "sqlserver");
        // 自定义格式，可选
        properties.put("sqlserverDebeziumConverter.format.date", "yyyy-MM-dd");
        properties.put("sqlserverDebeziumConverter.format.time", "HH:mm:ss");
        properties.put("sqlserverDebeziumConverter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        return properties;
    }
}

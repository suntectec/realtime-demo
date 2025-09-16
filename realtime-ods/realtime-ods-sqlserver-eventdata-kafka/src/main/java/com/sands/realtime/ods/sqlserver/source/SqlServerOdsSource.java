package com.sands.realtime.ods.sqlserver.source;

import com.sands.realtime.ods.sqlserver.schema.SqlserverDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;

import java.util.Properties;

/**
 * @author Jagger
 * @since 2025/8/19 11:17
 */
public class SqlServerOdsSource {

    /**
     * 一个 sqlserver 源，使用了自定义的 debezium deserialization schema 和 datetime converter
     *
     * @param parameters
     * @param databaseList
     * @param tableList
     * @return
     */
    public static SqlServerIncrementalSource<String> getSqlServerOdsSource(
            ParameterTool parameters, String  databaseList, String tableList, StartupOptions startupOptions) {
        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(parameters.get("sqlserver.host"))
                        .port(parameters.getInt("sqlserver.port"))
                        .databaseList(databaseList)
                        .tableList(tableList)
                        .username(parameters.get("sqlserver.username"))
                        .password(parameters.get("sqlserver.password"))
                        .deserializer(new SqlserverDeserializationSchema())
                        .debeziumProperties(getDebeziumProperties())
                        .startupOptions(startupOptions)
                        .build();
        return sqlServerSource;
    }

    public static Properties getDebeziumProperties() {
        Properties properties = new Properties();
        properties.put("converters", "sqlserverDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.type", "com.sands.realtime.ods.sqlserver.converter.DataDateTimeDebeziumConverter");
        properties.put("sqlserverDebeziumConverter.database.type", "sqlserver");
        // 自定义格式，可选
        properties.put("sqlserverDebeziumConverter.format.date", "yyyy-MM-dd");
        properties.put("sqlserverDebeziumConverter.format.time", "HH:mm:ss");
        properties.put("sqlserverDebeziumConverter.format.datetime", "yyyy-MM-dd HH:mm:ss");
        return properties;
    }
}

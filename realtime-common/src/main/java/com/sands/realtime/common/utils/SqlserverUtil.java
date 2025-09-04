package com.sands.realtime.common.utils;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jagger
 * @since 2025/8/19 9:53
 */
public class SqlserverUtil {

    // SqlServer CDC Source
    public static SqlServerIncrementalSource<String> getSqlServerCdcSource(ParameterTool parameters,
            String database, String table, StartupOptions startupOptions) {
        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(parameters.get("sqlserver.host"))
                        .port(parameters.getInt("sqlserver.port"))
                        .databaseList(database)
                        .tableList(table)
                        .username(parameters.get("sqlserver.username"))
                        .password(parameters.get("sqlserver.password"))
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(startupOptions)
                        .build();

        return sqlServerSource;
    }

    public static DataStreamSource<String> createSqlServerCdcDataStream(StreamExecutionEnvironment env,
                                                                        ParameterTool parameters, String database, String table, StartupOptions startupOptions) throws Exception {
        SqlServerIncrementalSource<String> sqlServerSource = getSqlServerCdcSource(parameters, database, table, startupOptions);
        return env.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "Sqlserver Source");
    }

}

package com.sands.realtime.common.utils;

import com.sands.realtime.common.constant.SqlServerConstant;
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
public class SqlServerUtil {

    public static SqlServerIncrementalSource<String> getSqlServerSource(ParameterTool parameters,
                                                                        String databaseList, String tableList, StartupOptions startupOptions) {
        return new SqlServerSourceBuilder<String>()
                .hostname(parameters.get("sqlserver.host"))
                .port(parameters.getInt("sqlserver.port"))
                .databaseList(SqlServerConstant.SQLSERVER_SOURCE_DB)
                .tableList(SqlServerConstant.SQLSERVER_SOURCE_TB)
                .username(parameters.get("sqlserver.username"))
                .password(parameters.get("sqlserver.password"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(startupOptions)
                .build();
    }

    public static DataStreamSource<String> createSqlServerCdcDataStream(StreamExecutionEnvironment streamEnv,
                                                                        ParameterTool parameters, String database, String table, StartupOptions startupOptions) throws Exception {
        SqlServerIncrementalSource<String> sqlServerSource = getSqlServerSource(parameters, database, table, startupOptions);
        return streamEnv.fromSource(sqlServerSource, WatermarkStrategy.noWatermarks(), "Sqlserver Source");
    }

}

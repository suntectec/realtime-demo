package com.sands.realtime.example.datastream;

import com.sands.realtime.common.utils.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/15 16:13
 */
public class CdcSqlserver2ConsoleJob {
    private final static Logger logger = LoggerFactory.getLogger(CdcSqlserver2ConsoleJob.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // CDC SqlServer Source
        SqlServerIncrementalSource<String> sqlServerIncrementalSource = new SqlServerSourceBuilder<String>()
                .hostname(sqlserver_host)
                .port(Integer.parseInt(sqlserver_port))
                .databaseList("inventory")
                .tableList("INV.orders")
                .username(sqlserver_username)
                .password(sqlserver_password)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        DataStreamSource<String> source = env
                .fromSource(sqlServerIncrementalSource, WatermarkStrategy.noWatermarks(), "SqlServer Source")
                .setParallelism(1);

        source.print(">source>").setParallelism(1);

        env.execute();
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = PropertiesUtil.getProperty("sqlserver.host");
        String sqlserver_port = PropertiesUtil.getProperty("sqlserver.port");
        String sqlserver_username = PropertiesUtil.getProperty("sqlserver.username");
        String sqlserver_password = PropertiesUtil.getProperty("sqlserver.password");

        CdcSqlserver2ConsoleJob.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password);
    }
}

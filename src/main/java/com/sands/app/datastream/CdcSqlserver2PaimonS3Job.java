package com.sands.app.datastream;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.sands.utils.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder;
import org.apache.flink.cdc.connectors.sqlserver.source.SqlServerSourceBuilder.SqlServerIncrementalSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.flink.sink.cdc.RichCdcRecord;
import org.apache.paimon.flink.sink.cdc.RichCdcSinkBuilder;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exception in thread "main" java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(java.lang.String, int)'
 * - issue temporarily resolved by add hadoop-3.4.0-win10-x64/bin/hadoop.dll to Windows/System32
 *
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class CdcSqlserver2PaimonS3Job {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(CdcSqlserver2PaimonS3Job.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {

        SqlServerIncrementalSource<String> sqlServerSource =
                new SqlServerSourceBuilder<String>()
                        .hostname(sqlserver_host)
                        .port(Integer.parseInt(sqlserver_port))
                        .databaseList("inventory")
                        .tableList("INV.orders")
                        .username(sqlserver_username)
                        .password(sqlserver_password)
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint for CONTINUOUS_UNBOUNDED source, set checkpoint interval
        env.enableCheckpointing(3000);

        // set the source parallelism to 2
        DataStreamSource<String> sourceDS = env.fromSource(
                sqlServerSource,
                WatermarkStrategy.noWatermarks(),
                "SqlServerIncrementalSource");
        sourceDS
                .setParallelism(2)
                .print()
                .setParallelism(1);

        // String convert to RichCdcRecord
        SingleOutputStreamOperator<RichCdcRecord> transformDS = sourceDS
                .map(JSON::parseObject)
                .map(jsonObj -> {
                    var op = jsonObj.getString("op");
                    var before = jsonObj.getString("before");
                    var after = jsonObj.getString("after");
                    var ts_ms = jsonObj.getString("ts_ms");

                    JSONObject afterObj = JSON.parseObject(after);

                    return RichCdcRecord.builder(op.equals("r") || op.equals("c") ? RowKind.INSERT : RowKind.UPDATE_AFTER)
                            .field("id", DataTypes.BIGINT(), afterObj.getString("id"))
                            .field("qty", DataTypes.INT(), afterObj.getString("qty"))
                            .field("status", DataTypes.STRING(), afterObj.getString("status"))
                            .field("__op_ts", DataTypes.TIMESTAMP(), ts_ms)
                            .build();
                });

        // Paimon S3 Sink
        Identifier identifier = Identifier.create("my_db", "T");
        Options options = new Options();
        options.set("warehouse", "s3://lakehouse/paimon/");
        options.set("s3.endpoint", s3_endpoint);
        options.set("s3.access-key", s3_access_key);
        options.set("s3.secret-key", s3_secret_key);
        options.set("s3.path.style.access", "true");
        CatalogLoader catalogLoader =
                () -> FlinkCatalogFactory.createPaimonCatalog(options);
        catalogLoader.load().createDatabase("my_db", true);
        catalogLoader.load().createTable(identifier,
                org.apache.paimon.schema.Schema.newBuilder()
                        .column("id", DataTypes.BIGINT())
                        .column("qty", DataTypes.INT())
                        .column("status", DataTypes.STRING())
                        .column("__op_ts", DataTypes.TIMESTAMP())
                        .primaryKey("id")
                        .build(), true);

        Table table = catalogLoader.load().getTable(identifier);

        new RichCdcSinkBuilder(table)
                .forRichCdcRecord(transformDS)
                .identifier(identifier)
                .catalogLoader(catalogLoader)
                .build();

        env.execute("Print SqlServer Snapshot + Change Stream");
    }

    public static void main(String[] args) throws Exception {
        String sqlserver_host = PropertiesUtil.getProperty("sqlserver.host");
        String sqlserver_port = PropertiesUtil.getProperty("sqlserver.port");
        String sqlserver_username = PropertiesUtil.getProperty("sqlserver.username");
        String sqlserver_password = PropertiesUtil.getProperty("sqlserver.password");

        String s3_endpoint = PropertiesUtil.getProperty("s3.endpoint");
        String s3_access_key = PropertiesUtil.getProperty("s3.access-key");
        String s3_secret_key = PropertiesUtil.getProperty("s3.secret-key");

        CdcSqlserver2PaimonS3Job.run(sqlserver_host, sqlserver_port, sqlserver_username, sqlserver_password,
                s3_endpoint, s3_access_key, s3_secret_key);
    }
}

package com.sands.realtime.ods.app;

import com.sands.realtime.common.base.BaseAPP;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/9/12 15:28
 */
public class OdsSinkToSqlServerAPP {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        // 写出到 sqlserver
        tEnv.executeSql(
                "CREATE TABLE SourceOrders (\n" +
                        "  id BIGINT,\n" +
                        "  order_id STRING,\n" +
                        "  supplier_id INT,\n" +
                        "  item_id INT,\n" +
                        "  status STRING,\n" +
                        "  qty INT,\n" +
                        "  net_price INT,\n" +
                        "  issued_at TIMESTAMP,\n" +
                        "  completed_at TIMESTAMP,\n" +
                        "  spec STRING,\n" +
                        "  created_at TIMESTAMP,\n" +
                        "  updated_at TIMESTAMP,\n" +
                        "  _row_kind STRING,\n" +
                        "  _ingestion_time TIMESTAMP,\n" +
                        "  _process_time TIMESTAMP,\n" +
                        "  _source_time TIMESTAMP\n" +
                        ") WITH (\n" +
                        "  'connector' = 'kafka',\n" +
                        "  'topic' = 'ods_orders_topic',\n" +
                        "  'properties.bootstrap.servers' = '192.168.138.15:9092',\n" +
                        "  'properties.group.id' = 'data_group',\n" +
                        "  'scan.startup.mode' = 'earliest-offset',\n" +
                        "  'format' = 'json'\n" +
                        ")");

        // Table sourceTable = tEnv.sqlQuery("SELECT * FROM SourceOrders");
        // sourceTable.execute().print();

        tEnv.executeSql(
                "CREATE TABLE SinkOrders (\n" +
                        "  id BIGINT,\n" +
                        "  order_id STRING,\n" +
                        "  supplier_id INT,\n" +
                        "  item_id INT,\n" +
                        "  status STRING,\n" +
                        "  qty INT,\n" +
                        "  net_price INT,\n" +
                        "  issued_at TIMESTAMP,\n" +
                        "  completed_at TIMESTAMP,\n" +
                        "  spec STRING,\n" +
                        "  created_at TIMESTAMP,\n" +
                        "  updated_at TIMESTAMP,\n" +
                        "  _row_kind STRING,\n" +
                        "  _ingestion_time TIMESTAMP,\n" +
                        "  _process_time TIMESTAMP,\n" +
                        "  _source_time TIMESTAMP\n" +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:sqlserver://192.168.138.15:14330;database=TestDB',\n" +
                        "  'driver' = 'com.microsoft.sqlserver.jdbc.SQLServerDriver',\n" +
                        "  'username' = 'SA',\n" +
                        "  'password' = 'YourStrong!Passw0rd',\n" +
                        "  'table-name' = 'TestDB.dbo.sink_order1'\n" +
                        "  );"
        );

        TableResult tableResult = tEnv.executeSql(
                "INSERT INTO SinkOrders SELECT * FROM SourceOrders");

        if (tableResult.getJobClient().isPresent()) {
            System.out.println("Job ID: " + tableResult.getJobClient().get().getJobID());
            System.out.println("Job Status: " + tableResult.getJobClient().get().getJobStatus());
        }

    }

}

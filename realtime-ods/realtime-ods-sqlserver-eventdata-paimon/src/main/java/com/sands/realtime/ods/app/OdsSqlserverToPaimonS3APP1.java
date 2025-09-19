package com.sands.realtime.ods.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * todo 打包运行存在 issue - ClassNotFoundException: org.apache.hadoop.conf.Configuration
 *
 * @author Jagger
 * @since 2025/8/13 10:10
 */
@Slf4j
public class OdsSqlserverToPaimonS3APP1 {

    public static void main(String[] args) {

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(10000);
        streamEnv.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);

        tableEnv.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "    'type'='paimon',\n" +
                "    'warehouse'='s3://lakehouse/paimon/',\n" +
                "    's3.endpoint'='http://192.168.138.15:9000',\n" +
                "    's3.access-key'='minioadmin',\n" +
                "    's3.secret-key'='minioadmin',\n" +
                "    's3.path.style.access'='true'\n" +
                ");");

        tableEnv.executeSql("USE CATALOG paimon_catalog;");

        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS inventory;");

        tableEnv.executeSql("USE inventory;");

        tableEnv.executeSql("CREATE TEMPORARY TABLE InventoryINVOrders (\n" +
                "    id BIGINT,\n" +
                "    order_id VARCHAR(36),\n" +
                "    supplier_id INT,\n" +
                "    item_id INT,\n" +
                "    status VARCHAR(20),\n" +
                "    qty INT,\n" +
                "    net_price INT,\n" +
                "    issued_at TIMESTAMP,\n" +
                "    completed_at TIMESTAMP,\n" +
                "    spec VARCHAR(1024),\n" +
                "    created_at TIMESTAMP,\n" +
                "    updated_at TIMESTAMP,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = '192.168.138.15',\n" +
                "    'port' = '1433',\n" +
                "    'username' = 'sa',\n" +
                "    'password' = 'Abcd1234',\n" +
                "    'database-name' = 'inventory',\n" +
                "    'table-name' = 'INV.orders'\n" +
                ");");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS Orders (\n" +
                "    id BIGINT,\n" +
                "    order_id VARCHAR(36),\n" +
                "    supplier_id INT,\n" +
                "    item_id INT,\n" +
                "    status VARCHAR(20),\n" +
                "    qty INT,\n" +
                "    net_price INT,\n" +
                "    issued_at TIMESTAMP,\n" +
                "    completed_at TIMESTAMP,\n" +
                "    spec VARCHAR(1024),\n" +
                "    created_at TIMESTAMP,\n" +
                "    updated_at TIMESTAMP,\n" +
                "    PRIMARY KEY (id) NOT ENFORCED\n" +
                ");");

        TableResult tableResult = tableEnv.executeSql("INSERT INTO Orders SELECT * FROM InventoryINVOrders;");
        if (tableResult.getJobClient().isPresent()) log.info("----------"+tableResult.getJobClient().get().getJobStatus());

        // tableEnv.sqlQuery("SELECT * FROM Orders;").execute().print();

    }

}

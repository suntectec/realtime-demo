package com.sands.realtime.ods.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 *
 *
 * @author Jagger
 * @since 2025/8/13 10:10
 */
@Slf4j
public class OdsSqlserverToPaimonS3APP1 {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10000);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql("CREATE CATALOG paimon_catalog WITH (\n" +
                "    'type'='paimon',\n" +
                "    'warehouse'='s3://lakehouse/paimon/',\n" +
                "    's3.endpoint'='http://192.168.138.15:9000',\n" +
                "    's3.access-key'='minioadmin',\n" +
                "    's3.secret-key'='minioadmin',\n" +
                "    's3.path.style.access'='true'\n" +
                ");");

        tEnv.executeSql("USE CATALOG paimon_catalog;");

        tEnv.executeSql("CREATE DATABASE IF NOT EXISTS inventory;");

        tEnv.executeSql("USE inventory;");

        tEnv.executeSql("CREATE TEMPORARY TABLE InventoryINVOrders (\n" +
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

        tEnv.executeSql("CREATE TABLE IF NOT EXISTS Orders (\n" +
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

        TableResult tableResult = tEnv.executeSql("INSERT INTO Orders SELECT * FROM InventoryINVOrders;");
        if (tableResult.getJobClient().isPresent()) log.info("----------"+tableResult.getJobClient().get().getJobStatus());

        // tEnv.sqlQuery("SELECT * FROM Orders;").execute().print();

    }

}

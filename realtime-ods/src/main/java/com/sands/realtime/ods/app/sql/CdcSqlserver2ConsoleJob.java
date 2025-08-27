package com.sands.realtime.ods.app.sql;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/1 11:30
 */
public class CdcSqlserver2ConsoleJob {

    // Define Logger at the class level
    private static final Logger logger = LoggerFactory.getLogger(CdcSqlserver2ConsoleJob.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE Orders (\n" +
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
                        ");"
        );

        tEnv.sqlQuery("SELECT * FROM Orders")
                .execute()
                .print();
    }
}

package com.sands.realtime.example.sql;

import com.sands.realtime.common.utils.PropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jagger
 * @since 2025/8/13 10:10
 */
public class CdcSqlserver2PaimonS3Job {
    private static final Logger logger= LoggerFactory.getLogger(CdcSqlserver2PaimonS3Job.class);

    public static void run(String sqlserver_host, String sqlserver_port, String sqlserver_username, String sqlserver_password,
                           String s3_endpoint, String s3_access_key, String s3_secret_key) throws Exception {
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

        tEnv.executeSql("CREATE TEMPORARY TABLE SourceTable (\n" +
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

        tEnv.executeSql("CREATE TABLE IF NOT EXISTS SinkTable (\n" +
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

        // TableResult tableResult = tEnv.executeSql("INSERT INTO SinkTable SELECT * FROM SourceTable;");
        // if (tableResult.getJobClient().isPresent()) System.out.println(tableResult.getJobClient().get().getJobStatus());

        tEnv.executeSql("INSERT INTO SinkTable SELECT * FROM SourceTable;");

        tEnv.executeSql("SELECT * FROM SinkTable;").print();
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
package com.sands.realtime.ods.app;

import com.sands.realtime.ods.base.BaseTableAPP;
import lombok.extern.slf4j.Slf4j;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jagger
 * @since 2025/9/12 15:28
 */
@Slf4j
public class OdsKafkaToSqlServerAPP extends BaseTableAPP {

    public static void main(String[] args) {

        String sourceTable =
                "CREATE TABLE InventoryINVOrders (\n" +
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
                        ")";

        String sinkTable =
                "CREATE TABLE TestDBOOrders (\n" +
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
                        "  'table-name' = 'test.dbo.orders'\n" +
                        "  );";

        // 使用正则表达式匹配表名
        Pattern pattern = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)\\s*\\(");
        Matcher sourceTableNameMatcher = pattern.matcher(sourceTable);
        Matcher sinkTableNameMatcher = pattern.matcher(sinkTable);

        if (sourceTableNameMatcher.find() && sinkTableNameMatcher.find()) {
            String sourceTableName = sourceTableNameMatcher.group(1).trim();
            String sinkTableName = sinkTableNameMatcher.group(1).trim();
            log.info("提取的源表名: '" + sourceTableName + "'");
            log.info("提取的目标表名: '" + sinkTableName + "'");

            new OdsKafkaToSqlServerAPP().start(sourceTableName, sourceTable, sinkTableName, sinkTable);

        } else {
            log.error("未找到表名");
        }

    }

}

package com.sands.realtime.ods.app;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Locale;

/**
 * 优化后读取 sql-scripts 文件
 *
 * @author Jagger
 * @since 2025/8/13 10:10
 */
@Slf4j
public class OdsSqlserverToPaimonS3APP2 {

    public static void main(String[] args) {
        System.out.println("--------------------  begin init ----------------------");
        final String sqlPath = ParameterTool.fromArgs(args).get("sql", "scripts/sqlserver2paimon.s3.sql");
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.enableCheckpointing(10000);
        streamEnv.setParallelism(1);
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);
        StatementSet statementSet = tableEnv.createStatementSet();
        String sqlStr;
        try {
            sqlStr = FileUtils.readFileToString(FileUtils.getFile(sqlPath), "utf-8");
        } catch (IOException e) {
            System.err.println("Read File - IOException");
            throw new RuntimeException(e);
        }
        String[] sqlArr = sqlStr.split(";");
        for (String sql : sqlArr) {
            sql = sql.trim();
            if (sql.toLowerCase(Locale.ROOT).startsWith("create")) {
                System.out.println("----------------------------------------------\nexecuteSql=\n" + sql);
                tableEnv.executeSql(sql);
            } else if (sql.toLowerCase(Locale.ROOT).startsWith("use")) {
                System.out.println("----------------------------------------------\nuse=\n" + sql);
                tableEnv.executeSql(sql);
            } else if (sql.toLowerCase(Locale.ROOT).startsWith("insert")) {
                System.out.println("----------------------------------------------\ninsert=\n" + sql);
                statementSet.addInsertSql(sql);
            }
        }
        System.out.println("---------------------- begin exec sql --------------------------");
        statementSet.execute();
    }

}

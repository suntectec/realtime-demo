package com.sands.realtime.common.utils;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Locale;

/**
 * flink run 形式指定 sql script:
 * 当作业的SQL语句修改频繁时，可使用Flink Jar的方式提交Flink SQL语句，以减少用户工作量。
 *  1、使用 ; 分号作为 sql script 的 sql 分隔符，不处理 -- 注释，读取不带注释的 .sql 文件更清晰方便
 *  2、set 执行 ckpt interval 语句 Flink 不支持以流表环境执行，程序不做执行
 * todo 打包运行存在 issue - ClassNotFoundException: org.apache.hadoop.conf.Configuration
 *
 * @author Jagger
 * @since 2025/9/19 16:45
 */
public class FlinkSQLExecutor {

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
        String[] sqlArr = sqlStr.split(";"); // 注意按 ; 分割 sql 执行，最好不要出现 -- 注释行
        for (String sql : sqlArr) {
            sql = sql.trim();
            // 跳过空行和注释行
            if (sql.isEmpty() || sql.startsWith("--")) {
                continue;
            }
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

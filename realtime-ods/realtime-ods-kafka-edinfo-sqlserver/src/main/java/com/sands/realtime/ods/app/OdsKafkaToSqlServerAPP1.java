package com.sands.realtime.ods.app;

import com.amazonaws.services.dynamodbv2.xspec.L;
import com.sands.realtime.common.base.TableBaseAPP;
import com.sands.realtime.common.utils.ResourcesFileReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jagger
 * @since 2025/9/12 15:28
 */
@Slf4j
public class OdsKafkaToSqlServerAPP1 extends TableBaseAPP {

    public static void main(String[] args) throws Exception {
        new OdsKafkaToSqlServerAPP1().start(8081, args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv, ParameterTool parameters) throws IOException {

        String sourceTable = ResourcesFileReader.readResourcesFile("sql-scripts/source-table.sql");

        String sinkTable = ResourcesFileReader.readResourcesFile("sql-scripts/sink-table.sql");

        tEnv.executeSql(sourceTable);
        tEnv.executeSql(sinkTable);

        // 使用正则表达式匹配表名
        Pattern pattern = Pattern.compile("CREATE\\s+TABLE\\s+(\\w+)\\s*\\(");
        Matcher sourceTableNameMatcher = pattern.matcher(sourceTable);
        Matcher sinkTableNameMatcher = pattern.matcher(sinkTable);

        if (sourceTableNameMatcher.find() && sinkTableNameMatcher.find()) {
            String sourceTableName = sourceTableNameMatcher.group(1).trim();
            String sinkTableName = sinkTableNameMatcher.group(1).trim();
            log.info("提取的源表名: '" + sourceTableName + "'");
            log.info("提取的目标表名: '" + sinkTableName + "'");

            TableResult tableResult = null;

            if (env instanceof LocalStreamEnvironment) { // 本地只做源表查询
                log.info("==================== 本地运行环境 ====================");
                log.info("源表名: '" + sourceTableName + "'");
                log.info("目标表名: '" + sinkTableName + "'");

                tableResult = tEnv.sqlQuery(String.format("SELECT * FROM %s", sourceTableName)).execute();
            } else if (env instanceof StreamExecutionEnvironment) {
                log.info("==================== 集群运行环境 ====================");
                log.info("源表名: '" + sourceTableName + "'");
                log.info("目标表名: '" + sinkTableName + "'");

                tableResult = tEnv.executeSql(
                        String.format("INSERT INTO %s SELECT * FROM %s", sinkTableName, sourceTableName));
            }

            if (tableResult != null && tableResult.getJobClient().isPresent()) {
                log.info("==================== Job Started Successfully ====================");
                log.info("Job Name: " + this.getClass().getSimpleName());
                log.info("Job ID: " + tableResult.getJobClient().get().getJobID());
                log.info("Job Status: " + tableResult.getJobClient().get().getJobStatus());
            }

        } else {
            log.error("未找到表名");
        }

    }

}

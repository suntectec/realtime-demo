package com.sands.realtime.common.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Jagger
 * @since 2025/9/16 16:03
 */
@Slf4j
public abstract class BaseTableAPP {

    private String sourceTable;

    private String sinkTable;

    public void setTable(String sourceTable, String sinkTable) {
        this.sourceTable = sourceTable;
        this.sinkTable = sinkTable;
    }

    public abstract void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv);

    /**
     * @author Jagger
     * @since 2025/9/16 14:55
     */
    public void start() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        // set the parallelism to 8
        tEnv.getConfig().set("parallelism.default", "1");
        // set the job name
        tEnv.getConfig().set("pipeline.name", this.getClass().getSimpleName());

        // 环境配置
        handle(env, tEnv);

        // 写出到 sqlserver
        tEnv.executeSql(sourceTable);

        // Table sourceTable = tEnv.sqlQuery("SELECT * FROM SourceOrders");
        // TableResult tableResult = sourceTable.execute();
        // tableResult.print();

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

            TableResult tableResult = tEnv.executeSql(
                    String.format("INSERT INTO %s SELECT * FROM %s", sinkTableName, sourceTableName));

            if (tableResult.getJobClient().isPresent()) {
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

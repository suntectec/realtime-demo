package com.sands.realtime.ods.base;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Jagger
 * @since 2025/9/16 16:03
 */
@Slf4j
public abstract class BaseTableAPP {

    /**
     * @author Jagger
     * @since 2025/9/16 14:55
     */
    public void start(String sourceTableName, String sourceTable, String sinkTableName, String sinkTable) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        // set the parallelism to 8
        tEnv.getConfig().set("parallelism.default", "1");
        // set the job name
        tEnv.getConfig().set("pipeline.name", this.getClass().getSimpleName());

        env.disableOperatorChaining();


        // 写出到 sqlserver
        tEnv.executeSql(sourceTable);

        // Table sourceTable = tEnv.sqlQuery("SELECT * FROM SourceOrders");
        // TableResult tableResult = sourceTable.execute();
        // tableResult.print();

        tEnv.executeSql(sinkTable);

        TableResult tableResult = tEnv.executeSql(
                String.format("INSERT INTO %s SELECT * FROM %s", sinkTableName, sourceTableName));

        if (tableResult.getJobClient().isPresent()) {
            log.info("==================== Job Started Successfully ====================");
            log.info("Job Name: " + this.getClass().getSimpleName());
            log.info("Job ID: " + tableResult.getJobClient().get().getJobID());
            log.info("Job Status: " + tableResult.getJobClient().get().getJobStatus());
        }

    }

}

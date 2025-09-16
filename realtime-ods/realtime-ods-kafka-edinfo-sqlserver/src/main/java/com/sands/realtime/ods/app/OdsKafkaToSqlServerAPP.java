package com.sands.realtime.ods.app;

import com.sands.realtime.common.utils.ResourcesFileReader;
import com.sands.realtime.common.base.BaseTableAPP;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;

/**
 * @author Jagger
 * @since 2025/9/12 15:28
 */
@Slf4j
public class OdsKafkaToSqlServerAPP extends BaseTableAPP {

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        env.disableOperatorChaining();
    }

    public static void main(String[] args) throws IOException {

        String sourceTable = ResourcesFileReader.readResourcesFile("sql-scripts/source-table.sql");

        String sinkTable = ResourcesFileReader.readResourcesFile("sql-scripts/sink-table.sql");

        OdsKafkaToSqlServerAPP odsKafkaToSqlServerAPP = new OdsKafkaToSqlServerAPP();
        odsKafkaToSqlServerAPP.setTable(sourceTable, sinkTable);
        odsKafkaToSqlServerAPP.start();

    }

}

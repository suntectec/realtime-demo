package com.sands.realtime.ods.app;

import com.alibaba.fastjson.JSON;
import com.sands.realtime.common.base.BaseAPP;
import com.sands.realtime.common.base.StreamBaseAPP;
import com.sands.realtime.common.bean.ods.SqlServerOrdersAfterInfo;
import com.sands.realtime.common.bean.ods.SqlServerOrdersEventData;
import com.sands.realtime.common.constant.SqlServerConstant;
import com.sands.realtime.common.constant.TopicConstant;
import com.sands.realtime.common.utils.FlinkSinkUtil;
import com.sands.realtime.ods.sqlserver.function.OrdersProcessFunction;
import com.sands.realtime.ods.sqlserver.source.SqlServerOdsSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将 flink sqlserver cdc debezium 写入 kafka
 *
 * @author Jagger
 * @since 2025/9/2 9:22
 */
@Slf4j
public class OdsSqlServerToKafkaAPP1 extends StreamBaseAPP {

    public static void main(String[] args) throws Exception {
        new OdsSqlServerToKafkaAPP1().start(8081, args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool parameters) {

        // Environment
        // 设置全局并行度
        env.setParallelism(1);
        // 设置时间语义为ProcessingTime
        env.getConfig().setAutoWatermarkInterval(0);
        // 每隔60s启动一个检查点
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        // checkpoint最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // checkpoint超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // Flink处理程序被cancel后，会保留Checkpoint数据
        env.getCheckpointConfig().getExternalizedCheckpointRetention();

        // Source
        // 使用自定义 SqlServer Line Debezium Schema 和 时间日期 Converter
        DataStreamSource<String> source = env.fromSource(SqlServerOdsSource.getSqlServerOdsSource(parameters, SqlServerConstant.SQLSERVER_SOURCE_DB, SqlServerConstant.SQLSERVER_SOURCE_TB, StartupOptions.initial()),
                WatermarkStrategy.noWatermarks(), "SqlServer Source");
        source.print(">source>");

        log.info("==================== SqlServerCDCSourceStarted ====================");

        // Transformation
        SingleOutputStreamOperator<SqlServerOrdersAfterInfo> infoDS = source
                .map(line -> JSON.parseObject(line, SqlServerOrdersEventData.class))
                .returns(Types.POJO(SqlServerOrdersEventData.class))
                .process(new OrdersProcessFunction())
                .returns(Types.POJO(SqlServerOrdersAfterInfo.class));

        SingleOutputStreamOperator<String> sink = infoDS.map(JSON::toJSONString);

        // Sink
        if (env instanceof LocalStreamEnvironment) { // 在本地测试运行的逻辑
            sink.print(">sink>");
        } else { // 写入kafka
            sink.sinkTo(FlinkSinkUtil.getKafkaSink(parameters, TopicConstant.ODS_ORDERS_TOPIC)).name("sink_ods_orders_topic");
        }

        env.disableOperatorChaining();

    }
}

package com.sands.realtime.ods.app;

import com.alibaba.fastjson2.JSON;
import com.sands.realtime.common.base.BaseAPP;
import com.sands.realtime.common.bean.ods.SqlserverOrdersBean;
import com.sands.realtime.common.constant.SqlserverConstant;
import com.sands.realtime.common.constant.TopicConstant;
import com.sands.realtime.common.utils.FlinkSinkUtil;
import com.sands.realtime.ods.function.OrdersProcessFunction;
import com.sands.realtime.ods.source.SqlserverOdsSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将 flink sqlserver cdc debezium 数据转化成表数据写入 kafka
 *
 * @author Jagger
 * @since 2025/9/2 9:22
 * 程序启动：
$FLINK_HOME/bin/flink run \
-m localhost:8081 \
-c com.sands.realtime.ods.app.OdsBaseAPP \
$FLINK_HOME/usrlib/realtime-ods/target/realtime-ods-1.0-SNAPSHOT.jar
 */
@Slf4j
public class OdsBaseAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new OdsBaseAPP().start(8081, args);
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
        DataStreamSource<String> ds = env.fromSource(SqlserverOdsSource.getSqlServerOdsSource(parameters, SqlserverConstant.SQLSERVER_SOURCE_DB, SqlserverConstant.SQLSERVER_SOURCE_TB), WatermarkStrategy.noWatermarks(), "SqlServer Source");

        log.info("=========================== SqlServerCDCSourceStarted ==================================");
        // Transformation
        SingleOutputStreamOperator<String> result = ds
                .map(v -> JSON.parseObject(v, SqlserverOrdersBean.class))
                .process(new OrdersProcessFunction());

        // Sink
        if (env instanceof LocalStreamEnvironment) { // 在本地测试运行的逻辑
            result.print(">result>");
        } else { // 写入kafka
            result.sinkTo(FlinkSinkUtil.getKafkaSink(parameters, TopicConstant.TOPIC_ODS_ORDERS)).name("sink_ods_orders_topic");
        }

        env.disableOperatorChaining();

    }
}

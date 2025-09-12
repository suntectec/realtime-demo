package com.sands.realtime.ods.app;

import com.sands.realtime.common.base.BaseAPP;
import com.sands.realtime.common.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author Jagger
 * @since 2025/9/11 17:51
 */
public class OdsSinkToPaimonAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new OdsSinkToPaimonAPP().start(8081, args);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, ParameterTool parameters) {

        env.setParallelism(1);
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> source = env.fromSource(FlinkSourceUtil.getKafkaSource(parameters.get("kafka.broker"), "data_group", "ods_orders_topic", OffsetsInitializer.earliest()),
                WatermarkStrategy.noWatermarks(), "Kafka Source");

        source.print(">source>");

        // Transformation

    }

}

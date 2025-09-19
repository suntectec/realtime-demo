package com.sands.realtime.example.app;

import com.sands.realtime.common.base.BaseStreamEnvAPP;
import com.sands.realtime.common.constant.TopicConstant;
import com.sands.realtime.common.utils.FlinkSinkUtil;
import com.sands.realtime.common.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 采用继承方式执行的 Flink 实时计算 WordCount 案例
 *
 * @author Jagger
 * @since 2025/8/28 14:13
 */
public class WordCountAPP extends BaseStreamEnvAPP {

    public static void main(String[] args) throws Exception {
        new WordCountAPP().start(8081, args);
    }

    @Override
    public void handle(StreamExecutionEnvironment streamEnv, ParameterTool parameters) {
        streamEnv.setParallelism(1);

        // Source
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(parameters.get("kafka.broker"), "test_group", "test_topic", OffsetsInitializer.earliest());
        DataStreamSource<String> source = streamEnv.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        // Transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = source
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).name("wordAndOne");

        // 进行分组聚合(keyBy：将key相同的分到一个组中)
        SingleOutputStreamOperator<Tuple2<String, Integer>> sink = wordAndOneDS.keyBy(v -> v.f0).sum(1).name("wordCount");

        // Sink
        if (streamEnv instanceof LocalStreamEnvironment) {
            sink.print(">sink>");
        } else {
            sink
                    .map(Tuple2::toString)
                    .sinkTo(FlinkSinkUtil.getKafkaSink(parameters, TopicConstant.ODS_SOCKET_TOPIC)).name("sink_ods_socket_topic");
        }

        streamEnv.disableOperatorChaining();
    }

}

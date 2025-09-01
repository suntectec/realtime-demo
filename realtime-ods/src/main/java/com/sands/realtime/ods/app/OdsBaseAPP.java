package com.sands.realtime.ods.app;

import com.sands.realtime.common.base.BaseAPP;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 验证测试
 * 打开两个cmd窗口
 * 第一个窗口执行：
 * nc -lp 9000 # -l 监听模式 -p 开启监听端口
 * 第二个窗口执行：
 * nc localhost 9000
 *
 * bin/flink run -c com.sands.realtime.ods.app.OdsBaseAPP ./lib/jobs/realtime-ods/target/realtime-ods-1.0-SNAPSHOT.jar
 *
 * @author Jagger
 * @since 2025/8/28 14:13
 */
public class OdsBaseAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new OdsBaseAPP().start(8081, "test_group", "test_topic", args, OffsetsInitializer.earliest());
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> dss, ParameterTool parameter) throws Exception {

        env.setParallelism(1);

        SingleOutputStreamOperator<String> upperedDS = dss.map(String::toUpperCase).name("toUpperCase");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = upperedDS
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).name("wordAndOne");

        // 进行分组聚合(keyBy：将key相同的分到一个组中)
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = wordAndOneDS.keyBy(v -> v.f0).sum(1).name("wordCount");

        // 数据输出
        if (env instanceof LocalStreamEnvironment) {
            resultDS.print();
        } else {
            KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                    .setBootstrapServers(parameter.get("kafka.broker"))
                    .setRecordSerializer(
                            KafkaRecordSerializationSchema.<String>builder()
                                    .setTopic("ods_socket_topic")
                                    .setValueSerializationSchema(new SimpleStringSchema())
                                    .build()
                    )
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .build();

            resultDS
                    .map(Tuple2::toString)
                    .sinkTo(kafkaSink);
        }

        env.disableOperatorChaining();

    }

}

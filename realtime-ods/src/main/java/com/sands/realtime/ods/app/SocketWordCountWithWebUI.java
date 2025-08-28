package com.sands.realtime.ods.app;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author Jagger
 * @since 2025/8/28 14:13
 */
public class SocketWordCountWithWebUI {
    public static void main(String[] args) throws Exception {

        // 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT,"8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2.读取数据
        DataStreamSource<String> dss = env.socketTextStream("localhost", 9999);

        // 3.进行数据转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = dss.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] words = line.split(",");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 4.聚合打印结果
        tupleDS.keyBy(tp -> tp.f0).sum(1).print();

        // 5.execute触发执行
        env.execute();

    }
}

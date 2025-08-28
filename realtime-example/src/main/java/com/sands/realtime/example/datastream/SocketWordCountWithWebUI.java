package com.sands.realtime.example.datastream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
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
 * @author Jagger
 * @since 2025/8/28 14:13
 */
public class SocketWordCountWithWebUI {
    public static void main(String[] args) throws Exception {

        // 1.准备环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2.读取数据
        // Source 接收一个socket文本流
        DataStreamSource<String> dss = env.socketTextStream("localhost", 9000);

        // 3.进行数据转换处理
        // Transformation(s) 对数据进行处理操作
        SingleOutputStreamOperator<String> upperedDS = dss.map(String::toUpperCase);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = upperedDS
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));

        // 进行分组聚合(keyBy：将key相同的分到一个组中)
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = wordAndOne.keyBy(v -> v.f0).sum(1);

        // 4.数据输出
        resultDataStream.print().setParallelism(1);

        // 5.execute触发执行
        env.execute();

    }
}

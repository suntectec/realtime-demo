package com.sands.realtime.ods.app;

import com.sands.realtime.common.base.BaseAPP;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
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
public class OdsBaseAPP extends BaseAPP {

    public static void main(String[] args) throws Exception {
        new OdsBaseAPP().testStart(args, 9999);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> streamSource, ParameterTool parameter) throws Exception {

    }

    @Override
    public void testHandle(StreamExecutionEnvironment env, DataStreamSource<String> dss) throws Exception {

        SingleOutputStreamOperator<String> upperedDS = dss.map(String::toUpperCase).name("toUpperCase");
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOneDS = upperedDS
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT)).name("wordAndOne");

        // 进行分组聚合(keyBy：将key相同的分到一个组中)
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDataStream = wordAndOneDS.keyBy(v -> v.f0).sum(1).name("wordCount");

        // Sink 数据输出
        resultDataStream.print().setParallelism(1);

    }
}

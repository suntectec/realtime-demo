package com.sands.realtime.common.base;

import com.sands.realtime.common.utils.FlinkSourceUtil;
import com.sands.realtime.common.utils.ParametersUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.apache.flink.configuration.ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION;

/**
 * BaseApp的设计初衷：
 *  因为flink编程的都是 source - Transformation - sink 编程模式，所有使用抽象类进行封装
 *
 * @author Jagger
 * @since 2025/8/18 16:51
 */
@Slf4j
public abstract class BaseAPP {

    /**
     * Transformation(s) 数据转换，核心业务处理逻辑
     *
     * @param env
     * @param dss
     * @param parameter
     * @throws Exception
     */
    public abstract void handle(StreamExecutionEnvironment env,
                                DataStreamSource<String> dss, ParameterTool parameter) throws Exception;

    public void start(int port, String ckAndGroupId, String topic,String[] args, OffsetsInitializer offsetsInitializer) throws Exception {

        // 1. 环境准备
        // 1.1 设置操作 Hadoop 的用户名为 Hadoop 超级用户 flink
        System.setProperty("HADOOP_USER_NAME", "flink");

        // 1.2 获取流处理环境，并指定本地测试时启动 WebUI 所绑定的端口
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        // 1.2.1 本地运行环境添加 8081 WebUI
        if (env instanceof LocalStreamEnvironment) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        }

        // 1.3 将ParameterTool的参数设置成全局的参数
        // ParameterTool parameter = ParameterTool.fromArgs(args);
        // env.getConfig().setGlobalJobParameters(parameter);
        // 1.3.1 升级 parameter 采用优先级：args, properties 的方式获取
        ParameterTool parameter = ParametersUtil.setGlobalJobParameters(env, args);
        // 1.4 状态后端及检查点相关配置
        // 1.4.1 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        // 1.4.2 开启 checkpoint
        env.enableCheckpointing(300000);
        // 1.4.3 设置 checkpoint 模式: 精准一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 1.4.4 checkpoint 存储
        if (env instanceof LocalStreamEnvironment) {  // 在本地运行的逻辑
            HashMapStateBackend hashMapStateBackend = new HashMapStateBackend();
            env.setStateBackend(hashMapStateBackend); // 使用HashMapStateBackend  作为状态后端
            env.getCheckpointConfig().setCheckpointStorage("file:///D:\\tmp\\flink-checkpoints");
            log.info("flink提交作业模式：--本地");
        } else { // 在集群运行的逻辑
            EmbeddedRocksDBStateBackend embeddedRocksDBStateBackend = new EmbeddedRocksDBStateBackend(true);
            env.setStateBackend(embeddedRocksDBStateBackend);  // 设置 EmbeddedRocksDBStateBackend 作为状态后端
            env.getCheckpointConfig().setCheckpointStorage("hdfs:///flink/flink-checkpoints");
            log.info("flink提交作业模式：--集群");
        }
        // 1.4.5 checkpoint 并发数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 1.4.6 checkpoint 之间的最小间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        // 1.4.7 checkpoint  的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(300000);
        // 1.4.8 job 取消时 checkpoint 保留策略
        env.getCheckpointConfig().setExternalizedCheckpointRetention(RETAIN_ON_CANCELLATION);

        // 1.5 从 Kafka 目标主题读取数据，封装为流
        String kafkaServer = parameter.get("kafka.broker");
        KafkaSource<String> source = FlinkSourceUtil.getKafkaSource(kafkaServer, ckAndGroupId, topic, offsetsInitializer);

        // 2. 读取数据
        DataStreamSource<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka_source");

        // 3. 核心业务处理逻辑  4. 数据输出
        handle(env, stream, parameter);

        // 5. execute触发执行
        env.execute();

    }

    /**
     * 本地 8081 WebUI 和 socket文本流 测试
     * @param args
     * @param socketTextStreamPort
     * @throws Exception
     */
    public void localStart(String[] args, int socketTextStreamPort) throws Exception {
        // 1. 准备环境
        // 本地测试WebUI
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8081");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);

        // 2. 读取数据
        // Source 接收一个socket文本流
        DataStreamSource<String> dss = env.socketTextStream("localhost", socketTextStreamPort);

        ParameterTool parameter = ParametersUtil.setGlobalJobParameters(env, args);

        // 3. 核心业务处理逻辑  4. 数据输出
        handle(env, dss, parameter);

        // 5. execute触发执行
        env.execute();

    }
}
